package handler_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/ONSdigital/dp-api-clients-go/v2/image"
	"github.com/ONSdigital/dp-image-importer/handler"
	"github.com/ONSdigital/dp-image-importer/handler/mock"
	"github.com/ONSdigital/dp-image-importer/schema"
	dpkafka "github.com/ONSdigital/dp-kafka/v5"
	"github.com/ONSdigital/dp-kafka/v5/kafkatest"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	testAuthToken   = "auth-123"
	testDownloadURL = "http://some.download.server"
)

var (
	testCtx                  = context.Background()
	testWorkerID             = 1
	testPrivateBucket        = "privateBucket"
	testUploadedBucket       = "uploadedBucket"
	testPrivatePath          = "images/123/original"
	testSize           int64 = 1234
	fileBytes                = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	testFileContent          = io.NopCloser(bytes.NewReader(fileBytes))
	errS3Private             = errors.New("s3Private error")
	errS3Uploaded            = errors.New("s3Uploaded error")
	errImageAPI              = errors.New("imageAPI error")

	testImportStarted   = time.Date(2020, time.April, 26, 8, 5, 52, 0, time.UTC)
	testImportCompleted = time.Date(2020, time.April, 26, 8, 7, 32, 0, time.UTC)

	testCreatedDownload = image.ImageDownload{
		Id:            "original",
		State:         "importing",
		ImportStarted: &testImportStarted,
	}
	testImportedDownload = image.ImageDownload{
		Id:              "original",
		State:           "imported",
		ImportStarted:   &testImportStarted,
		ImportCompleted: &testImportCompleted,
	}
)

var testEvent = handler.ImageUploaded{
	ImageID:  "123",
	Filename: "Filename.png",
	Path:     "1234-uploadpng",
}

var testEventNoFilename = handler.ImageUploaded{
	ImageID: "123",
	Path:    "1234-uploadpng",
}

func TestImageUploadedHandler_Handle(t *testing.T) {

	Convey("Given S3 client mock", t, func() {

		mockS3Private := &mock.S3WriterMock{
			BucketNameFunc: func() string {
				return testPrivateBucket
			},
		}
		mockS3Upload := &mock.S3ReaderMock{
			BucketNameFunc: func() string {
				return testUploadedBucket
			},
		}
		mockImageAPI := &mock.ImageAPIClientMock{
			GetImageFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, imageID string) (image.Image, error) {
				return image.Image{}, nil
			},
			PutImageFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, imageID string, data image.Image) (image.Image, error) {
				return data, nil
			},
			PostDownloadVariantFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, imageID string, data image.NewImageDownload) (image.ImageDownload, error) {
				return testCreatedDownload, nil
			},
			PutDownloadVariantFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, imageID string, variant string, data image.ImageDownload) (image.ImageDownload, error) {
				return testImportedDownload, nil
			},
		}

		Convey("And a successful event handler, when Handle is triggered", func() {
			mockS3Upload.GetFunc = func(ctx context.Context, key string) (io.ReadCloser, *int64, error) {
				return testFileContent, &testSize, nil
			}
			mockS3Private.UploadFunc = func(ctx context.Context, input *s3.PutObjectInput, options ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
				return &manager.UploadOutput{}, nil
			}
			eventHandler := handler.ImageUploadedHandler{
				AuthToken:          testAuthToken,
				S3Upload:           mockS3Upload,
				S3Private:          mockS3Private,
				ImageCli:           mockImageAPI,
				DownloadServiceURL: testDownloadURL,
			}
			msg := createMessage(testEvent)
			err := eventHandler.Handle(testCtx, testWorkerID, msg)
			So(err, ShouldBeNil)

			Convey("An image download variant is posted to the image API", func() {
				So(mockImageAPI.PostDownloadVariantCalls(), ShouldHaveLength, 1)
				So(mockImageAPI.PostDownloadVariantCalls()[0].ImageID, ShouldEqual, testEvent.ImageID)
				So(mockImageAPI.PostDownloadVariantCalls()[0].ServiceAuthToken, ShouldResemble, testAuthToken)
				newImageData := mockImageAPI.PostDownloadVariantCalls()[0].Data
				So(newImageData, ShouldNotBeNil)
				So(newImageData.Id, ShouldEqual, "original")
				So(newImageData.State, ShouldEqual, "importing")
			})

			Convey("The file is uploaded to the private bucket", func() {
				So(mockS3Private.UploadCalls(), ShouldHaveLength, 1)
				So(*mockS3Private.UploadCalls()[0].Input, ShouldResemble, s3.PutObjectInput{
					Body:   testFileContent,
					Bucket: &testPrivateBucket,
					Key:    &testPrivatePath,
				})
			})

			Convey("The image download variant is put to the image API with a state of imported", func() {
				So(mockImageAPI.PutDownloadVariantCalls(), ShouldHaveLength, 1)
				So(mockImageAPI.PutDownloadVariantCalls()[0].ImageID, ShouldEqual, testEvent.ImageID)
				So(mockImageAPI.PutDownloadVariantCalls()[0].ServiceAuthToken, ShouldResemble, testAuthToken)
				newImageData := mockImageAPI.PutDownloadVariantCalls()[0].Data
				So(newImageData, ShouldNotBeNil)
				So(newImageData.Id, ShouldEqual, "original")
				So(newImageData.State, ShouldEqual, "imported")
				So(newImageData.ImportCompleted, ShouldNotBeNil)
				So(newImageData.Href, ShouldEqual, testDownloadURL+"/"+testPrivatePath+"/"+testEvent.Filename)
			})
		})

		Convey("And an event with no filename supplied, when Handle is triggered", func() {
			mockS3Upload.GetFunc = func(ctx context.Context, key string) (io.ReadCloser, *int64, error) {
				return testFileContent, &testSize, nil
			}
			mockS3Private.UploadFunc = func(ctx context.Context, input *s3.PutObjectInput, options ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
				return &manager.UploadOutput{}, nil
			}
			eventHandler := handler.ImageUploadedHandler{
				AuthToken:          testAuthToken,
				S3Upload:           mockS3Upload,
				S3Private:          mockS3Private,
				ImageCli:           mockImageAPI,
				DownloadServiceURL: testDownloadURL,
			}
			msg := createMessage(testEventNoFilename)
			err := eventHandler.Handle(testCtx, testWorkerID, msg)
			So(err, ShouldBeNil)

			Convey("The image download variant is put to the image API with a state of imported", func() {
				So(mockImageAPI.PostDownloadVariantCalls(), ShouldHaveLength, 1)
				So(mockImageAPI.PostDownloadVariantCalls()[0].ImageID, ShouldEqual, testEvent.ImageID)
				So(mockImageAPI.PostDownloadVariantCalls()[0].ServiceAuthToken, ShouldResemble, testAuthToken)
				newImageData := mockImageAPI.PutDownloadVariantCalls()[0].Data
				So(newImageData, ShouldNotBeNil)
				So(newImageData.Id, ShouldEqual, "original")
				So(newImageData.State, ShouldEqual, "imported")
				So(newImageData.Href, ShouldEqual, testDownloadURL+"/"+testPrivatePath+"/"+testEventNoFilename.Path)
			})
		})

		Convey("And an event handler (developer env), when Handle is triggered", func() {
			mockS3Upload.GetFunc = func(ctx context.Context, key string) (io.ReadCloser, *int64, error) {
				return testFileContent, &testSize, nil
			}
			mockS3Private.UploadFunc = func(ctx context.Context, input *s3.PutObjectInput, options ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
				return &manager.UploadOutput{}, nil
			}
			eventHandler := handler.ImageUploadedHandler{
				AuthToken:          testAuthToken,
				S3Upload:           mockS3Upload,
				S3Private:          mockS3Private,
				ImageCli:           mockImageAPI,
				DownloadServiceURL: testDownloadURL,
			}
			msg := createMessage(testEvent)
			err := eventHandler.Handle(testCtx, testWorkerID, msg)
			So(err, ShouldBeNil)

			Convey("An image download variant is posted to the image API", func() {
				So(mockImageAPI.PostDownloadVariantCalls(), ShouldHaveLength, 1)
				So(mockImageAPI.PostDownloadVariantCalls()[0].ImageID, ShouldEqual, testEvent.ImageID)
				So(mockImageAPI.PostDownloadVariantCalls()[0].ServiceAuthToken, ShouldResemble, testAuthToken)
				newImageData := mockImageAPI.PostDownloadVariantCalls()[0].Data
				So(newImageData, ShouldNotBeNil)
				So(newImageData.Id, ShouldEqual, "original")
				So(newImageData.State, ShouldEqual, "importing")
			})

			Convey("The file is obtained from the private bucket", func() {
				So(mockS3Upload.GetCalls(), ShouldHaveLength, 1)
				So(mockS3Upload.GetCalls()[0].Key, ShouldEqual, testEvent.Path)
			})

			Convey("The file is uploaded to the private bucket", func() {
				So(mockS3Private.UploadCalls(), ShouldHaveLength, 1)
				So(*mockS3Private.UploadCalls()[0].Input, ShouldResemble, s3.PutObjectInput{
					Body:   testFileContent,
					Bucket: &testPrivateBucket,
					Key:    &testPrivatePath,
				})
			})

			Convey("The image download variant is put to the image API with a state of imported", func() {
				So(mockImageAPI.PutDownloadVariantCalls(), ShouldHaveLength, 1)
				So(mockImageAPI.PutDownloadVariantCalls()[0].ImageID, ShouldEqual, testEvent.ImageID)
				So(mockImageAPI.PutDownloadVariantCalls()[0].ServiceAuthToken, ShouldResemble, testAuthToken)
				newImageData := mockImageAPI.PutDownloadVariantCalls()[0].Data
				So(newImageData, ShouldNotBeNil)
				So(newImageData.Id, ShouldEqual, "original")
				So(newImageData.State, ShouldEqual, "imported")
				So(newImageData.ImportCompleted, ShouldNotBeNil)
				So(newImageData.Href, ShouldEqual, testDownloadURL+"/"+testPrivatePath+"/"+testEvent.Filename)
			})
		})

		Convey("And an event handler with an S3Uploaded client that fails to obtain the source file, when Handle is triggered", func() {
			mockS3Upload.GetFunc = func(ctx context.Context, key string) (io.ReadCloser, *int64, error) {
				return nil, nil, errS3Uploaded
			}
			eventHandler := handler.ImageUploadedHandler{
				AuthToken:          testAuthToken,
				S3Upload:           mockS3Upload,
				S3Private:          mockS3Private,
				ImageCli:           mockImageAPI,
				DownloadServiceURL: testDownloadURL,
			}
			msg := createMessage(testEvent)
			err := eventHandler.Handle(testCtx, testWorkerID, msg)

			Convey("S3Private is called and the same error is returned", func() {
				So(err, ShouldResemble, errS3Uploaded)
			})

			Convey("The Image is retrieved from the API and updated with a state of failed_import", func() {
				So(mockImageAPI.GetImageCalls(), ShouldHaveLength, 1)
				So(mockImageAPI.GetImageCalls()[0].ImageID, ShouldEqual, testEvent.ImageID)
				So(mockImageAPI.GetImageCalls()[0].ServiceAuthToken, ShouldResemble, testAuthToken)
				So(mockImageAPI.PutImageCalls(), ShouldHaveLength, 1)
				So(mockImageAPI.PutImageCalls()[0].ImageID, ShouldEqual, testEvent.ImageID)
				So(mockImageAPI.PutImageCalls()[0].ServiceAuthToken, ShouldResemble, testAuthToken)
				updatedImage := mockImageAPI.PutImageCalls()[0].Data
				So(updatedImage.State, ShouldEqual, "failed_import")
				So(updatedImage.Error, ShouldEqual, "error getting s3 object reader")
			})
		})

		Convey("And an event handler (developer env) with an S3Uploaded client that fails to obtain the source file, when Handle is triggered", func() {
			mockS3Upload.GetFunc = func(ctx context.Context, key string) (io.ReadCloser, *int64, error) {
				return nil, nil, errS3Uploaded
			}
			eventHandler := handler.ImageUploadedHandler{
				AuthToken:          testAuthToken,
				S3Upload:           mockS3Upload,
				S3Private:          mockS3Private,
				ImageCli:           mockImageAPI,
				DownloadServiceURL: testDownloadURL,
			}
			msg := createMessage(testEvent)
			err := eventHandler.Handle(testCtx, testWorkerID, msg)

			Convey("S3Private is called and the same error is returned", func() {
				So(err, ShouldResemble, errS3Uploaded)
				So(mockS3Upload.GetCalls(), ShouldHaveLength, 1)
			})

			Convey("The Image is retrieved from the API and updated with a state of failed_import", func() {
				So(mockImageAPI.GetImageCalls(), ShouldHaveLength, 1)
				So(mockImageAPI.GetImageCalls()[0].ImageID, ShouldEqual, testEvent.ImageID)
				So(mockImageAPI.GetImageCalls()[0].ServiceAuthToken, ShouldResemble, testAuthToken)
				So(mockImageAPI.PutImageCalls(), ShouldHaveLength, 1)
				So(mockImageAPI.PutImageCalls()[0].ImageID, ShouldEqual, testEvent.ImageID)
				So(mockImageAPI.PutImageCalls()[0].ServiceAuthToken, ShouldResemble, testAuthToken)
				updatedImage := mockImageAPI.PutImageCalls()[0].Data
				So(updatedImage.State, ShouldEqual, "failed_import")
				So(updatedImage.Error, ShouldEqual, "error getting s3 object reader")
			})
		})

		Convey("And an event handler with an image client that fails to create a new variant, when Handle is triggered", func() {
			mockS3Upload.GetFunc = func(ctx context.Context, key string) (io.ReadCloser, *int64, error) {
				return testFileContent, &testSize, nil
			}
			mockImageAPIFail := &mock.ImageAPIClientMock{
				GetImageFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, imageID string) (image.Image, error) {
					return image.Image{}, nil
				},
				PutImageFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, imageID string, data image.Image) (image.Image, error) {
					return data, nil
				},
				PostDownloadVariantFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, imageID string, data image.NewImageDownload) (image.ImageDownload, error) {
					return image.ImageDownload{}, errImageAPI
				},
			}
			eventHandler := handler.ImageUploadedHandler{
				AuthToken:          testAuthToken,
				S3Upload:           mockS3Upload,
				S3Private:          mockS3Private,
				ImageCli:           mockImageAPIFail,
				DownloadServiceURL: testDownloadURL,
			}
			msg := createMessage(testEvent)
			err := eventHandler.Handle(testCtx, testWorkerID, msg)

			Convey("ImageAPI.PostDownloadVariant is called and the error is returned", func() {
				So(err, ShouldNotBeNil)
				So(mockImageAPIFail.PostDownloadVariantCalls(), ShouldHaveLength, 1)
			})

			Convey("The Image is retrieved from the API and updated with a state of failed_import", func() {
				So(mockImageAPIFail.GetImageCalls(), ShouldHaveLength, 1)
				So(mockImageAPIFail.GetImageCalls()[0].ImageID, ShouldEqual, testEvent.ImageID)
				So(mockImageAPIFail.GetImageCalls()[0].ServiceAuthToken, ShouldResemble, testAuthToken)
				So(mockImageAPIFail.PutImageCalls(), ShouldHaveLength, 1)
				So(mockImageAPIFail.PutImageCalls()[0].ImageID, ShouldEqual, testEvent.ImageID)
				So(mockImageAPIFail.PutImageCalls()[0].ServiceAuthToken, ShouldResemble, testAuthToken)
				updatedImage := mockImageAPIFail.PutImageCalls()[0].Data
				So(updatedImage.State, ShouldEqual, "failed_import")
				So(updatedImage.Error, ShouldEqual, "error posting image variant to API")
			})
		})

		Convey("And an event handler with an S3Private client that fails to upload the file, when Handle is triggered", func() {
			mockS3Upload.GetFunc = func(ctx context.Context, key string) (io.ReadCloser, *int64, error) {
				return testFileContent, &testSize, nil
			}
			mockS3Private.UploadFunc = func(ctx context.Context, input *s3.PutObjectInput, options ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
				return nil, errS3Private
			}
			eventHandler := handler.ImageUploadedHandler{
				AuthToken:          testAuthToken,
				S3Upload:           mockS3Upload,
				S3Private:          mockS3Private,
				ImageCli:           mockImageAPI,
				DownloadServiceURL: testDownloadURL,
			}
			msg := createMessage(testEvent)
			err := eventHandler.Handle(testCtx, testWorkerID, msg)

			Convey("S3Private is called and the same error is returned", func() {
				So(err, ShouldResemble, errS3Private)
				So(mockS3Private.BucketNameCalls(), ShouldHaveLength, 2)
			})

			Convey("The Image Download Variant is updated with a state of failed_import", func() {
				So(mockImageAPI.PutDownloadVariantCalls(), ShouldHaveLength, 1)
				So(mockImageAPI.PutDownloadVariantCalls()[0].ImageID, ShouldEqual, testEvent.ImageID)
				So(mockImageAPI.PutDownloadVariantCalls()[0].Variant, ShouldEqual, "original")
				So(mockImageAPI.PutDownloadVariantCalls()[0].ServiceAuthToken, ShouldResemble, testAuthToken)
				newImageData := mockImageAPI.PutDownloadVariantCalls()[0].Data
				So(newImageData, ShouldNotBeNil)
				So(newImageData.Id, ShouldEqual, "original")
				So(newImageData.State, ShouldEqual, "failed_import")
				So(newImageData.Error, ShouldEqual, "failed to upload variant to s3")
				So(newImageData.ImportCompleted, ShouldBeNil)
			})
		})

		Convey("And an event handler (developer env) with an S3Private client that fails to upload the file, when Handle is triggered", func() {
			mockS3Upload.GetFunc = func(ctx context.Context, key string) (io.ReadCloser, *int64, error) {
				return testFileContent, &testSize, nil
			}
			mockS3Private.UploadFunc = func(ctx context.Context, input *s3.PutObjectInput, options ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
				return nil, errS3Private
			}
			eventHandler := handler.ImageUploadedHandler{
				AuthToken:          testAuthToken,
				S3Upload:           mockS3Upload,
				S3Private:          mockS3Private,
				ImageCli:           mockImageAPI,
				DownloadServiceURL: testDownloadURL,
			}
			msg := createMessage(testEvent)
			err := eventHandler.Handle(testCtx, testWorkerID, msg)

			Convey("S3Private is called and the same error is returned", func() {
				So(err, ShouldResemble, errS3Private)
				So(mockS3Upload.GetCalls(), ShouldHaveLength, 1)
				So(mockS3Private.BucketNameCalls(), ShouldHaveLength, 2)
			})

			Convey("The Image Download Variant is updated with a state of failed_import", func() {
				So(mockImageAPI.PutDownloadVariantCalls(), ShouldHaveLength, 1)
				So(mockImageAPI.PutDownloadVariantCalls()[0].ImageID, ShouldEqual, testEvent.ImageID)
				So(mockImageAPI.PutDownloadVariantCalls()[0].Variant, ShouldEqual, "original")
				So(mockImageAPI.PutDownloadVariantCalls()[0].ServiceAuthToken, ShouldResemble, testAuthToken)
				newImageData := mockImageAPI.PutDownloadVariantCalls()[0].Data
				So(newImageData, ShouldNotBeNil)
				So(newImageData.Id, ShouldEqual, "original")
				So(newImageData.State, ShouldEqual, "failed_import")
				So(newImageData.Error, ShouldEqual, "failed to upload variant to s3")
				So(newImageData.ImportCompleted, ShouldBeNil)
			})
		})

		Convey("And an event handler with an image client that fails to update a variant, when Handle is triggered", func() {
			mockS3Upload.GetFunc = func(ctx context.Context, key string) (io.ReadCloser, *int64, error) {
				return testFileContent, &testSize, nil
			}
			mockS3Private.UploadFunc = func(ctx context.Context, input *s3.PutObjectInput, options ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
				return &manager.UploadOutput{}, nil
			}
			mockImageAPIFail := &mock.ImageAPIClientMock{
				GetImageFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, imageID string) (image.Image, error) {
					return image.Image{}, nil
				},
				PutImageFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, imageID string, data image.Image) (image.Image, error) {
					return data, nil
				},
				PostDownloadVariantFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, imageID string, data image.NewImageDownload) (image.ImageDownload, error) {
					return testCreatedDownload, nil
				},
				PutDownloadVariantFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, imageID string, variant string, data image.ImageDownload) (image.ImageDownload, error) {
					return image.ImageDownload{}, errImageAPI
				},
			}
			eventHandler := handler.ImageUploadedHandler{
				AuthToken:          testAuthToken,
				S3Upload:           mockS3Upload,
				S3Private:          mockS3Private,
				ImageCli:           mockImageAPIFail,
				DownloadServiceURL: testDownloadURL,
			}
			msg := createMessage(testEvent)
			err := eventHandler.Handle(testCtx, testWorkerID, msg)

			Convey("ImageAPI.PutDownloadVariant is called and the error is returned", func() {
				So(err, ShouldNotBeNil)
				So(mockImageAPIFail.PutDownloadVariantCalls(), ShouldHaveLength, 1)
			})

			Convey("The Image is retrieved from the API and updated with a state of failed_import", func() {
				So(mockImageAPIFail.GetImageCalls(), ShouldHaveLength, 1)
				So(mockImageAPIFail.GetImageCalls()[0].ImageID, ShouldEqual, testEvent.ImageID)
				So(mockImageAPIFail.GetImageCalls()[0].ServiceAuthToken, ShouldResemble, testAuthToken)
				So(mockImageAPIFail.PutImageCalls(), ShouldHaveLength, 1)
				So(mockImageAPIFail.PutImageCalls()[0].ImageID, ShouldEqual, testEvent.ImageID)
				So(mockImageAPIFail.PutImageCalls()[0].ServiceAuthToken, ShouldResemble, testAuthToken)
				updatedImage := mockImageAPIFail.PutImageCalls()[0].Data
				So(updatedImage.State, ShouldEqual, "failed_import")
				So(updatedImage.Error, ShouldEqual, "error putting updated image variant to API")
			})
		})
	})

}

func createMessage(s interface{}) dpkafka.Message {
	e, err := schema.ImageUploadedEvent.Marshal(s)
	So(err, ShouldBeNil)
	msg := kafkatest.NewMessage(e)
	return msg
}

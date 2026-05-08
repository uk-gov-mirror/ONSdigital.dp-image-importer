package handler

import (
	"context"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/ONSdigital/dp-api-clients-go/v2/image"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-image-importer/schema"
	kafka "github.com/ONSdigital/dp-kafka/v5"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

//go:generate moq -out mock/s3_reader.go -pkg mock . S3Reader
//go:generate moq -out mock/s3_writer.go -pkg mock . S3Writer
//go:generate moq -out mock/image_api.go -pkg mock . ImageAPIClient

const (
	importingState = "importing"
	importedState  = "imported"
	failedState    = "failed_import"
	variantID      = "original"
	variantType    = "png"
)

// ImageUploadedHandler ...
type ImageUploadedHandler struct {
	AuthToken          string
	S3Upload           S3Reader
	S3Private          S3Writer
	ImageCli           ImageAPIClient
	DownloadServiceURL string
}

// S3Writer defines the required methods from dp-s3 to interact with a particular bucket of AWS S3
type S3Writer interface {
	Checker(ctx context.Context, state *healthcheck.CheckState) error
	Config() aws.Config
	BucketName() string
	Upload(ctx context.Context, input *s3.PutObjectInput, options ...func(*manager.Uploader)) (*manager.UploadOutput, error)
	Get(ctx context.Context, key string) (io.ReadCloser, *int64, error)
}

// S3Reader defines the required methods from dp-s3 to read data to an AWS S3 Bucket
type S3Reader interface {
	Checker(ctx context.Context, state *healthcheck.CheckState) error
	Config() aws.Config
	BucketName() string
	Get(ctx context.Context, key string) (io.ReadCloser, *int64, error)
}

// ImageAPIClient defines the required methods from image API client
type ImageAPIClient interface {
	Checker(ctx context.Context, state *healthcheck.CheckState) error
	GetImage(ctx context.Context, userAuthToken, serviceAuthToken, collectionID, imageID string) (image.Image, error)
	PutImage(ctx context.Context, userAuthToken, serviceAuthToken, collectionID, imageID string, data image.Image) (image.Image, error)
	PostDownloadVariant(ctx context.Context, userAuthToken, serviceAuthToken, collectionID, imageID string, data image.NewImageDownload) (image.ImageDownload, error)
	PutDownloadVariant(ctx context.Context, userAuthToken, serviceAuthToken, collectionID, imageID, variant string, data image.ImageDownload) (image.ImageDownload, error)
}

// Handle takes a single event. From the uploaded S3 bucket, it writes it to the private bucket.
// It also calls the API to create a new download variant and to update it after the variant has been imported.
func (h *ImageUploadedHandler) Handle(ctx context.Context, _ int, msg kafka.Message) error {
	event := &ImageUploaded{}
	s := schema.ImageUploadedEvent

	if err := s.Unmarshal(msg.GetData(), event); err != nil {
		return &Error{
			err: fmt.Errorf("failed to unmarshal event: %w", err),
			logData: map[string]interface{}{
				"msg_data": string(msg.GetData()),
			},
		}
	}

	uploadBucket := h.S3Upload.BucketName()
	privateBucket := h.S3Private.BucketName()
	logData := log.Data{
		"event":          event,
		"upload_bucket":  uploadBucket,
		"private_bucket": privateBucket,
	}
	log.Info(ctx, "event handler called", logData)

	startTime := time.Now().UTC()
	uploadPath := event.Path

	reader, err := h.getS3Reader(ctx, uploadPath)
	if err != nil {
		log.Error(ctx, "error getting s3 object reader", err, logData)
		h.setImageStatusToFailed(ctx, event.ImageID, "error getting s3 object reader")
		return err
	}
	defer reader.Close()

	// POST /images/{id}/downloads
	imageDownload, err := h.ImageCli.PostDownloadVariant(ctx, "", h.AuthToken, "", event.ImageID, image.NewImageDownload{
		Id:            variantID,
		Height:        nil,
		Type:          variantType,
		Width:         nil,
		State:         importingState,
		ImportStarted: &startTime,
	})
	if err != nil {
		log.Error(ctx, "error posting image variant to API", err, logData)
		h.setImageStatusToFailed(ctx, event.ImageID, "error posting image variant to API")
		return err
	}
	logData["imageDownload"] = &imageDownload
	log.Info(ctx, "posted image download", logData)

	// Variant S3 key 'images/{id}/{variantId}'
	variantPath := path.Join("images", event.ImageID, variantID)
	logData["variant_path"] = variantPath

	log.Info(ctx, "uploading private file to s3", logData)
	err = h.uploadToS3(ctx, variantPath, reader)
	if err != nil {
		log.Error(ctx, "error uploading to s3", err, logData)
		h.setVariantStatusToFailed(ctx, event.ImageID, imageDownload, "failed to upload variant to s3")
		return err
	}
	endTime := time.Now().UTC()

	// PUT /images/{id}/downloads/{variantId})
	imageDownload.State = importedState
	imageDownload.ImportCompleted = &endTime
	fileName := event.Filename
	if fileName == "" {
		fileName = path.Base(uploadPath)
	}
	imageDownload.Href = fmt.Sprintf("%s/%s/%s", h.DownloadServiceURL, variantPath, fileName)
	imageDownload, err = h.ImageCli.PutDownloadVariant(ctx, "", h.AuthToken, "", event.ImageID, imageDownload.Id, imageDownload)
	if err != nil {
		log.Error(ctx, "error putting image variant to API", err, logData)
		h.setImageStatusToFailed(ctx, event.ImageID, "error putting updated image variant to API")
		return err
	}
	log.Info(ctx, "put image download", logData)
	log.Info(ctx, "handler successfully handled", logData)
	return nil
}

// Get an S3 reader
func (h *ImageUploadedHandler) getS3Reader(ctx context.Context, path string) (reader io.ReadCloser, err error) {
	// Get image from upload bucket
	reader, _, err = h.S3Upload.Get(ctx, path)
	return
}

// Upload to S3 from a reader
func (h *ImageUploadedHandler) uploadToS3(ctx context.Context, path string, reader io.Reader) error {
	uploadInput := &s3.PutObjectInput{
		Body:   reader,
		Bucket: new(h.S3Private.BucketName()),
		Key:    &path,
	}

	// Upload file to private bucket
	_, err := h.S3Private.Upload(ctx, uploadInput)
	if err != nil {
		return err
	}
	return nil
}

func (h *ImageUploadedHandler) setImageStatusToFailed(ctx context.Context, imageID string, desc string) {
	image, err := h.ImageCli.GetImage(ctx, "", h.AuthToken, "", imageID)
	if err != nil {
		log.Error(ctx, "error getting image from API to set failed_import status", err)
		return
	}
	image.State = failedState
	image.Error = desc
	_, err = h.ImageCli.PutImage(ctx, "", h.AuthToken, "", imageID, image)
	if err != nil {
		log.Error(ctx, "error putting image to API to set failed_import status", err)
		return
	}
}

func (h *ImageUploadedHandler) setVariantStatusToFailed(ctx context.Context, imageID string, variant image.ImageDownload, desc string) {
	variant.State = failedState
	variant.Error = desc
	_, err := h.ImageCli.PutDownloadVariant(ctx, "", h.AuthToken, "", imageID, variant.Id, variant)
	if err != nil {
		log.Error(ctx, "error putting image variant to API to set failed_import status", err)
		return
	}
}

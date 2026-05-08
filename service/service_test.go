package service_test

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-image-importer/config"
	"github.com/ONSdigital/dp-image-importer/handler"
	clientMock "github.com/ONSdigital/dp-image-importer/handler/mock"
	service "github.com/ONSdigital/dp-image-importer/service"
	serviceMock "github.com/ONSdigital/dp-image-importer/service/mock"
	kafka "github.com/ONSdigital/dp-kafka/v5"
	"github.com/ONSdigital/dp-kafka/v5/kafkatest"

	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	ctx           = context.Background()
	testBuildTime = "BuildTime"
	testGitCommit = "GitCommit"
	testVersion   = "Version"
	testChecks    = map[string]*healthcheck.Check{
		"ImageAPI client":              {},
		"S3Uploaded client":            {},
		"S3Private client":             {},
		"ImageUploaded Kafka consumer": {},
	}
	errKafkaConsumer = errors.New("Kafka consumer error")
	errHealthcheck   = errors.New("healthCheck error")
	errServer        = errors.New("HTTP Server error")
	errAddCheck      = fmt.Errorf("healthcheck add check error")
)

func TestNew(t *testing.T) {
	Convey("service.New returns a new empty service struct", t, func() {
		srv := service.New()
		So(*srv, ShouldResemble, service.Service{})
	})
}

func TestInit(t *testing.T) {
	Convey("Having a set of mocked dependencies", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)

		// Mocking the Kafka consumers
		consumerMock := &kafkatest.IConsumerGroupMock{
			RegisterHandlerFunc: func(ctx context.Context, h kafka.Handler) error {
				return nil
			},
		}

		// Mocking the Kafka consumer retrieval function to return both consumers
		service.GetKafkaConsumer = func(ctx context.Context, cfg *config.Kafka, topic string) (kafka.IConsumerGroup, error) {
			return consumerMock, nil
		}

		subscribedTo := []*healthcheck.Check{}
		hcMock := &serviceMock.HealthCheckerMock{
			AddAndGetCheckFunc: func(name string, checker healthcheck.Checker) (*healthcheck.Check, error) {
				return testChecks[name], nil
			},
			SubscribeFunc: func(s healthcheck.Subscriber, checks ...*healthcheck.Check) {
				subscribedTo = append(subscribedTo, checks...)
			},
		}
		service.GetHealthCheck = func(cfg *config.Config, buildTime, gitCommit, version string) (service.HealthChecker, error) {
			return hcMock, nil
		}

		serverMock := &serviceMock.HTTPServerMock{}
		service.GetHTTPServer = func(bindAddr string, router http.Handler) service.HTTPServer {
			return serverMock
		}

		imageAPIMock := &clientMock.ImageAPIClientMock{
			CheckerFunc: func(context.Context, *healthcheck.CheckState) error { return nil },
		}
		service.GetImageAPI = func(ctx context.Context, cfg *config.Config) handler.ImageAPIClient {
			return imageAPIMock
		}

		s3UploadedMock := &clientMock.S3WriterMock{
			CheckerFunc: func(context.Context, *healthcheck.CheckState) error { return nil },
		}
		s3PrivateMock := &clientMock.S3WriterMock{
			CheckerFunc: func(context.Context, *healthcheck.CheckState) error { return nil },
		}
		service.GetS3Client = func(ctx context.Context, awsRegion, bucketName string) (handler.S3Writer, error) {
			switch bucketName {
			case cfg.S3UploadedBucketName:
				return s3UploadedMock, nil
			case cfg.S3PrivateBucketName:
				return s3PrivateMock, nil
			}
			return nil, errors.New("unknown bucket in mock")
		}

		svc := &service.Service{}

		Convey("Tying to initialise a service without a config returns the expected error", func() {
			err := svc.Init(ctx, nil, testBuildTime, testGitCommit, testVersion)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "nil config passed to service init")
		})

		Convey("Given that initialising Kafka consumer returns an error", func() {
			service.GetKafkaConsumer = func(ctx context.Context, cfg *config.Kafka, _ string) (kafka.IConsumerGroup, error) {
				return nil, errKafkaConsumer
			}

			Convey("Then service Init fails with the same error and no further initialisations are attempted", func() {
				err := svc.Init(ctx, cfg, testBuildTime, testGitCommit, testVersion)
				So(errors.Unwrap(err), ShouldResemble, errKafkaConsumer)
				So(svc.Config, ShouldResemble, cfg)

				Convey("And no checkers are registered ", func() {
					So(hcMock.AddAndGetCheckCalls(), ShouldHaveLength, 0)
				})
			})
		})

		Convey("Given that Kafka consumers fail to register a handler", func() {
			consumerMock.RegisterHandlerFunc = func(ctx context.Context, h kafka.Handler) error {
				return errKafkaConsumer
			}

			Convey("Then service Init fails with the same error and no further initialisations are attempted", func() {
				err := svc.Init(ctx, cfg, testBuildTime, testGitCommit, testVersion)
				So(errors.Unwrap(err), ShouldResemble, errKafkaConsumer)
				So(svc.Config, ShouldResemble, cfg)

				Convey("And no checkers are registered ", func() {
					So(hcMock.AddAndGetCheckCalls(), ShouldHaveLength, 0)
				})
			})
		})

		Convey("Given that initialising healthcheck returns an error", func() {
			service.GetHealthCheck = func(cfg *config.Config, buildTime, gitCommit, version string) (service.HealthChecker, error) {
				return nil, errHealthcheck
			}

			Convey("Then service Init fails with the same error and no further initialisations are attempted", func() {
				err := svc.Init(ctx, cfg, testBuildTime, testGitCommit, testVersion)
				So(errors.Unwrap(err), ShouldResemble, errHealthcheck)
				So(svc.Config, ShouldResemble, cfg)
				So(svc.ImageUploadedConsumer, ShouldResemble, consumerMock)

				Convey("And no checkers are registered ", func() {
					So(hcMock.AddAndGetCheckCalls(), ShouldHaveLength, 0)
				})
			})
		})

		Convey("Given that Checkers cannot be registered", func() {
			hcMock.AddAndGetCheckFunc = func(name string, checker healthcheck.Checker) (*healthcheck.Check, error) { return nil, errAddCheck }

			Convey("Then service Init fails with the expected error", func() {
				err := svc.Init(ctx, cfg, testBuildTime, testGitCommit, testVersion)
				So(err, ShouldNotBeNil)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "unable to register checkers: Error(s) registering checkers for healthcheck")
				So(svc.Config, ShouldResemble, cfg)
				So(svc.ImageUploadedConsumer, ShouldResemble, consumerMock)

				Convey("And all other checkers try to register", func() {
					So(hcMock.AddAndGetCheckCalls(), ShouldHaveLength, 4)
				})
			})
		})

		Convey("Given that all dependencies are successfully initialised", func() {
			Convey("Then service Init succeeds and all dependencies are initialised", func() {
				err := svc.Init(ctx, cfg, testBuildTime, testGitCommit, testVersion)
				So(err, ShouldBeNil)
				So(svc.Config, ShouldResemble, cfg)
				So(svc.Server, ShouldEqual, serverMock)
				So(svc.HealthCheck, ShouldResemble, hcMock)
				So(svc.ImageUploadedConsumer, ShouldResemble, consumerMock)
				So(svc.ImageCli, ShouldResemble, imageAPIMock)
				So(svc.S3UploadedCli, ShouldResemble, s3UploadedMock)
				So(svc.S3PrivateCli, ShouldResemble, s3PrivateMock)

				Convey("Then only necessary checks are registered based on feature flags", func() {
					registeredChecks := make(map[string]bool)
					for _, check := range hcMock.AddAndGetCheckCalls() {
						registeredChecks[check.Name] = true
					}

					So(registeredChecks["ImageAPI client"], ShouldBeTrue)
					So(registeredChecks["S3Uploaded client"], ShouldBeTrue)
					So(registeredChecks["S3Private client"], ShouldBeTrue)

					So(registeredChecks["ImageUploaded Kafka consumer"], ShouldBeTrue)
				})

				Convey("Then Kafka consumers subscribe to the correct health checks", func() {
					So(subscribedTo, ShouldHaveLength, 3)
					So(hcMock.SubscribeCalls()[0].Checks, ShouldContain, testChecks["ImageAPI client"])
					So(hcMock.SubscribeCalls()[0].Checks, ShouldContain, testChecks["S3Uploaded client"])
					So(hcMock.SubscribeCalls()[0].Checks, ShouldContain, testChecks["S3Private client"])
				})
			})
		})
	})
}

func TestStart(t *testing.T) {
	Convey("Having a correctly initialised Service with mocked dependencies", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)

		consumerMock := &kafkatest.IConsumerGroupMock{
			LogErrorsFunc: func(ctx context.Context) {},
		}

		hcMock := &serviceMock.HealthCheckerMock{
			StartFunc: func(ctx context.Context) {},
		}

		serverWg := &sync.WaitGroup{}
		serverMock := &serviceMock.HTTPServerMock{}

		svc := &service.Service{
			Config:                cfg,
			Server:                serverMock,
			HealthCheck:           hcMock,
			ImageUploadedConsumer: consumerMock,
		}

		Convey("When a service with a successful HTTP server is started", func() {
			cfg.StopConsumingOnUnhealthy = true
			serverMock.ListenAndServeFunc = func() error {
				serverWg.Done()
				return nil
			}
			serverWg.Add(1)
			err := svc.Start(ctx, make(chan error, 1))
			So(err, ShouldBeNil)

			Convey("Then healthcheck is started and HTTP server starts listening", func() {
				So(len(hcMock.StartCalls()), ShouldEqual, 1)
				serverWg.Wait() // Wait for HTTP server go-routine to finish
				So(len(serverMock.ListenAndServeCalls()), ShouldEqual, 1)
			})
		})

		Convey("When a service is started with StopConsumingOnUnhealthy disabled", func() {
			cfg.StopConsumingOnUnhealthy = false
			consumerMock.StartFunc = func() error { return nil }
			serverMock.ListenAndServeFunc = func() error { return nil }
			err := svc.Start(ctx, make(chan error, 1))
			So(err, ShouldBeNil)

			Convey("Then the kafka consumer is manually started", func() {
				So(consumerMock.StartCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("When a service is started with StopConsumingOnUnhealthy disabled and the Start func returns an error", func() {
			cfg.StopConsumingOnUnhealthy = false
			consumerMock.StartFunc = func() error { return errKafkaConsumer }
			serverMock.ListenAndServeFunc = func() error { return nil }
			err := svc.Start(ctx, make(chan error, 1))

			Convey("Then the expected error is returned", func() {
				So(consumerMock.StartCalls(), ShouldHaveLength, 1)
				So(err, ShouldNotBeNil)
				So(errors.Unwrap(err), ShouldResemble, errKafkaConsumer)
			})
		})

		Convey("When a service with a failing HTTP server is started", func() {
			cfg.StopConsumingOnUnhealthy = true
			serverMock.ListenAndServeFunc = func() error {
				serverWg.Done()
				return errServer
			}
			errChan := make(chan error, 1)
			serverWg.Add(1)
			err := svc.Start(ctx, errChan)
			So(err, ShouldBeNil)

			Convey("Then HTTP server errors are reported to the provided errors channel", func() {
				rxErr := <-errChan
				So(rxErr.Error(), ShouldResemble, fmt.Sprintf("failure in http listen and serve: %s", errServer.Error()))
			})
		})
	})
}

func TestClose(t *testing.T) {
	Convey("Having a service without initialised dependencies", t, func() {
		cfg := &config.Config{
			GracefulShutdownTimeout: 5 * time.Second,
		}
		svc := service.Service{
			Config: cfg,
		}

		Convey("Then the service can be closed without any issue (noop)", func() {
			err := svc.Close(context.Background())
			So(err, ShouldBeNil)
		})
	})

	Convey("Having a service with a kafka consumers that takes more time to stop listening than the graceful shutdown timeout", t, func() {
		cfg := &config.Config{
			GracefulShutdownTimeout: time.Millisecond,
		}
		consumerMock := &kafkatest.IConsumerGroupMock{
			StopAndWaitFunc: func() error {
				time.Sleep(100 * time.Millisecond)
				return nil
			},
			CloseFunc: func(ctx context.Context, optFuncs ...kafka.OptFunc) error {
				return nil
			},
		}

		svc := service.Service{
			Config:                cfg,
			ImageUploadedConsumer: consumerMock,
		}

		Convey("Then the service fails to close due to a timeout error", func() {
			err := svc.Close(context.Background())
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "shutdown timed out: context deadline exceeded")
		})
	})

	Convey("Having a fully initialised service", t, func() {
		cfg := &config.Config{
			GracefulShutdownTimeout: 5 * time.Second,
		}
		consumerMock := &kafkatest.IConsumerGroupMock{}
		hcMock := &serviceMock.HealthCheckerMock{}
		serverMock := &serviceMock.HTTPServerMock{}

		svc := &service.Service{
			Config:                cfg,
			Server:                serverMock,
			HealthCheck:           hcMock,
			ImageUploadedConsumer: consumerMock,
		}

		Convey("And all mocks can successfully close, if done in the right order", func() {
			hcStopped := false

			consumerMock.StopAndWaitFunc = func() error { return nil }
			consumerMock.CloseFunc = func(ctx context.Context, optFuncs ...kafka.OptFunc) error { return nil }
			hcMock.StopFunc = func() { hcStopped = true }
			serverMock.ShutdownFunc = func(ctx context.Context) error {
				if !hcStopped {
					return fmt.Errorf("server stopped before healthcheck")
				}
				return nil
			}

			Convey("Then the service can be successfully closed", func() {
				err := svc.Close(context.Background())
				So(err, ShouldBeNil)

				Convey("And all the dependencies are closed", func() {
					So(consumerMock.StopAndWaitCalls(), ShouldHaveLength, 1)
					So(hcMock.StopCalls(), ShouldHaveLength, 1)
					So(consumerMock.CloseCalls(), ShouldHaveLength, 1)
					So(serverMock.ShutdownCalls(), ShouldHaveLength, 1)
				})
			})
		})

		Convey("And all mocks fail to close", func() {
			consumerMock.StopAndWaitFunc = func() error { return errKafkaConsumer }
			consumerMock.CloseFunc = func(ctx context.Context, optFuncs ...kafka.OptFunc) error { return errKafkaConsumer }
			hcMock.StopFunc = func() {}
			serverMock.ShutdownFunc = func(ctx context.Context) error { return errServer }

			Convey("Then the service returns the expected error when closed", func() {
				err := svc.Close(context.Background())
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "failed to shutdown gracefully")

				Convey("And all the dependencies are closed", func() {
					So(consumerMock.StopAndWaitCalls(), ShouldHaveLength, 1)
					So(hcMock.StopCalls(), ShouldHaveLength, 1)
					So(serverMock.ShutdownCalls(), ShouldHaveLength, 1)
				})
			})
		})
	})
}

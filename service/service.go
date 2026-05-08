package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-image-importer/config"
	"github.com/ONSdigital/dp-image-importer/handler"
	kafka "github.com/ONSdigital/dp-kafka/v5"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

// Service contains all the configs, server and clients to run the Image API
type Service struct {
	Config                *config.Config
	Server                HTTPServer
	HealthCheck           HealthChecker
	ImageUploadedConsumer kafka.IConsumerGroup
	ImageCli              handler.ImageAPIClient
	S3UploadedCli         handler.S3Reader
	S3PrivateCli          handler.S3Writer
}

// New returns a new empty [service.Service] struct
func New() *Service {
	return &Service{}
}

// Init initializes the Service by setting up essential components like Kafka producers, consumers, health checks, and caching.
// It validates the provided configuration, initializes clients and consumers, registers health checks, and sets up an HTTP server for health endpoints.
func (svc *Service) Init(ctx context.Context, cfg *config.Config, buildTime, gitCommit, version string) error {
	var err error

	if cfg == nil {
		return errors.New("nil config passed to service init")
	}

	svc.Config = cfg

	if err := svc.initClients(ctx); err != nil {
		return err
	}

	if err := svc.initConsumers(ctx); err != nil {
		return err
	}

	// Get HealthCheck
	if svc.HealthCheck, err = GetHealthCheck(cfg, buildTime, gitCommit, version); err != nil {
		return fmt.Errorf("could not instantiate healthcheck: %w", err)
	}

	if err := svc.registerCheckers(ctx); err != nil {
		return fmt.Errorf("unable to register checkers: %w", err)
	}

	r := mux.NewRouter()
	r.StrictSlash(true).Path("/health").HandlerFunc(svc.HealthCheck.Handler)
	svc.Server = GetHTTPServer(cfg.BindAddr, r)

	return nil
}

// initClients initializes external service clients based on the service configuration.
// It sets up clients for Zebedee, DatasetAPI, and Topic services if their corresponding flags are enabled.
func (svc *Service) initClients(ctx context.Context) error {

	// Get S3 Clients
	s3Uploaded, s3Private, err := GetS3Clients(svc.Config)
	if err != nil {
		log.Error(ctx, "could not instantiate S3 clients", err)
		return err
	}
	svc.S3UploadedCli = s3Uploaded
	svc.S3PrivateCli = s3Private

	// Get Image API Client
	svc.ImageCli = GetImageAPI(ctx, svc.Config)

	return nil
}

func (svc *Service) initConsumers(ctx context.Context) error {
	var err error

	if svc.ImageUploadedConsumer, err = GetKafkaConsumer(ctx, svc.Config.Kafka, svc.Config.Kafka.ImageUploadedTopic); err != nil {
		return fmt.Errorf("failed to create image-uploaded consumer: %w", err)
	}
	contentHandler := &handler.ImageUploadedHandler{
		AuthToken:          svc.Config.ServiceAuthToken,
		S3Upload:           svc.S3UploadedCli,
		S3Private:          svc.S3PrivateCli,
		ImageCli:           svc.ImageCli,
		DownloadServiceURL: svc.Config.DownloadServiceURL,
	}
	if err = svc.ImageUploadedConsumer.RegisterHandler(ctx, contentHandler.Handle); err != nil {
		return fmt.Errorf("could not register image-uploaded handler: %w", err)
	}
	log.Info(ctx, "image-uploaded consumer and handler registered")

	return nil
}

// Start the service
func (svc *Service) Start(ctx context.Context, svcErrors chan error) error {
	log.Info(ctx, "starting service")

	svc.ImageUploadedConsumer.LogErrors(ctx)

	// If start/stop on health updates is disabled, start consuming as soon as possible
	if !svc.Config.StopConsumingOnUnhealthy {
		if err := svc.ImageUploadedConsumer.Start(); err != nil {
			return fmt.Errorf("content-publish consumer failed to start: %w", err)
		}
	}

	// Always start healthcheck.
	// If start/stop on health updates is enabled,
	// the consumer will start consuming on the first healthy update
	svc.HealthCheck.Start(ctx)

	// Run the http server in a new go-routine
	go func() {
		if err := svc.Server.ListenAndServe(); err != nil {
			svcErrors <- fmt.Errorf("failure in http listen and serve: %w", err)
		}
	}()

	return nil
}

// Close gracefully shuts the service down in the required order, with timeout
func (svc *Service) Close(ctx context.Context) error {
	timeout := svc.Config.GracefulShutdownTimeout
	log.Info(ctx, "commencing graceful shutdown", log.Data{"graceful_shutdown_timeout": timeout})
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	hasShutdownError := false

	go func() {
		defer cancel()

		// Stop health check, as it depends on everything else
		if svc.HealthCheck != nil {
			log.Info(ctx, "stopping health checker...")
			svc.HealthCheck.Stop()
			log.Info(ctx, "stopped health checker")
		}

		// Shutdown consumers and producer
		if err := svc.shutdownConsumers(ctx); err != nil {
			log.Error(ctx, "consumer shutdown error", err)
			hasShutdownError = true
		}

		// Shutdown the HTTP server
		if svc.Server != nil {
			log.Info(ctx, "shutting http server down...")
			if err := svc.Server.Shutdown(ctx); err != nil {
				log.Error(ctx, "failed to shutdown http server", err)
				hasShutdownError = true
			} else {
				log.Info(ctx, "shut down http server")
			}
		}
	}()

	// Wait for shutdown success (via cancel) or failure (timeout)
	<-ctx.Done()

	// Timeout expired
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("shutdown timed out: %w", ctx.Err())
	}

	// Other error
	if hasShutdownError {
		return errors.New("failed to shutdown gracefully")
	}

	log.Info(ctx, "graceful shutdown was successful")
	return nil
}

// shutdownConsumers stops and closes Kafka consumers
func (svc *Service) shutdownConsumers(ctx context.Context) error {
	var errMessages []string

	stopAndCloseConsumer := func(consumer kafka.IConsumerGroup, name string) {
		if consumer == nil {
			return
		}

		log.Info(ctx, "stopping kafka consumer listener...", log.Data{"Consumer": name})
		if err := consumer.StopAndWait(); err != nil {
			log.Error(ctx, "error stopping kafka consumer listener", err, log.Data{"Consumer": name})
			errMessages = append(errMessages, fmt.Sprintf("%s consumer stop error: %v", name, err))
		} else {
			log.Info(ctx, "stopped kafka consumer listener", log.Data{"Consumer": name})
		}

		log.Info(ctx, "closing kafka consumer...", log.Data{"Consumer": name})
		if err := consumer.Close(ctx); err != nil {
			log.Error(ctx, "error closing kafka consumer", err, log.Data{"Consumer": name})
			errMessages = append(errMessages, fmt.Sprintf("%s consumer close error: %v", name, err))
		} else {
			log.Info(ctx, "closed kafka consumer", log.Data{"Consumer": name})
		}
	}

	// Handle both consumers
	stopAndCloseConsumer(svc.ImageUploadedConsumer, "image-uploaded")

	// Aggregate errors, if any
	if len(errMessages) > 0 {
		return fmt.Errorf("consumer shutdown error: %s", strings.Join(errMessages, "; "))
	}
	return nil
}

// registerCheckers registers health checks for various service components such as Kafka consumers and clients.
// It divides the registration process into three distinct steps:
// 1. Client Checkers: Registers health checks for ImageAPI and S3 clients.
// 2. Consumer Checkers: Registers health checks for Kafka consumers (ImageUploaded).
// 3. Subscription: Subscribes consumers to health checks based on feature flags and component availability.
// If any of the health checks fail during registration, the method returns an error.
func (svc *Service) registerCheckers(ctx context.Context) error {
	var hasErrors bool

	// Step 1: Register client checkers
	chkImageAPI, s3UploadedCheck, s3PrivateCheck := svc.registerClientCheckers(ctx, &hasErrors)

	// Step 2: Register consumer checkers
	svc.registerConsumerCheckers(ctx, &hasErrors)

	if hasErrors {
		return errors.New("Error(s) registering checkers for healthcheck")
	}

	// Step 3: Subscribe consumers to health checks
	svc.subscribeToHealthCheck(chkImageAPI, s3UploadedCheck, s3PrivateCheck)
	return nil
}

// registerClientCheckers registers health checks for ImageAPI and S3 clients
// It returns the respective health check references and updates the hasErrors flag if any registration fails.
func (svc *Service) registerClientCheckers(ctx context.Context, hasErrors *bool) (imageAPICheck, s3UploadedCheck, s3PrivateCheck *healthcheck.Check) {
	if svc.ImageCli != nil {
		imageAPICheck, *hasErrors = svc.addHealthCheck(ctx, true, "ImageAPI client", svc.ImageCli.Checker, *hasErrors)
	}
	if svc.S3UploadedCli != nil {
		s3UploadedCheck, *hasErrors = svc.addHealthCheck(ctx, true, "S3Uploaded client", svc.S3UploadedCli.Checker, *hasErrors)
	}
	if svc.S3UploadedCli != nil {
		s3PrivateCheck, *hasErrors = svc.addHealthCheck(ctx, true, "S3Private client", svc.S3UploadedCli.Checker, *hasErrors)
	}
	return imageAPICheck, s3UploadedCheck, s3PrivateCheck
}

// registerConsumerCheckers registers health checks for Kafka consumers, such as ImageUploaded
// It updates the hasErrors flag if any health check registration fails.
func (svc *Service) registerConsumerCheckers(ctx context.Context, hasErrors *bool) {
	if svc.ImageUploadedConsumer != nil {
		if _, err := svc.HealthCheck.AddAndGetCheck("ImageUploaded Kafka consumer", svc.ImageUploadedConsumer.Checker); err != nil {
			*hasErrors = true
			log.Error(ctx, "error adding check for ImageUploaded Kafka consumer", err)
		}
	}
}

// subscribeToHealthCheck handles the subscription process of Kafka consumers to relevant health checks.
// It evaluates the configuration settings and component availability to ensure that only healthy
// components participate in message consumption. If a component becomes unhealthy and stop-consume
// behavior is enabled, the corresponding consumer is halted.
func (svc *Service) subscribeToHealthCheck(chkImageAPI, s3UploadedCheck, s3PrivateCheck *healthcheck.Check) {
	if svc.Config.StopConsumingOnUnhealthy {
		subscribers := []healthcheck.Subscriber{}
		checks := []*healthcheck.Check{}

		if chkImageAPI != nil {
			checks = append(checks, chkImageAPI)
		}
		if s3UploadedCheck != nil {
			checks = append(checks, s3UploadedCheck)
		}
		if s3PrivateCheck != nil {
			checks = append(checks, s3PrivateCheck)
		}
		if svc.ImageUploadedConsumer != nil {
			subscribers = append(subscribers, svc.ImageUploadedConsumer)
		}

		for _, sub := range subscribers {
			svc.HealthCheck.Subscribe(sub, checks...)
		}
	}
}

func (svc *Service) addHealthCheck(ctx context.Context, enabled bool, name string, checker healthcheck.Checker, hasErrors bool) (*healthcheck.Check, bool) {
	if enabled {
		check, err := svc.HealthCheck.AddAndGetCheck(name, checker)
		if err != nil {
			log.Error(ctx, "error adding check for "+name, err)
			hasErrors = true
			return nil, hasErrors
		}
		return check, hasErrors
	}
	log.Info(ctx, "skipping health check registration as configuration is disabled", log.Data{
		"name":    name,
		"enabled": enabled,
	})
	return nil, hasErrors
}

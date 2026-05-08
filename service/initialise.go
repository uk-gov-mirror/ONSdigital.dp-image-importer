package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/ONSdigital/dp-api-clients-go/v2/image"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-image-importer/config"
	"github.com/ONSdigital/dp-image-importer/handler"
	kafka "github.com/ONSdigital/dp-kafka/v5"
	dphttp "github.com/ONSdigital/dp-net/v3/http"
	dps3 "github.com/ONSdigital/dp-s3/v3"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// GetHTTPServer creates an HTTP Server with the provided bind address and router
var GetHTTPServer = func(bindAddr string, router http.Handler) HTTPServer {
	s := dphttp.NewServer(bindAddr, router)
	s.HandleOSSignals = false
	return s
}

var GetHealthCheck = func(cfg *config.Config, buildTime, gitCommit, version string) (HealthChecker, error) {
	versionInfo, err := healthcheck.NewVersionInfo(buildTime, gitCommit, version)
	if err != nil {
		return nil, fmt.Errorf("failed to get version info: %w", err)
	}
	return new(healthcheck.New(
		versionInfo,
		cfg.HealthCheckCriticalTimeout,
		cfg.HealthCheckInterval,
	)), nil
}

// ExternalServiceList holds the initialiser and initialisation state of external services.
type ExternalServiceList struct {
	S3Private     bool
	S3Uploaded    bool
	ImageAPI      bool
	HealthCheck   bool
	KafkaConsumer bool
	Init          Initialiser
}

// NewServiceList creates a new service list with the provided initialiser
func NewServiceList(initialiser Initialiser) *ExternalServiceList {
	return &ExternalServiceList{
		S3Private:     false,
		S3Uploaded:    false,
		ImageAPI:      false,
		HealthCheck:   false,
		KafkaConsumer: false,
		Init:          initialiser,
	}
}

// GetS3Clients returns S3 clients uploaded and private. They share the same AWS session.
var GetS3Clients = func(cfg *config.Config) (s3Uploaded handler.S3Reader, s3Private handler.S3Writer, err error) {
	ctx := context.Background()
	s3Private, err = GetS3Client(ctx, cfg.AwsRegion, cfg.S3PrivateBucketName)
	if err != nil {
		return nil, nil, err
	}
	s3Uploaded, err = GetS3Client(ctx, cfg.AwsRegion, cfg.S3UploadedBucketName)
	if err != nil {
		return nil, nil, err
	}
	return
}

// GetImageAPI creates an ImageAPI client
var GetImageAPI = func(ctx context.Context, cfg *config.Config) handler.ImageAPIClient {
	return image.NewAPIClient(cfg.ImageAPIURL)
}

// GetS3Client creates a new S3Client for the provided AWS region and bucket name.
var GetS3Client = func(ctx context.Context, awsRegion, bucketName string) (handler.S3Writer, error) {
	cfg, _ := config.Get()

	var s3Client *dps3.Client

	if cfg.LocalS3URL != "" {
		config, err := awsConfig.LoadDefaultConfig(
			ctx, awsConfig.WithRegion(awsRegion),
			awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.LocalS3ID, cfg.LocalS3Secret, "")),
		)
		if err != nil {
			return nil, err
		}

		s3Client = dps3.NewClientWithConfig(bucketName, config, func(options *s3.Options) {
			options.BaseEndpoint = aws.String(cfg.LocalS3URL)
			options.UsePathStyle = true
		})

	} else {
		config, err := awsConfig.LoadDefaultConfig(ctx, awsConfig.WithRegion(awsRegion))
		if err != nil {
			return nil, err
		}

		s3Client = dps3.NewClientWithConfig(bucketName, config)
	}

	return s3Client, nil
}

// GetKafkaConsumer returns a Kafka Consumer group
var GetKafkaConsumer = func(ctx context.Context, cfg *config.Kafka, topic string) (kafka.IConsumerGroup, error) {
	if cfg == nil {
		return nil, errors.New("cannot create a kafka consumer without kafka config")
	}
	kafkaOffset := kafka.OffsetNewest
	if cfg.OffsetOldest {
		kafkaOffset = kafka.OffsetOldest
	}
	cgConfig := &kafka.ConsumerGroupConfig{
		BrokerAddrs:       cfg.Addr,
		Topic:             topic,
		GroupName:         cfg.ImageUploadedGroup,
		MinBrokersHealthy: &cfg.ConsumerMinBrokersHealthy,
		KafkaVersion:      &cfg.Version,
		Offset:            &kafkaOffset,
	}
	if cfg.SecProtocol == config.KafkaTLSProtocol {
		cgConfig.SecurityConfig = kafka.GetSecurityConfig(
			cfg.SecCACerts,
			cfg.SecClientCert,
			cfg.SecClientKey,
			cfg.SecSkipVerify,
		)
	}
	return kafka.NewConsumerGroup(ctx, cgConfig)
}

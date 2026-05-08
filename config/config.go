package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

// KafkaTLSProtocol informs service to use TLS protocol for kafka
const KafkaTLSProtocol = "TLS"

// Config represents service configuration for dp-image-importer
type Config struct {
	BindAddr                   string        `envconfig:"BIND_ADDR"`
	ServiceAuthToken           string        `envconfig:"SERVICE_AUTH_TOKEN"             json:"-"`
	AwsRegion                  string        `envconfig:"AWS_REGION"`
	GracefulShutdownTimeout    time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	HealthCheckInterval        time.Duration `envconfig:"HEALTHCHECK_INTERVAL"`
	HealthCheckCriticalTimeout time.Duration `envconfig:"HEALTHCHECK_CRITICAL_TIMEOUT"`
	ImageAPIURL                string        `envconfig:"IMAGE_API_URL"`
	S3PrivateBucketName        string        `envconfig:"S3_PRIVATE_BUCKET_NAME"`
	S3UploadedBucketName       string        `envconfig:"S3_UPLOADED_BUCKET_NAME"`
	DownloadServiceURL         string        `envconfig:"DOWNLOAD_SERVICE_URL"`
	LocalS3URL                 string        `envconfig:"S3_LOCAL_URL"`
	LocalS3ID                  string        `envconfig:"S3_LOCAL_ID"`
	LocalS3Secret              string        `envconfig:"S3_LOCAL_SECRET"`
	StopConsumingOnUnhealthy   bool          `envconfig:"STOP_CONSUMING_ON_UNHEALTHY"`
	Kafka                      *Kafka
}

// Kafka contains the config required to connect to Kafka
type Kafka struct {
	Addr                      []string `envconfig:"KAFKA_ADDR"`
	Version                   string   `envconfig:"KAFKA_VERSION"`
	OffsetOldest              bool     `envconfig:"KAFKA_OFFSET_OLDEST"`
	SecProtocol               string   `envconfig:"KAFKA_SEC_PROTO"`
	SecCACerts                string   `envconfig:"KAFKA_SEC_CA_CERTS"       json:"-"`
	SecClientKey              string   `envconfig:"KAFKA_SEC_CLIENT_KEY"     json:"-"`
	SecClientCert             string   `envconfig:"KAFKA_SEC_CLIENT_CERT"    json:"-"`
	SecSkipVerify             bool     `envconfig:"KAFKA_SEC_SKIP_VERIFY"`
	NumWorkers                int      `envconfig:"KAFKA_CONSUMER_WORKERS"`
	ImageUploadedGroup        string   `envconfig:"IMAGE_UPLOADED_GROUP"`
	ImageUploadedTopic        string   `envconfig:"IMAGE_UPLOADED_TOPIC"`
	ConsumerMinBrokersHealthy int      `envconfig:"KAFKA_CONSUMER_MIN_BROKERS_HEALTHY"`
}

var cfg *Config

// Get returns the default config with any modifications through environment
// variables
func Get() (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg := &Config{
		BindAddr:                   "localhost:24800",
		ServiceAuthToken:           "4424A9F2-B903-40F4-85F1-240107D1AFAF",
		AwsRegion:                  "eu-west-1",
		GracefulShutdownTimeout:    5 * time.Second,
		HealthCheckInterval:        30 * time.Second,
		HealthCheckCriticalTimeout: 90 * time.Second,
		ImageAPIURL:                "http://localhost:24700",
		S3PrivateBucketName:        "csv-exported",
		S3UploadedBucketName:       "dp-frontend-florence-file-uploads",
		DownloadServiceURL:         "http://localhost:23600",
		StopConsumingOnUnhealthy:   true,
		Kafka: &Kafka{
			Addr:                      []string{"localhost:9092", "localhost:9093", "localhost:9094"},
			Version:                   "1.0.2",
			OffsetOldest:              true,
			NumWorkers:                1,
			ImageUploadedGroup:        "dp-image-importer",
			ImageUploadedTopic:        "image-uploaded",
			ConsumerMinBrokersHealthy: 1,
		},
	}

	return cfg, envconfig.Process("", cfg)
}

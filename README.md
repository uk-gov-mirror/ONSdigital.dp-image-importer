# dp-image-importer

ONS service that imports uploaded images and adds them to a private bucket

## Getting started

* Run `make debug`

The service runs in the background consuming messages from Kafka. The messages are produced by the
[image API](https://github.com/ONSdigital/dp-image-api) and an example image can be created using the helper script,
`make produce`.

### Dependencies

* Requires running…
  * [kafka & kafka](https://github.com/ONSdigital/dp/blob/master/guides/INSTALLING.md#prerequisites)
  * [zebedee](https://github.com/ONSdigital/zebedee)
  * [dp-image-api](https://github.com/ONSdigital/dp-image-api)
  * [AWS S3 access](https://github.com/ONSdigital/dp/blob/master/guides/AWS_CREDENTIALS.md)
* No further dependencies other than those defined in `go.mod`

### Tools

To run some of our tests you will need additional tooling:

#### Audit

We use `dis-vulncheck` to do auditing, which you will [need to install](https://github.com/ONSdigital/dis-vulncheck).

#### Linting

We use v2 of golangci-lint, which you will [need to install](https://golangci-lint.run/docs/welcome/install).

### Configuration

| Environment variable               | Default                                        | Description
|------------------------------------|------------------------------------------------|--------------------------------------------------------------------------------------------------------------------
| BIND_ADDR                          | localhost:24800                                | The host and port to bind to
| SERVICE_AUTH_TOKEN                 | -                                              | The service token for this app
| AWS_REGION                         | eu-west-2                                      | The AWS region
| GRACEFUL_SHUTDOWN_TIMEOUT          | 5s                                             | The graceful shutdown timeout in seconds (`time.Duration` format)
| HEALTHCHECK_INTERVAL               | 30s                                            | Time between self-healthchecks (`time.Duration` format)
| HEALTHCHECK_CRITICAL_TIMEOUT       | 90s                                            | Time to wait until an unhealthy dependent propagates its state to make this app unhealthy (`time.Duration` format)
| IMAGE_API_URL                      | <http://localhost:24700>                       | The image api url
| KAFKA_ADDR                         | `localhost:9092,localhost:9093,localhost:9094` | The address of Kafka brokers (comma-separated values)
| KAFKA_CONSUMER_MIN_BROKERS_HEALTHY | 1                                              | Then minimum bumber of brokers required in order to be considered healthy
| KAFKA_OFFSET_OLDEST                | true                                           | Start processing Kafka messages in order from the oldest in the queue
| KAFKA_VERSION                      | `1.0.2`                                        | The version of Kafka
| KAFKA_SEC_PROTO                    | _unset_            (only `TLS`)                | if set to `TLS`, kafka connections will use TLS
| KAFKA_SEC_CLIENT_KEY               | _unset_                                        | PEM [2] for the client key (optional, used for client auth) [1]
| KAFKA_SEC_CLIENT_CERT              | _unset_                                        | PEM [2] for the client certificate (optional, used for client auth) [1]
| KAFKA_SEC_CA_CERTS                 | _unset_                                        | PEM [2] of CA cert chain if using private CA for the server cert [1]
| KAFKA_SEC_SKIP_VERIFY              | false                                          | ignore server certificate issues if set to `true` [1]
| KAFKA_CONSUMER_WORKERS             | 1                                              | The maximum number of parallel kafka consumers
| IMAGE_UPLOADED_GROUP               | dp-image-importer                              | The consumer group this application to consume ImageUploaded messages
| IMAGE_UPLOADED_TOPIC               | image-uploaded                                 | The name of the topic to consume messages from
| S3_PRIVATE_BUCKET_NAME             | ons-dp-sandbox-encrypted-datasets              | Name of the S3 bucket used to store generated images
| S3_UPLOADED_BUCKET_NAME            | ons-dp-sandbox-publishing-uploaded-datasets    | Name of the S3 bucket used to read original images from
| S3_LOCAL_URL                       |                                                | S3 Configuration for integration tests
| S3_LOCAL_ID                        |                                                | S3 Configuration for integration tests
| S3_LOCAL_SECRET                    |                                                | S3 Configuration for integration tests
| STOP_CONSUMING_ON_UNHEALTHY        | true                                           | Application stops consuming kafka messages if application is in unhealthy state
| DOWNLOAD_SERVICE_URL               | <http://localhost:23600>                       | The public address of the download service

**Notes:**

1. For more info, see the [kafka TLS examples documentation](https://github.com/ONSdigital/dp-kafka/tree/main/examples#tls)

### Healthcheck

 The `/health` endpoint returns the current status of the service. Dependent services are health checked on an interval defined by the `HEALTHCHECK_INTERVAL` environment variable.

 On a development machine a request to the health check endpoint can be made by:

 `curl localhost:24800/health`

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2025, Office for National Statistics (<https://www.ons.gov.uk>)

Released under MIT license, see [LICENSE](LICENSE.md) for details.

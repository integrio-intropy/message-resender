# message-resender

Kubernetes operator that automatically retries dead-lettered messages using declarative policies.

## Description

When a message consumer fails to process a message, the message broker routes it to a dead-letter queue. These messages often represent transient failures (downstream outage, rate limiting, temporary bugs) and can be safely retried.

message-resender automates this: for each `ResendPolicy` custom resource, the operator periodically drains the corresponding DLQ and republishes messages to the origin queue. It tracks drain results in the resource status and exposes Prometheus metrics.

### Supported brokers

| Broker | Flag | DLQ model |
|--------|------|-----------|
| **RabbitMQ** | `--broker-type=rabbitmq` | Configurable prefix (default `dlq-`) or per-queue override via `spec.dlqName` |
| **Azure Service Bus** | `--broker-type=azureservicebus` | Built-in `$deadletterqueue` sub-queue. Supports both queues and topic subscriptions (`topic/subscription` in `spec.queueName`) |

## Getting Started

### Prerequisites

- Go 1.25+
- Docker 17.03+
- kubectl v1.11.3+
- Access to a Kubernetes cluster

### Build and deploy

```sh
# Build the container image
make docker-build IMG=<your-registry>/message-resender:<tag>

# Push to your registry
make docker-push IMG=<your-registry>/message-resender:<tag>

# Install CRDs
make install

# Deploy the operator
make deploy IMG=<your-registry>/message-resender:<tag>
```

The operator requires broker credentials. Create a secret and reference it in the manager deployment:

**RabbitMQ:**
```sh
kubectl create secret generic rabbitmq-credentials \
  -n message-resender-system \
  --from-literal=dsn='amqp://user:pass@rabbitmq.example.com:5672/vhost'
```

**Azure Service Bus (connection string):**
```sh
kubectl create secret generic asb-credentials \
  -n message-resender-system \
  --from-literal=connection-string='Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...'
```

**Azure Service Bus (managed identity):**
No secret needed — pass `--asb-namespace=mynamespace.servicebus.windows.net` and configure workload identity on the pod.

## Configuration

### Operator flags

| Flag | Default | Env fallback | Description |
|------|---------|-------------|-------------|
| `--broker-type` | `rabbitmq` | — | Broker type: `rabbitmq` or `azureservicebus` |
| `--rabbitmq-dsn` | — | `RABBITMQ_DSN` | RabbitMQ connection string |
| `--dlq-prefix` | `dlq-` | — | Default DLQ name prefix (RabbitMQ only) |
| `--asb-connection-string` | — | `ASB_CONNECTION_STRING` | Azure Service Bus connection string |
| `--asb-namespace` | — | `ASB_NAMESPACE` | Azure Service Bus namespace (for managed identity) |
| `--drain-interval` | `5m` | — | Interval between drain runs per queue |
| `--max-concurrent-drains` | `10` | — | Maximum concurrent drain operations |

### Prometheus metrics

All metrics carry `queue` and `host` labels.

| Metric | Type | Description |
|--------|------|-------------|
| `resend_messages_processed_total` | Counter | Messages dequeued from a DLQ |
| `resend_messages_succeeded_total` | Counter | Messages successfully republished |
| `resend_messages_failed_total` | Counter | Messages that failed to republish |
| `resend_last_run_timestamp_seconds` | Gauge | Unix timestamp of last drain run |
| `resend_run_duration_seconds` | Histogram | Duration of each drain run |

## CRD Reference

### ResendPolicy

```yaml
apiVersion: messaging.intropy.io/v1
kind: ResendPolicy
metadata:
  name: orders
spec:
  # Origin queue whose DLQ should be drained (required).
  # For Azure Service Bus topic subscriptions, use "topic/subscription".
  queueName: orders

  # Override the DLQ name derivation (optional, RabbitMQ only).
  # When omitted, the DLQ name is derived as <dlqPrefix><queueName>.
  dlqName: ""
```

### Status

```yaml
status:
  conditions:
    - type: Ready
      status: "True"
      reason: DrainSucceeded
      message: "Processed 5 messages (5 succeeded, 0 failed)"
  observedGeneration: 1
  lastRunTime: "2026-03-06T12:00:00Z"
  lastRunStats:
    processed: 5
    succeeded: 5
    failed: 0
```

### Examples

**RabbitMQ — simple queue:**
```yaml
apiVersion: messaging.intropy.io/v1
kind: ResendPolicy
metadata:
  name: orders
spec:
  queueName: orders
```

**RabbitMQ — custom DLQ name:**
```yaml
apiVersion: messaging.intropy.io/v1
kind: ResendPolicy
metadata:
  name: payments
spec:
  queueName: payments
  dlqName: payments.dead-letter
```

**Azure Service Bus — queue:**
```yaml
apiVersion: messaging.intropy.io/v1
kind: ResendPolicy
metadata:
  name: orders
spec:
  queueName: orders
```

**Azure Service Bus — topic subscription:**
```yaml
apiVersion: messaging.intropy.io/v1
kind: ResendPolicy
metadata:
  name: orders-processor
spec:
  queueName: orders-topic/processor-subscription
```

## Uninstall

```sh
kubectl delete -k config/samples/
make undeploy
make uninstall
```

## Contributing

Run `make help` for all available targets. Key commands:

```sh
make test        # Unit tests + envtest
make lint        # Lint with golangci-lint
make manifests   # Regenerate CRDs and RBAC from markers
```

## License

This project is licensed under the [MIT License](LICENSE).

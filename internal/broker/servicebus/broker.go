package servicebus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/go-logr/logr"
	"k8s.io/klog/v2"

	"github.com/integrio-intropy/message-resender/internal/broker"
)

const (
	// retryCountExtensionName is the CloudEvent extension that consumers read
	// to detect a retry. Dapr only exposes extensions from the CloudEvent in
	// the message body, not from Service Bus application properties, and
	// extension names cannot contain underscores.
	retryCountExtensionName = "retrycount"
	cloudEventSpecVersion   = "specversion"
	receiveTimeout          = 10 * time.Second
)

// messageReceiver abstracts the subset of *azservicebus.Receiver used by drain.
// *azservicebus.Receiver satisfies this interface.
type messageReceiver interface {
	ReceiveMessages(ctx context.Context, maxMessages int, options *azservicebus.ReceiveMessagesOptions) ([]*azservicebus.ReceivedMessage, error)
	CompleteMessage(ctx context.Context, message *azservicebus.ReceivedMessage, options *azservicebus.CompleteMessageOptions) error
}

// messageSender abstracts the subset of *azservicebus.Sender used by drain.
// *azservicebus.Sender satisfies this interface.
type messageSender interface {
	SendMessage(ctx context.Context, message *azservicebus.Message, options *azservicebus.SendMessageOptions) error
}

// Broker implements broker.Broker for Azure Service Bus.
type Broker struct {
	client *azservicebus.Client
	logger logr.Logger
}

// New creates an Azure Service Bus broker. Exactly one of connStr or namespace
// must be non-empty.
//   - connStr: authenticates via connection string
//   - namespace: authenticates via DefaultAzureCredential (managed identity)
func New(connStr, namespace string, logger logr.Logger) (*Broker, error) {
	var client *azservicebus.Client
	var err error

	switch {
	case connStr != "":
		client, err = azservicebus.NewClientFromConnectionString(connStr, nil)
	case namespace != "":
		var cred *azidentity.DefaultAzureCredential
		cred, err = azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("create Azure credential: %w", err)
		}
		client, err = azservicebus.NewClient(namespace, cred, nil)
	default:
		return nil, fmt.Errorf("either connection string or namespace is required")
	}

	if err != nil {
		return nil, fmt.Errorf("create ASB client: %w", err)
	}
	return &Broker{client: client, logger: logger}, nil
}

// Drain implements broker.Broker.
func (b *Broker) Drain(ctx context.Context, queue broker.QueueInfo, runStart time.Time) (broker.DrainResult, error) {
	entityName, subscriptionName := parseQueueName(queue.QueueName)

	var receiver *azservicebus.Receiver
	var err error
	if subscriptionName == "" {
		receiver, err = b.client.NewReceiverForQueue(entityName, &azservicebus.ReceiverOptions{
			ReceiveMode: azservicebus.ReceiveModePeekLock,
			SubQueue:    azservicebus.SubQueueDeadLetter,
		})
	} else {
		receiver, err = b.client.NewReceiverForSubscription(entityName, subscriptionName, &azservicebus.ReceiverOptions{
			ReceiveMode: azservicebus.ReceiveModePeekLock,
			SubQueue:    azservicebus.SubQueueDeadLetter,
		})
	}
	if err != nil {
		return broker.DrainResult{}, fmt.Errorf("create DLQ receiver for %s: %w", queue.QueueName, err)
	}
	defer func() { _ = receiver.Close(ctx) }()

	sender, err := b.client.NewSender(entityName, nil)
	if err != nil {
		return broker.DrainResult{}, fmt.Errorf("create sender for %s: %w", entityName, err)
	}
	defer func() { _ = sender.Close(ctx) }()

	ctx = logr.NewContext(ctx, b.logger)
	return drain(ctx, receiver, sender, queue.QueueName, runStart)
}

// Close releases the ASB client connection.
func (b *Broker) Close() error {
	return b.client.Close(context.Background())
}

// parseQueueName splits "topic/subscription" into its parts.
// If there is no "/", returns (queueName, "").
func parseQueueName(name string) (entity, subscription string) {
	if before, after, ok := strings.Cut(name, "/"); ok {
		return before, after
	}
	return name, ""
}

// drain runs the DLQ drain loop. Separated from Broker for unit testing
// with mock receiver/sender. The logger is extracted from ctx.
func drain(
	ctx context.Context,
	receiver messageReceiver,
	sender messageSender,
	queueName string,
	runStart time.Time,
) (broker.DrainResult, error) {
	var result broker.DrainResult
	log := logr.FromContextOrDiscard(ctx).WithValues("queue", queueName)

	for {
		if err := ctx.Err(); err != nil {
			log.Info("Context cancelled, stopping drain")
			return result, nil
		}

		receiveCtx, cancel := context.WithTimeout(ctx, receiveTimeout)
		messages, err := receiver.ReceiveMessages(receiveCtx, 1, nil)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				log.Info("Context cancelled, stopping drain")
				return result, nil
			}
			if errors.Is(err, context.DeadlineExceeded) {
				log.V(1).Info("Receive timed out, no more messages available")
				break
			}
			return result, fmt.Errorf("receive from DLQ for %s: %w", queueName, err)
		}

		if len(messages) == 0 {
			break
		}

		for _, msg := range messages {
			result.Processed++

			if msg.EnqueuedTime == nil || !msg.EnqueuedTime.Truncate(time.Second).Before(runStart.Truncate(time.Second)) {
				log.Info("Reached message from current window, stopping",
					"enqueuedTime", klog.SafePtr(msg.EnqueuedTime), "runStart", runStart)
				// Under PeekLock, not completing returns the message to the DLQ.
				return result, nil
			}

			mapped := mapMessage(msg)
			if err := sender.SendMessage(ctx, mapped, nil); err != nil {
				log.Error(err, "Failed to send message, leaving in DLQ")
				result.Failed++
				continue
			}

			if err := receiver.CompleteMessage(ctx, msg, nil); err != nil {
				log.Error(err, "Failed to complete message")
				result.Failed++
				continue
			}

			result.Succeeded++
		}
	}

	return result, nil
}

// mapMessage builds a new ASB message preserving the original's properties.
// When the body is a structured CloudEvent, the retrycount extension is set
// so the consumer sees the message as a retry.
func mapMessage(msg *azservicebus.ReceivedMessage) *azservicebus.Message {
	return &azservicebus.Message{
		ApplicationProperties: msg.ApplicationProperties,
		Body:                  setCloudEventRetryCount(msg.Body),
		ContentType:           msg.ContentType,
		CorrelationID:         msg.CorrelationID,
		MessageID:             &msg.MessageID,
		PartitionKey:          msg.PartitionKey,
		ReplyTo:               msg.ReplyTo,
		ReplyToSessionID:      msg.ReplyToSessionID,
		SessionID:             msg.SessionID,
		Subject:               msg.Subject,
	}
}

// parseCloudEvent decodes body as a structured-mode JSON CloudEvent. It
// reports false when the body is not a JSON object with a specversion
// attribute.
func parseCloudEvent(body []byte) (map[string]json.RawMessage, bool) {
	var event map[string]json.RawMessage
	if err := json.Unmarshal(body, &event); err != nil {
		return nil, false
	}
	if _, ok := event[cloudEventSpecVersion]; !ok {
		return nil, false
	}
	return event, true
}

// setCloudEventRetryCount returns body with the retrycount extension set to
// one more than its current value (or 1) when body is a structured
// CloudEvent, and body unchanged otherwise. Other attributes and data are
// kept as-is, though key order and whitespace may change.
func setCloudEventRetryCount(body []byte) []byte {
	event, ok := parseCloudEvent(body)
	if !ok {
		return body
	}
	retryCount := 0
	_ = json.Unmarshal(event[retryCountExtensionName], &retryCount)
	event[retryCountExtensionName] = json.RawMessage(fmt.Sprint(retryCount + 1))

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(event); err != nil {
		return body
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

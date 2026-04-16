package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/integrio-intropy/message-resender/internal/broker"
)

// messageGetter fetches individual messages from a queue synchronously.
// *amqp.Channel satisfies this interface.
type messageGetter interface {
	Get(queue string, autoAck bool) (amqp.Delivery, bool, error)
}

// messagePublisher publishes a message to a queue.
// *amqp.Channel satisfies this interface.
type messagePublisher interface {
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
}

// Broker implements broker.Broker for RabbitMQ.
type Broker struct {
	dsn       string
	dlqPrefix string
	conn      *amqp.Connection
	mu        sync.Mutex
	logger    logr.Logger
}

// New creates a RabbitMQ broker and dials immediately to fail fast.
func New(dsn, dlqPrefix string, logger logr.Logger) (*Broker, error) {
	b := &Broker{
		dsn:       dsn,
		dlqPrefix: dlqPrefix,
		logger:    logger,
	}
	if err := b.dial(); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Broker) dial() error {
	conn, err := amqp.Dial(b.dsn)
	if err != nil {
		return fmt.Errorf("dial RabbitMQ: %w", err)
	}
	b.conn = conn
	return nil
}

func (b *Broker) channel() (*amqp.Channel, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.conn == nil || b.conn.IsClosed() {
		if err := b.dial(); err != nil {
			return nil, err
		}
	}
	return b.conn.Channel()
}

// Drain implements broker.Broker.
func (b *Broker) Drain(ctx context.Context, queue broker.QueueInfo, runStart time.Time) (broker.DrainResult, error) {
	ch, err := b.channel()
	if err != nil {
		return broker.DrainResult{}, fmt.Errorf("open channel for %s: %w", queue.QueueName, err)
	}
	defer func() { _ = ch.Close() }()

	dlqName := queue.DLQOverride
	if dlqName == "" {
		dlqName = b.dlqPrefix + queue.QueueName
	}

	ctx = logr.NewContext(ctx, b.logger)
	return drain(ctx, ch, ch, queue.QueueName, dlqName, runStart)
}

// Close releases the AMQP connection.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn != nil && !b.conn.IsClosed() {
		return b.conn.Close()
	}
	return nil
}

// drain runs the DLQ drain loop. Separated from Broker for unit testing
// with mock getter/publisher. The logger is extracted from ctx.
func drain(
	ctx context.Context,
	getter messageGetter,
	publisher messagePublisher,
	queueName, dlqName string,
	runStart time.Time,
) (broker.DrainResult, error) {
	var result broker.DrainResult
	log := logr.FromContextOrDiscard(ctx).WithValues("queue", queueName, "dlq", dlqName)

	for {
		if err := ctx.Err(); err != nil {
			log.Info("Context cancelled, stopping drain")
			return result, nil
		}

		msg, ok, err := getter.Get(dlqName, false)
		if err != nil {
			return result, fmt.Errorf("get from %s: %w", dlqName, err)
		}
		if !ok {
			break
		}

		result.Processed++

		deathTime, err := parseDeathTime(msg)
		if err != nil {
			// No x-death header — message was manually placed on the DLQ.
			// Republish directly without a boundary check.
			publishAndAck(publisher, queueName, msg, &result, log)
			continue
		}

		if !deathTime.Truncate(time.Second).Before(runStart.Truncate(time.Second)) {
			log.Info("Reached message from current window, stopping",
				"deathTime", deathTime, "runStart", runStart)
			_ = msg.Nack(false, true)
			break
		}

		publishAndAck(publisher, queueName, msg, &result, log)
	}

	return result, nil
}

// publishAndAck publishes msg to the origin queue and acks it from the DLQ.
// On publish failure the message is nacked and requeued.
func publishAndAck(publisher messagePublisher, queueName string, msg amqp.Delivery, result *broker.DrainResult, log logr.Logger) {
	err := publisher.Publish("", queueName, false, false, amqp.Publishing{
		// Headers intentionally omitted to strip x-death history.
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		DeliveryMode:    msg.DeliveryMode,
		Priority:        msg.Priority,
		CorrelationId:   msg.CorrelationId,
		ReplyTo:         msg.ReplyTo,
		Expiration:      msg.Expiration,
		MessageId:       msg.MessageId,
		Timestamp:       msg.Timestamp,
		Type:            msg.Type,
		UserId:          msg.UserId,
		AppId:           msg.AppId,
		Body:            msg.Body,
	})
	if err != nil {
		log.Error(err, "Failed to publish message, requeuing", "queue", queueName)
		_ = msg.Nack(false, true)
		result.Failed++
		return
	}

	if err := msg.Ack(false); err != nil {
		log.Error(err, "Failed to ack message")
	}
	result.Succeeded++
}

func parseDeathTime(msg amqp.Delivery) (time.Time, error) {
	xDeath, ok := msg.Headers["x-death"].([]any)
	if !ok || len(xDeath) == 0 {
		return time.Time{}, fmt.Errorf("missing or invalid x-death header")
	}

	table, ok := xDeath[0].(amqp.Table)
	if !ok {
		return time.Time{}, fmt.Errorf("x-death entry is not a table")
	}

	t, ok := table["time"].(time.Time)
	if !ok {
		return time.Time{}, fmt.Errorf("x-death.time is missing or not a time.Time")
	}

	return t, nil
}

package rabbitmq

import (
	"context"
	"fmt"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/integrio-intropy/message-resender/internal/broker"
)

// --- mocks ---

type mockAcknowledger struct {
	acked  []uint64
	nacked []struct {
		tag     uint64
		requeue bool
	}
}

func (m *mockAcknowledger) Ack(tag uint64, _ bool) error {
	m.acked = append(m.acked, tag)
	return nil
}

func (m *mockAcknowledger) Nack(tag uint64, _ bool, requeue bool) error {
	m.nacked = append(m.nacked, struct {
		tag     uint64
		requeue bool
	}{tag, requeue})
	return nil
}

func (m *mockAcknowledger) Reject(_ uint64, _ bool) error { return nil }

type mockGetter struct {
	messages []amqp.Delivery
	idx      int
	err      error
}

func (m *mockGetter) Get(_ string, _ bool) (amqp.Delivery, bool, error) {
	if m.err != nil {
		return amqp.Delivery{}, false, m.err
	}
	if m.idx >= len(m.messages) {
		return amqp.Delivery{}, false, nil
	}
	msg := m.messages[m.idx]
	m.idx++
	return msg, true, nil
}

type mockPublisher struct {
	published []amqp.Publishing
	err       error
}

func (m *mockPublisher) Publish(_, _ string, _, _ bool, msg amqp.Publishing) error {
	if m.err != nil {
		return m.err
	}
	m.published = append(m.published, msg)
	return nil
}

// --- helpers ---

func makeDelivery(tag uint64, acker amqp.Acknowledger, deathTime time.Time, body []byte) amqp.Delivery {
	return amqp.Delivery{
		Acknowledger: acker,
		DeliveryTag:  tag,
		Headers: amqp.Table{
			"x-death": []any{
				amqp.Table{
					"time": deathTime,
				},
			},
		},
		Body:        body,
		ContentType: "application/json",
	}
}

func makeDeliveryNoXDeath(tag uint64, acker amqp.Acknowledger) amqp.Delivery {
	return amqp.Delivery{
		Acknowledger: acker,
		DeliveryTag:  tag,
		Headers:      amqp.Table{},
		Body:         []byte(`{}`),
	}
}

func runDrain(getter messageGetter, publisher messagePublisher, startTime time.Time) (broker.DrainResult, error) {
	return drain(context.Background(), getter, publisher, "products", "dlq-products", startTime)
}

// --- tests ---

func TestDrain_EmptyQueue(t *testing.T) {
	getter := &mockGetter{}
	publisher := &mockPublisher{}

	result, err := runDrain(getter, publisher, time.Now())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Processed != 0 || result.Succeeded != 0 || result.Failed != 0 {
		t.Errorf("expected all zeros, got %+v", result)
	}
	if len(publisher.published) != 0 {
		t.Error("expected no messages published")
	}
}

func TestDrain_AllMessagesBeforeStartTime(t *testing.T) {
	startTime := time.Now()
	acker := &mockAcknowledger{}

	getter := &mockGetter{
		messages: []amqp.Delivery{
			makeDelivery(1, acker, startTime.Add(-10*time.Minute), []byte(`{"id":1}`)),
			makeDelivery(2, acker, startTime.Add(-5*time.Minute), []byte(`{"id":2}`)),
		},
	}
	publisher := &mockPublisher{}

	result, err := runDrain(getter, publisher, startTime)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Processed != 2 {
		t.Errorf("expected 2 processed, got %d", result.Processed)
	}
	if result.Succeeded != 2 {
		t.Errorf("expected 2 succeeded, got %d", result.Succeeded)
	}
	if result.Failed != 0 {
		t.Errorf("expected 0 failed, got %d", result.Failed)
	}
	if len(acker.acked) != 2 {
		t.Errorf("expected 2 acks, got %d", len(acker.acked))
	}
	if len(publisher.published) != 2 {
		t.Errorf("expected 2 published, got %d", len(publisher.published))
	}
}

func TestDrain_StopsAtBoundaryMessage(t *testing.T) {
	startTime := time.Now()
	acker := &mockAcknowledger{}

	getter := &mockGetter{
		messages: []amqp.Delivery{
			makeDelivery(1, acker, startTime.Add(-10*time.Minute), []byte(`{"id":1}`)),
			makeDelivery(2, acker, startTime, []byte(`{"id":2}`)),
			makeDelivery(3, acker, startTime.Add(-1*time.Minute), []byte(`{"id":3}`)),
		},
	}
	publisher := &mockPublisher{}

	result, err := runDrain(getter, publisher, startTime)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Processed != 2 {
		t.Errorf("expected 2 processed (1 success + 1 boundary), got %d", result.Processed)
	}
	if result.Succeeded != 1 {
		t.Errorf("expected 1 succeeded, got %d", result.Succeeded)
	}
	if len(acker.nacked) != 1 || !acker.nacked[0].requeue {
		t.Errorf("expected boundary message to be nacked with requeue=true, nacked=%+v", acker.nacked)
	}
	if len(publisher.published) != 1 {
		t.Errorf("expected 1 published, got %d", len(publisher.published))
	}
}

func TestDrain_PublishFailure_NacksAndContinues(t *testing.T) {
	startTime := time.Now()
	acker := &mockAcknowledger{}

	getter := &mockGetter{
		messages: []amqp.Delivery{
			makeDelivery(1, acker, startTime.Add(-10*time.Minute), []byte(`{"id":1}`)),
			makeDelivery(2, acker, startTime.Add(-5*time.Minute), []byte(`{"id":2}`)),
		},
	}
	publisher := &mockPublisher{err: fmt.Errorf("broker unavailable")}

	result, err := runDrain(getter, publisher, startTime)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Failed != 2 {
		t.Errorf("expected 2 failed, got %d", result.Failed)
	}
	if result.Succeeded != 0 {
		t.Errorf("expected 0 succeeded, got %d", result.Succeeded)
	}
	if len(acker.nacked) != 2 {
		t.Errorf("expected 2 nacks, got %d", len(acker.nacked))
	}
	for _, n := range acker.nacked {
		if !n.requeue {
			t.Error("expected nack with requeue=true")
		}
	}
}

func TestDrain_MissingXDeath_RepublishesDirectly(t *testing.T) {
	startTime := time.Now()
	acker := &mockAcknowledger{}

	getter := &mockGetter{
		messages: []amqp.Delivery{
			makeDeliveryNoXDeath(1, acker),
			makeDelivery(2, acker, startTime.Add(-5*time.Minute), []byte(`{"id":2}`)),
		},
	}
	publisher := &mockPublisher{}

	result, err := runDrain(getter, publisher, startTime)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Processed != 2 {
		t.Errorf("expected 2 processed, got %d", result.Processed)
	}
	if result.Succeeded != 2 {
		t.Errorf("expected 2 succeeded, got %d", result.Succeeded)
	}
	if len(acker.nacked) != 0 {
		t.Errorf("expected 0 nacks, got %d", len(acker.nacked))
	}
}

func TestDrain_GetError_ReturnsError(t *testing.T) {
	getter := &mockGetter{err: fmt.Errorf("channel closed")}
	publisher := &mockPublisher{}

	result, err := runDrain(getter, publisher, time.Now())

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if result.Processed != 0 {
		t.Errorf("expected 0 processed on get error, got %d", result.Processed)
	}
	if len(publisher.published) != 0 {
		t.Error("expected no messages published on get error")
	}
}

func TestDrain_StripsHeaders(t *testing.T) {
	startTime := time.Now()
	acker := &mockAcknowledger{}

	getter := &mockGetter{
		messages: []amqp.Delivery{
			makeDelivery(1, acker, startTime.Add(-10*time.Minute), []byte(`{"id":1}`)),
		},
	}
	publisher := &mockPublisher{}

	_, _ = runDrain(getter, publisher, startTime)

	if len(publisher.published) != 1 {
		t.Fatalf("expected 1 published message")
	}
	if len(publisher.published[0].Headers) != 0 {
		t.Errorf("expected headers to be stripped, got %v", publisher.published[0].Headers)
	}
}

func TestDrain_PreservesMessageProperties(t *testing.T) {
	startTime := time.Now()
	acker := &mockAcknowledger{}

	delivery := makeDelivery(1, acker, startTime.Add(-10*time.Minute), []byte(`{"id":1}`))
	delivery.ContentType = "application/json"
	delivery.CorrelationId = "corr-123"
	delivery.MessageId = "msg-456"

	getter := &mockGetter{messages: []amqp.Delivery{delivery}}
	publisher := &mockPublisher{}

	_, _ = runDrain(getter, publisher, startTime)

	if len(publisher.published) != 1 {
		t.Fatalf("expected 1 published message")
	}
	pub := publisher.published[0]
	if pub.ContentType != "application/json" {
		t.Errorf("expected ContentType preserved, got %s", pub.ContentType)
	}
	if pub.CorrelationId != "corr-123" {
		t.Errorf("expected CorrelationId preserved, got %s", pub.CorrelationId)
	}
	if pub.MessageId != "msg-456" {
		t.Errorf("expected MessageId preserved, got %s", pub.MessageId)
	}
}

func TestDrain_ContextCancelled_StopsGracefully(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	startTime := time.Now()
	acker := &mockAcknowledger{}
	getter := &mockGetter{
		messages: []amqp.Delivery{
			makeDelivery(1, acker, startTime.Add(-10*time.Minute), []byte(`{"id":1}`)),
		},
	}
	publisher := &mockPublisher{}

	result, err := drain(ctx, getter, publisher, "products", "dlq-products", startTime)

	if err != nil {
		t.Fatalf("expected nil error on context cancel, got %v", err)
	}
	if result.Processed != 0 {
		t.Errorf("expected 0 processed when context cancelled, got %d", result.Processed)
	}
}

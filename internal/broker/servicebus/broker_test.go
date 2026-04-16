package servicebus

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/integrio-intropy/message-resender/internal/broker"
)

// --- mocks ---

type mockReceiver struct {
	batches     [][]*azservicebus.ReceivedMessage
	idx         int
	err         error
	completed   []*azservicebus.ReceivedMessage
	completeErr error
}

func (m *mockReceiver) ReceiveMessages(_ context.Context, _ int, _ *azservicebus.ReceiveMessagesOptions) ([]*azservicebus.ReceivedMessage, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.idx >= len(m.batches) {
		return nil, nil
	}
	batch := m.batches[m.idx]
	m.idx++
	return batch, nil
}

func (m *mockReceiver) CompleteMessage(_ context.Context, msg *azservicebus.ReceivedMessage, _ *azservicebus.CompleteMessageOptions) error {
	if m.completeErr != nil {
		return m.completeErr
	}
	m.completed = append(m.completed, msg)
	return nil
}

type mockSender struct {
	sent []*azservicebus.Message
	err  error
}

func (m *mockSender) SendMessage(_ context.Context, msg *azservicebus.Message, _ *azservicebus.SendMessageOptions) error {
	if m.err != nil {
		return m.err
	}
	m.sent = append(m.sent, msg)
	return nil
}

// --- helpers ---

func makeMessage(enqueuedTime time.Time, body []byte, msgID string) *azservicebus.ReceivedMessage {
	t := enqueuedTime
	return &azservicebus.ReceivedMessage{
		EnqueuedTime: &t,
		Body:         body,
		MessageID:    msgID,
	}
}

func runTestDrain(receiver messageReceiver, sender messageSender, startTime time.Time) (broker.DrainResult, error) {
	return drain(context.Background(), receiver, sender, "test-queue", startTime)
}

// --- tests ---

func TestDrain_EmptyQueue(t *testing.T) {
	receiver := &mockReceiver{}
	sender := &mockSender{}

	result, err := runTestDrain(receiver, sender, time.Now())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Processed != 0 || result.Succeeded != 0 || result.Failed != 0 {
		t.Errorf("expected all zeros, got %+v", result)
	}
	if len(sender.sent) != 0 {
		t.Error("expected no messages sent")
	}
}

func TestDrain_AllMessagesBeforeStartTime(t *testing.T) {
	startTime := time.Now()

	receiver := &mockReceiver{
		batches: [][]*azservicebus.ReceivedMessage{
			{makeMessage(startTime.Add(-10*time.Minute), []byte(`{"id":1}`), "msg-1")},
			{makeMessage(startTime.Add(-5*time.Minute), []byte(`{"id":2}`), "msg-2")},
		},
	}
	sender := &mockSender{}

	result, err := runTestDrain(receiver, sender, startTime)

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
	if len(sender.sent) != 2 {
		t.Errorf("expected 2 sent, got %d", len(sender.sent))
	}
	if len(receiver.completed) != 2 {
		t.Errorf("expected 2 completed, got %d", len(receiver.completed))
	}
}

func TestDrain_StopsAtBoundaryMessage(t *testing.T) {
	startTime := time.Now()

	msg1 := makeMessage(startTime.Add(-10*time.Minute), []byte(`{"id":1}`), "msg-1")
	msg2 := makeMessage(startTime, []byte(`{"id":2}`), "msg-2")

	receiver := &mockReceiver{
		batches: [][]*azservicebus.ReceivedMessage{
			{msg1},
			{msg2},
		},
	}
	sender := &mockSender{}

	result, err := runTestDrain(receiver, sender, startTime)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Processed != 2 {
		t.Errorf("expected 2 processed (1 success + 1 boundary), got %d", result.Processed)
	}
	if result.Succeeded != 1 {
		t.Errorf("expected 1 succeeded, got %d", result.Succeeded)
	}
	if len(sender.sent) != 1 {
		t.Errorf("expected 1 sent, got %d", len(sender.sent))
	}
	// Boundary message should NOT be completed (PeekLock returns it to DLQ).
	if len(receiver.completed) != 1 {
		t.Errorf("expected 1 completed (only the first message), got %d", len(receiver.completed))
	}
}

func TestDrain_SendFailure_LeavesInDLQ(t *testing.T) {
	startTime := time.Now()

	receiver := &mockReceiver{
		batches: [][]*azservicebus.ReceivedMessage{
			{makeMessage(startTime.Add(-10*time.Minute), []byte(`{"id":1}`), "msg-1")},
			{makeMessage(startTime.Add(-5*time.Minute), []byte(`{"id":2}`), "msg-2")},
		},
	}
	sender := &mockSender{err: fmt.Errorf("service bus unavailable")}

	result, err := runTestDrain(receiver, sender, startTime)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Failed != 2 {
		t.Errorf("expected 2 failed, got %d", result.Failed)
	}
	if result.Succeeded != 0 {
		t.Errorf("expected 0 succeeded, got %d", result.Succeeded)
	}
	// Messages should NOT be completed — PeekLock will return them to DLQ.
	if len(receiver.completed) != 0 {
		t.Errorf("expected 0 completed on send failure, got %d", len(receiver.completed))
	}
}

func TestDrain_CompleteFailure_CountsAsFailed(t *testing.T) {
	startTime := time.Now()

	receiver := &mockReceiver{
		batches: [][]*azservicebus.ReceivedMessage{
			{makeMessage(startTime.Add(-10*time.Minute), []byte(`{"id":1}`), "msg-1")},
		},
		completeErr: fmt.Errorf("lock lost"),
	}
	sender := &mockSender{}

	result, err := runTestDrain(receiver, sender, startTime)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Processed != 1 {
		t.Errorf("expected 1 processed, got %d", result.Processed)
	}
	if result.Failed != 1 {
		t.Errorf("expected 1 failed, got %d", result.Failed)
	}
	if result.Succeeded != 0 {
		t.Errorf("expected 0 succeeded, got %d", result.Succeeded)
	}
	// Message was sent but not completed — it will reappear in DLQ next cycle.
	if len(sender.sent) != 1 {
		t.Errorf("expected 1 sent, got %d", len(sender.sent))
	}
}

func TestDrain_ReceiveError_ReturnsError(t *testing.T) {
	receiver := &mockReceiver{err: fmt.Errorf("connection reset")}
	sender := &mockSender{}

	result, err := runTestDrain(receiver, sender, time.Now())

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if result.Processed != 0 {
		t.Errorf("expected 0 processed on receive error, got %d", result.Processed)
	}
	if len(sender.sent) != 0 {
		t.Error("expected no messages sent on receive error")
	}
}

func TestDrain_ContextCancelled_StopsGracefully(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	startTime := time.Now()
	receiver := &mockReceiver{
		batches: [][]*azservicebus.ReceivedMessage{
			{makeMessage(startTime.Add(-10*time.Minute), []byte(`{"id":1}`), "msg-1")},
		},
	}
	sender := &mockSender{}

	result, err := drain(ctx, receiver, sender, "test-queue", startTime)

	if err != nil {
		t.Fatalf("expected nil error on context cancel, got %v", err)
	}
	if result.Processed != 0 {
		t.Errorf("expected 0 processed when context cancelled, got %d", result.Processed)
	}
}

func TestDrain_PreservesMessageProperties(t *testing.T) {
	startTime := time.Now()

	contentType := "application/json"
	correlationID := "corr-123"
	partitionKey := "pk-1"
	replyTo := "reply-queue"
	replyToSessionID := "session-reply-1"
	sessionID := "session-1"
	subject := "test-subject"

	msg := makeMessage(startTime.Add(-10*time.Minute), []byte(`{"id":1}`), "msg-1")
	msg.ContentType = &contentType
	msg.CorrelationID = &correlationID
	msg.PartitionKey = &partitionKey
	msg.ReplyTo = &replyTo
	msg.ReplyToSessionID = &replyToSessionID
	msg.SessionID = &sessionID
	msg.Subject = &subject
	msg.ApplicationProperties = map[string]any{"custom-key": "custom-value"}

	receiver := &mockReceiver{
		batches: [][]*azservicebus.ReceivedMessage{{msg}},
	}
	sender := &mockSender{}

	_, _ = runTestDrain(receiver, sender, startTime)

	if len(sender.sent) != 1 {
		t.Fatalf("expected 1 sent message, got %d", len(sender.sent))
	}
	sent := sender.sent[0]

	if sent.ContentType == nil || *sent.ContentType != "application/json" {
		t.Errorf("expected ContentType preserved, got %v", sent.ContentType)
	}
	if sent.CorrelationID == nil || *sent.CorrelationID != "corr-123" {
		t.Errorf("expected CorrelationID preserved, got %v", sent.CorrelationID)
	}
	if sent.MessageID == nil || *sent.MessageID != "msg-1" {
		t.Errorf("expected MessageID preserved, got %v", sent.MessageID)
	}
	if sent.PartitionKey == nil || *sent.PartitionKey != "pk-1" {
		t.Errorf("expected PartitionKey preserved, got %v", sent.PartitionKey)
	}
	if sent.ReplyTo == nil || *sent.ReplyTo != "reply-queue" {
		t.Errorf("expected ReplyTo preserved, got %v", sent.ReplyTo)
	}
	if sent.ReplyToSessionID == nil || *sent.ReplyToSessionID != "session-reply-1" {
		t.Errorf("expected ReplyToSessionID preserved, got %v", sent.ReplyToSessionID)
	}
	if sent.SessionID == nil || *sent.SessionID != "session-1" {
		t.Errorf("expected SessionID preserved, got %v", sent.SessionID)
	}
	if sent.Subject == nil || *sent.Subject != "test-subject" {
		t.Errorf("expected Subject preserved, got %v", sent.Subject)
	}
	// Custom app property should be preserved.
	if val, ok := sent.ApplicationProperties["custom-key"]; !ok || val != "custom-value" {
		t.Errorf("expected custom-key preserved, got %v", sent.ApplicationProperties)
	}
	// retry_count should be added.
	if val, ok := sent.ApplicationProperties[retryCountPropertyName]; !ok || val != 1 {
		t.Errorf("expected retry_count=1, got %v", sent.ApplicationProperties[retryCountPropertyName])
	}
}

func TestDrain_NilEnqueuedTime_StopsDrain(t *testing.T) {
	msg := &azservicebus.ReceivedMessage{
		EnqueuedTime: nil,
		Body:         []byte(`{"id":1}`),
		MessageID:    "msg-1",
	}

	receiver := &mockReceiver{
		batches: [][]*azservicebus.ReceivedMessage{{msg}},
	}
	sender := &mockSender{}

	result, err := runTestDrain(receiver, sender, time.Now())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Processed != 1 {
		t.Errorf("expected 1 processed, got %d", result.Processed)
	}
	if result.Succeeded != 0 {
		t.Errorf("expected 0 succeeded (boundary), got %d", result.Succeeded)
	}
	if len(sender.sent) != 0 {
		t.Errorf("expected 0 sent, got %d", len(sender.sent))
	}
}

func TestParseQueueName_Queue(t *testing.T) {
	entity, sub := parseQueueName("orders")
	if entity != "orders" {
		t.Errorf("expected entity=orders, got %s", entity)
	}
	if sub != "" {
		t.Errorf("expected empty subscription, got %s", sub)
	}
}

func TestParseQueueName_TopicSubscription(t *testing.T) {
	entity, sub := parseQueueName("orders-topic/my-sub")
	if entity != "orders-topic" {
		t.Errorf("expected entity=orders-topic, got %s", entity)
	}
	if sub != "my-sub" {
		t.Errorf("expected subscription=my-sub, got %s", sub)
	}
}

func TestGetRetryCount_NoProperty(t *testing.T) {
	msg := &azservicebus.ReceivedMessage{}
	count := getRetryCount(msg)
	if count != 1 {
		t.Errorf("expected 1, got %d", count)
	}
}

func TestGetRetryCount_ExistingCount(t *testing.T) {
	msg := &azservicebus.ReceivedMessage{
		ApplicationProperties: map[string]any{
			retryCountPropertyName: int64(3),
		},
	}
	count := getRetryCount(msg)
	if count != 4 {
		t.Errorf("expected 4 (3+1), got %d", count)
	}
}

func TestMapMessage_AddsRetryCount(t *testing.T) {
	msg := &azservicebus.ReceivedMessage{
		Body:      []byte(`{"id":1}`),
		MessageID: "msg-1",
	}

	mapped := mapMessage(msg)

	if val, ok := mapped.ApplicationProperties[retryCountPropertyName]; !ok || val != 1 {
		t.Errorf("expected retry_count=1, got %v", mapped.ApplicationProperties)
	}
}

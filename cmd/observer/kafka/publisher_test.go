package kafka

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/ecociel/when/lib/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

// mockClient mocks kgo.Client for testing
type mockClient struct {
	produceErr   error
	lastRecord   *kgo.Record
	produceCalls int
}

func (m *mockClient) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	m.produceCalls++
	if len(rs) > 0 {
		m.lastRecord = rs[0]
	}

	// Return ProduceResults slice
	if m.produceErr != nil {
		return kgo.ProduceResults{
			{
				Err: m.produceErr,
			},
		}
	}
	return kgo.ProduceResults{}
}

func TestPublishSync_Success(t *testing.T) {
	mock := &mockClient{}
	pub := &Publisher{client: mock}

	task := domain.Task{
		ID:           12345,
		Name:         "test-task",
		PartitionKey: "partition-1",
		Args:         []byte(`{"key":"value"}`),
		RetryCount:   0,
	}

	err := pub.PublishSync(context.Background(), task)

	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if mock.produceCalls != 1 {
		t.Fatalf("expected 1 produce call, got: %d", mock.produceCalls)
	}

	// Verify record structure
	rec := mock.lastRecord
	if string(rec.Key) != task.PartitionKey {
		t.Errorf("expected key %s, got %s", task.PartitionKey, string(rec.Key))
	}

	if string(rec.Value) != string(task.Args) {
		t.Errorf("expected value %s, got %s", string(task.Args), string(rec.Value))
	}

	// Verify headers
	if len(rec.Headers) != 2 {
		t.Fatalf("expected 2 headers, got %d", len(rec.Headers))
	}

	// Verify ID header
	var foundID bool
	for _, h := range rec.Headers {
		if h.Key == domain.HeaderID {
			foundID = true
			id := binary.BigEndian.Uint64(h.Value)
			if id != task.ID {
				t.Errorf("expected ID %d, got %d", task.ID, id)
			}
		}
	}
	if !foundID {
		t.Error("ID header not found")
	}
}

func TestPublishSync_WithRetry(t *testing.T) {
	mock := &mockClient{}
	pub := &Publisher{client: mock}

	task := domain.Task{
		ID:           67890,
		Name:         "retry-task",
		PartitionKey: "partition-2",
		Args:         []byte(`{"retry":true}`),
		RetryCount:   3,
		RetryReason:  "previous failure",
	}

	err := pub.PublishSync(context.Background(), task)

	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	rec := mock.lastRecord

	// Should have 4 headers with retry info
	if len(rec.Headers) != 4 {
		t.Fatalf("expected 4 headers, got %d", len(rec.Headers))
	}

	// Verify retry headers
	var foundRetryCount, foundRetryReason bool
	for _, h := range rec.Headers {
		switch h.Key {
		case domain.HeaderRetryCount:
			foundRetryCount = true
			count := binary.BigEndian.Uint16(h.Value)
			if count != task.RetryCount {
				t.Errorf("expected retry count %d, got %d", task.RetryCount, count)
			}
		case domain.HeaderRetryReason:
			foundRetryReason = true
			if string(h.Value) != task.RetryReason {
				t.Errorf("expected retry reason %s, got %s", task.RetryReason, string(h.Value))
			}
		}
	}

	if !foundRetryCount {
		t.Error("RetryCount header not found")
	}
	if !foundRetryReason {
		t.Error("RetryReason header not found")
	}
}

func TestPublishSync_Error(t *testing.T) {
	expectedErr := errors.New("kafka connection failed")
	mock := &mockClient{produceErr: expectedErr}
	pub := &Publisher{client: mock}

	task := domain.Task{
		ID:           99999,
		Name:         "error-task",
		PartitionKey: "partition-3",
		Args:         []byte(`{}`),
	}

	err := pub.PublishSync(context.Background(), task)

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error to be %v, got %v", expectedErr, err)
	}

	if mock.produceCalls != 1 {
		t.Errorf("expected 1 produce call, got: %d", mock.produceCalls)
	}
}

func TestPublishSync_ContextCancellation(t *testing.T) {
	expectedErr := context.Canceled
	mock := &mockClient{produceErr: expectedErr}
	pub := &Publisher{client: mock}

	task := domain.Task{
		ID:           11111,
		Name:         "cancelled-task",
		PartitionKey: "partition-4",
		Args:         []byte(`{}`),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := pub.PublishSync(ctx, task)

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

func TestTaskToRec_NoRetry(t *testing.T) {
	task := domain.Task{
		ID:           12345,
		Name:         "test-task",
		PartitionKey: "key-1",
		Args:         []byte("test-args"),
		RetryCount:   0,
	}

	rec := taskToRec(task)

	if string(rec.Key) != task.PartitionKey {
		t.Errorf("expected key %s, got %s", task.PartitionKey, string(rec.Key))
	}

	if string(rec.Value) != string(task.Args) {
		t.Errorf("expected value %s, got %s", string(task.Args), string(rec.Value))
	}

	if len(rec.Headers) != 2 {
		t.Errorf("expected 2 headers, got %d", len(rec.Headers))
	}
}

func TestTaskToRec_WithRetry(t *testing.T) {
	task := domain.Task{
		ID:           67890,
		Name:         "retry-task",
		PartitionKey: "key-2",
		Args:         []byte("retry-args"),
		RetryCount:   5,
		RetryReason:  "timeout",
	}

	rec := taskToRec(task)

	if len(rec.Headers) != 4 {
		t.Errorf("expected 4 headers, got %d", len(rec.Headers))
	}

	headerMap := make(map[string][]byte)
	for _, h := range rec.Headers {
		headerMap[h.Key] = h.Value
	}

	// Verify all headers exist
	if _, ok := headerMap[domain.HeaderID]; !ok {
		t.Error("ID header missing")
	}
	if _, ok := headerMap[domain.HeaderName]; !ok {
		t.Error("Name header missing")
	}
	if _, ok := headerMap[domain.HeaderRetryCount]; !ok {
		t.Error("RetryCount header missing")
	}
	if _, ok := headerMap[domain.HeaderRetryReason]; !ok {
		t.Error("RetryReason header missing")
	}

	// Verify header values
	id := binary.BigEndian.Uint64(headerMap[domain.HeaderID])
	if id != task.ID {
		t.Errorf("expected ID %d, got %d", task.ID, id)
	}

	name := string(headerMap[domain.HeaderName])
	if name != task.Name {
		t.Errorf("expected name %s, got %s", task.Name, name)
	}

	retryCount := binary.BigEndian.Uint16(headerMap[domain.HeaderRetryCount])
	if retryCount != task.RetryCount {
		t.Errorf("expected retry count %d, got %d", task.RetryCount, retryCount)
	}

	retryReason := string(headerMap[domain.HeaderRetryReason])
	if retryReason != task.RetryReason {
		t.Errorf("expected retry reason %s, got %s", task.RetryReason, retryReason)
	}
}

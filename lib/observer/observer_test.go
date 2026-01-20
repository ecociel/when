package observer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ecociel/when/lib/domain"
)

// mockStore implements the store interface for testing
type mockStore struct {
	claimDueTasksFunc func(ctx context.Context, limit int) ([]domain.Task, error)
	deleteFunc        func(ctx context.Context, id uint64) error
	claimCalls        int
	deleteCalls       int
	deletedIDs        []uint64
}

func (m *mockStore) ClaimDueTasks(ctx context.Context, limit int) ([]domain.Task, error) {
	m.claimCalls++
	if m.claimDueTasksFunc != nil {
		return m.claimDueTasksFunc(ctx, limit)
	}
	return nil, nil
}

func (m *mockStore) Delete(ctx context.Context, id uint64) error {
	m.deleteCalls++
	m.deletedIDs = append(m.deletedIDs, id)
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, id)
	}
	return nil
}

// mockPublisher implements the publisher interface for testing
type mockPublisher struct {
	publishSyncFunc func(ctx context.Context, task domain.Task) error
	publishCalls    int
	publishedTasks  []domain.Task
}

func (m *mockPublisher) PublishSync(ctx context.Context, task domain.Task) error {
	m.publishCalls++
	m.publishedTasks = append(m.publishedTasks, task)
	if m.publishSyncFunc != nil {
		return m.publishSyncFunc(ctx, task)
	}
	return nil
}

func TestClaimDueTasks_Success(t *testing.T) {
	expectedTasks := []domain.Task{
		{ID: 1, Name: "task-1", PartitionKey: "key-1"},
		{ID: 2, Name: "task-2", PartitionKey: "key-2"},
		{ID: 3, Name: "task-3", PartitionKey: "key-3"},
	}

	store := &mockStore{
		claimDueTasksFunc: func(ctx context.Context, limit int) ([]domain.Task, error) {
			if limit != 10 {
				t.Errorf("expected limit 10, got %d", limit)
			}
			return expectedTasks, nil
		},
	}

	pub := &mockPublisher{}

	runner := New(10, time.Second, store, pub)
	err := runner.process(context.Background())

	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if store.claimCalls != 1 {
		t.Errorf("expected 1 claim call, got %d", store.claimCalls)
	}

	if pub.publishCalls != len(expectedTasks) {
		t.Errorf("expected %d publish calls, got %d", len(expectedTasks), pub.publishCalls)
	}

	if store.deleteCalls != len(expectedTasks) {
		t.Errorf("expected %d delete calls, got %d", len(expectedTasks), store.deleteCalls)
	}

	// Verify all tasks were published
	for i, task := range expectedTasks {
		if pub.publishedTasks[i].ID != task.ID {
			t.Errorf("task %d: expected ID %d, got %d", i, task.ID, pub.publishedTasks[i].ID)
		}
	}

	// Verify all tasks were deleted
	for i, id := range expectedTasks {
		if store.deletedIDs[i] != id.ID {
			t.Errorf("task %d: expected deleted ID %d, got %d", i, id.ID, store.deletedIDs[i])
		}
	}
}

func TestClaimDueTasks_NoTasks(t *testing.T) {
	store := &mockStore{
		claimDueTasksFunc: func(ctx context.Context, limit int) ([]domain.Task, error) {
			return []domain.Task{}, nil
		},
	}

	pub := &mockPublisher{}

	runner := New(5, time.Second, store, pub)
	err := runner.process(context.Background())

	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if store.claimCalls != 1 {
		t.Errorf("expected 1 claim call, got %d", store.claimCalls)
	}

	if pub.publishCalls != 0 {
		t.Errorf("expected 0 publish calls, got %d", pub.publishCalls)
	}

	if store.deleteCalls != 0 {
		t.Errorf("expected 0 delete calls, got %d", store.deleteCalls)
	}
}

func TestClaimDueTasks_Error(t *testing.T) {
	expectedErr := errors.New("database connection failed")

	store := &mockStore{
		claimDueTasksFunc: func(ctx context.Context, limit int) ([]domain.Task, error) {
			return nil, expectedErr
		},
	}

	pub := &mockPublisher{}

	runner := New(10, time.Second, store, pub)
	err := runner.process(context.Background())

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error to wrap %v, got %v", expectedErr, err)
	}

	if store.claimCalls != 1 {
		t.Errorf("expected 1 claim call, got %d", store.claimCalls)
	}

	// Should not attempt to publish or delete if claiming fails
	if pub.publishCalls != 0 {
		t.Errorf("expected 0 publish calls, got %d", pub.publishCalls)
	}

	if store.deleteCalls != 0 {
		t.Errorf("expected 0 delete calls, got %d", store.deleteCalls)
	}
}

func TestClaimDueTasks_PublishError(t *testing.T) {
	tasks := []domain.Task{
		{ID: 1, Name: "task-1"},
		{ID: 2, Name: "task-2"},
		{ID: 3, Name: "task-3"},
	}

	store := &mockStore{
		claimDueTasksFunc: func(ctx context.Context, limit int) ([]domain.Task, error) {
			return tasks, nil
		},
	}

	publishErr := errors.New("kafka unavailable")
	pub := &mockPublisher{
		publishSyncFunc: func(ctx context.Context, task domain.Task) error {
			// Fail on second task
			if task.ID == 2 {
				return publishErr
			}
			return nil
		},
	}

	runner := New(10, time.Second, store, pub)
	err := runner.process(context.Background())

	// Should not return error for publish failures (logs and continues)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if pub.publishCalls != 3 {
		t.Errorf("expected 3 publish calls, got %d", pub.publishCalls)
	}

	// Should only delete successfully published tasks (1 and 3)
	if store.deleteCalls != 2 {
		t.Errorf("expected 2 delete calls, got %d", store.deleteCalls)
	}

	// Verify task 2 was NOT deleted
	for _, id := range store.deletedIDs {
		if id == 2 {
			t.Error("task 2 should not have been deleted after publish failure")
		}
	}

	// Verify tasks 1 and 3 were deleted
	if !contains(store.deletedIDs, 1) {
		t.Error("task 1 should have been deleted")
	}
	if !contains(store.deletedIDs, 3) {
		t.Error("task 3 should have been deleted")
	}
}

func TestClaimDueTasks_DeleteError(t *testing.T) {
	tasks := []domain.Task{
		{ID: 1, Name: "task-1"},
		{ID: 2, Name: "task-2"},
	}

	store := &mockStore{
		claimDueTasksFunc: func(ctx context.Context, limit int) ([]domain.Task, error) {
			return tasks, nil
		},
		deleteFunc: func(ctx context.Context, id uint64) error {
			if id == 1 {
				return errors.New("delete failed")
			}
			return nil
		},
	}

	pub := &mockPublisher{}

	runner := New(10, time.Second, store, pub)
	err := runner.process(context.Background())

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if pub.publishCalls != 1 {
		t.Errorf("expected 1 publish call, got %d", pub.publishCalls)
	}

	if store.deleteCalls != 1 {
		t.Errorf("expected 1 delete call, got %d", store.deleteCalls)
	}
}

func TestClaimDueTasks_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	store := &mockStore{
		claimDueTasksFunc: func(ctx context.Context, limit int) ([]domain.Task, error) {
			// Check if context is cancelled
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return []domain.Task{}, nil
		},
	}

	pub := &mockPublisher{}

	runner := New(10, time.Second, store, pub)
	err := runner.process(ctx)

	if err == nil {
		t.Fatal("expected context cancellation error, got nil")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestClaimDueTasks_WithLimit(t *testing.T) {
	tests := []struct {
		name     string
		limit    int
		expected int
	}{
		{"limit 1", 1, 1},
		{"limit 5", 5, 5},
		{"limit 100", 100, 100},
		{"limit 0", 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var receivedLimit int
			store := &mockStore{
				claimDueTasksFunc: func(ctx context.Context, limit int) ([]domain.Task, error) {
					receivedLimit = limit
					return []domain.Task{}, nil
				},
			}

			pub := &mockPublisher{}

			runner := New(tt.limit, time.Second, store, pub)
			_ = runner.process(context.Background())

			if receivedLimit != tt.expected {
				t.Errorf("expected limit %d, got %d", tt.expected, receivedLimit)
			}
		})
	}
}

func TestClaimDueTasks_SingleTask(t *testing.T) {
	task := domain.Task{
		ID:           99,
		Name:         "single-task",
		PartitionKey: "partition-1",
		Args:         []byte(`{"data":"test"}`),
	}

	store := &mockStore{
		claimDueTasksFunc: func(ctx context.Context, limit int) ([]domain.Task, error) {
			return []domain.Task{task}, nil
		},
	}

	pub := &mockPublisher{}

	runner := New(10, time.Second, store, pub)
	err := runner.process(context.Background())

	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(pub.publishedTasks) != 1 {
		t.Fatalf("expected 1 published task, got %d", len(pub.publishedTasks))
	}

	if pub.publishedTasks[0].ID != task.ID {
		t.Errorf("expected task ID %d, got %d", task.ID, pub.publishedTasks[0].ID)
	}

	if len(store.deletedIDs) != 1 {
		t.Fatalf("expected 1 deleted task, got %d", len(store.deletedIDs))
	}

	if store.deletedIDs[0] != task.ID {
		t.Errorf("expected deleted task ID %d, got %d", task.ID, store.deletedIDs[0])
	}
}

func TestRun_ContextCancellation(t *testing.T) {
	store := &mockStore{
		claimDueTasksFunc: func(ctx context.Context, limit int) ([]domain.Task, error) {
			return []domain.Task{}, nil
		},
	}

	pub := &mockPublisher{}

	runner := New(10, 10*time.Millisecond, store, pub)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Run should exit when context is cancelled
	runner.Run(ctx)

	// Should have made at least one claim call
	if store.claimCalls < 1 {
		t.Errorf("expected at least 1 claim call, got %d", store.claimCalls)
	}
}

// Helper function
func contains(slice []uint64, val uint64) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

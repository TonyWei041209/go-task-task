package task

import (
	"context"
	"sync"
	"time"
)

type State string

const (
	StatePending   State = "Pending"
	StateRunning   State = "Running"
	StateCompleted State = "Completed"
	StateFailed    State = "Failed"
)

type Task struct {
	mu         sync.RWMutex
	ID         string
	State      State
	Action     func(ctx context.Context) error
	Err        error
	Metadata   map[string]interface{}
	History    []State
	StartedAt  time.Time
	FinishedAt time.Time
}

func NewTask(id string, action func(ctx context.Context) error) *Task {
	return &Task{
		ID:       id,
		State:    StatePending,
		Action:   action,
		Metadata: make(map[string]interface{}),
		History:  []State{StatePending},
	}
}

// GetState returns the current state of the task.
func (t *Task) GetState() State {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.State
}

// SetState updates the task's state and appends it to the history.
func (t *Task) SetState(state State) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.State = state
	t.History = append(t.History, state)
}

// TransitionTo is an atomic state-and-error transition.
// It sets the state, appends to history, records the error,
// and records FinishedAt if the state is a terminal one
// (Completed or Failed).
func (t *Task) TransitionTo(state State, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.State = state
	t.History = append(t.History, state)
	t.Err = err
	if state == StateRunning {
		t.StartedAt = time.Now()
	}
	if state == StateCompleted || state == StateFailed {
		t.FinishedAt = time.Now()
	}
}

func (t *Task) GetErr() error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Err
}

func (t *Task) SetErr(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Err = err
}

func (t *Task) GetMetadata(key string) (interface{}, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	val, ok := t.Metadata[key]
	return val, ok
}

func (t *Task) SetMetadata(key string, val interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Metadata[key] = val
}

func (t *Task) GetHistory() []State {
	t.mu.RLock()
	defer t.mu.RUnlock()
	h := make([]State, len(t.History))
	copy(h, t.History)
	return h
}

func (t *Task) GetStartedAt() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.StartedAt
}

func (t *Task) GetFinishedAt() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.FinishedAt
}

package task

import (
	"context"
	"sync"
)

// Runner executes tasks concurrently.
type Runner struct {
	mu    sync.RWMutex
	tasks []*Task
}

func NewRunner() *Runner {
	return &Runner{
		tasks: make([]*Task, 0),
	}
}

func (r *Runner) AddTask(t *Task) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tasks = append(r.tasks, t)
}

// RunnerStatus contains a snapshot of the runner's task states.
type RunnerStatus struct {
	TaskID string
	State  State
	Err    error
}

// Status returns a thread-safe snapshot of every task's state and error.
func (r *Runner) Status() []RunnerStatus {
	r.mu.RLock()
	snap := make([]*Task, len(r.tasks))
	copy(snap, r.tasks)
	r.mu.RUnlock()

	results := make([]RunnerStatus, len(snap))
	for i, t := range snap {
		results[i] = RunnerStatus{
			TaskID: t.ID,
			State:  t.GetState(),
			Err:    t.GetErr(),
		}
	}
	return results
}

// Run executes all tasks concurrently. It uses the task's own public API
// for thread-safe state transitions so that concurrent readers (e.g. the
// race-detector test in runner_test.go) do not cause data races.
func (r *Runner) Run(ctx context.Context) {
	r.mu.RLock()
	tasks := make([]*Task, len(r.tasks))
	copy(tasks, r.tasks)
	r.mu.RUnlock()

	var wg sync.WaitGroup
	for _, t := range tasks {
		wg.Add(1)
		go func(task *Task) {
			defer wg.Done()

			// Check whether the context has been cancelled before we start.
			if ctx.Err() != nil {
				task.TransitionTo(StateFailed, ctx.Err())
				return
			}

			task.TransitionTo(StateRunning, nil)

			err := task.Action(ctx)

			if err != nil {
				task.TransitionTo(StateFailed, err)
			} else {
				task.TransitionTo(StateCompleted, nil)
			}
		}(t)
	}
	wg.Wait()
}

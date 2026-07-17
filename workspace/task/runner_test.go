package task

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRunner_Run(t *testing.T) {
	runner := NewRunner()

	t1 := NewTask("1", func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	t2 := NewTask("2", func(ctx context.Context) error {
		time.Sleep(10 * time.Millisecond)
		return errors.New("failed")
	})

	runner.AddTask(t1)
	runner.AddTask(t2)

	runner.Run(context.Background())

	if s := t1.GetState(); s != StateCompleted {
		t.Errorf("expected t1 to be Completed, got %s", s)
	}
	if s := t2.GetState(); s != StateFailed {
		t.Errorf("expected t2 to be Failed, got %s", s)
	}
}

func TestRunner_Run_Concurrent(t *testing.T) {
	runner := NewRunner()
	numTasks := 100

	var tasks []*Task
	for i := 0; i < numTasks; i++ {
		task := NewTask(fmt.Sprintf("%d", i), func(ctx context.Context) error {
			time.Sleep(5 * time.Millisecond)
			return nil
		})
		runner.AddTask(task)
		tasks = append(tasks, task)
	}

	stopChan := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
				for _, task := range tasks {
					_ = task.GetState()
					_, _ = task.GetMetadata("key")
				}
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	runner.Run(context.Background())
	close(stopChan)
	wg.Wait()

	for _, task := range tasks {
		if state := task.GetState(); state != StateCompleted {
			t.Errorf("expected task %s to be Completed, got %s", task.ID, state)
		}
	}
}

func TestRunner_Run_Cancel(t *testing.T) {
	runner := NewRunner()
	ctx, cancel := context.WithCancel(context.Background())

	t1 := NewTask("1", func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	runner.AddTask(t1)

	cancel()
	runner.Run(ctx)

	if state := t1.GetState(); state != StateFailed {
		t.Errorf("expected t1 to be Failed due to cancellation, got %s", state)
	}
	if err := t1.GetErr(); err != context.Canceled {
		t.Errorf("expected t1 error to be context.Canceled, got %v", err)
	}
}

func TestRunner_Run_ConcurrentWrites(t *testing.T) {
	// Create tasks that use SetMetadata/GetMetadata while running.
	runner := NewRunner()
	numTasks := 50

	// Collect tasks for background reading.
	taskList := make([]*Task, numTasks)
	for i := 0; i < numTasks; i++ {
		id := fmt.Sprintf("write-%d", i)
		tk := NewTask(id, nil)
		tk2 := tk
		tk.Action = func(ctx context.Context) error {
			for j := 0; j < 5; j++ {
				tk2.SetMetadata(fmt.Sprintf("key-%d", j), j)
				_, _ = tk2.GetMetadata(fmt.Sprintf("key-%d", j))
			}
			return nil
		}
		runner.AddTask(tk)
		taskList[i] = tk
	}

	// Concurrently read Metadata from another goroutine while tasks run.
	stopChan := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
				for _, t := range taskList {
					_ = t.GetState()
					_, _ = t.GetMetadata("key-0")
				}
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	runner.Run(context.Background())
	close(stopChan)
	wg.Wait()
}

func TestRunner_Run_Timeout(t *testing.T) {
	runner := NewRunner()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	t1 := NewTask("slow", func(ctx context.Context) error {
		select {
		case <-time.After(500 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	runner.AddTask(t1)
	runner.Run(ctx)

	if state := t1.GetState(); state != StateFailed {
		t.Errorf("expected t1 to be Failed after timeout, got %s", state)
	}
}

func TestRunner_Run_Status(t *testing.T) {
	runner := NewRunner()
	t1 := NewTask("s1", func(ctx context.Context) error { return nil })
	t2 := NewTask("s2", func(ctx context.Context) error { return errors.New("err") })

	runner.AddTask(t1)
	runner.AddTask(t2)
	runner.Run(context.Background())

	st := runner.Status()
	if len(st) != 2 {
		t.Fatalf("expected 2 status entries, got %d", len(st))
	}
	if st[0].State != StateCompleted && st[0].State != StateFailed {
		t.Errorf("unexpected state for task s1: %s", st[0].State)
	}
	if st[1].State != StateFailed && st[1].State != StateCompleted {
		t.Errorf("unexpected state for task s2: %s", st[1].State)
	}
}

func TestRunner_Run_Historical(t *testing.T) {
	runner := NewRunner()
	t1 := NewTask("hist", func(ctx context.Context) error { return nil })

	runner.AddTask(t1)
	runner.Run(context.Background())

	hist := t1.GetHistory()
	expectedStates := []State{StatePending, StateRunning, StateCompleted}
	if len(hist) != len(expectedStates) {
		t.Fatalf("expected %d history entries, got %d: %v", len(expectedStates), len(hist), hist)
	}
	for i, s := range expectedStates {
		if hist[i] != s {
			t.Errorf("history[%d] = %s, want %s", i, hist[i], s)
		}
	}
}

func TestTask_ConcurrentAccessors(t *testing.T) {
	task := NewTask("stress", func(ctx context.Context) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = task.GetState()
				_ = task.GetErr()
				_, _ = task.GetMetadata("any")
				_ = task.GetHistory()
				_ = task.GetStartedAt()
				_ = task.GetFinishedAt()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			task.SetState(StateRunning)
			task.SetErr(nil)
			task.SetMetadata(fmt.Sprintf("k-%d", j), j)
		}
		task.TransitionTo(StateCompleted, nil)
	}()

	wg.Wait()

	if s := task.GetState(); s != StateCompleted {
		t.Errorf("expected final state Completed, got %s", s)
	}
}

func TestRunner_Run_MetadataRace(t *testing.T) {
	runner := NewRunner()
	numTasks := 50

	var tasks []*Task
	for i := 0; i < numTasks; i++ {
		id := fmt.Sprintf("%d", i)
		tk := NewTask(id, nil)
		tk2 := tk
		tk.Action = func(ctx context.Context) error {
			tk2.SetMetadata("result", "ok")
			return nil
		}
		runner.AddTask(tk)
		tasks = append(tasks, tk)
	}

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopChan:
					return
				default:
					for _, t := range tasks {
						_ = t.GetState()
						_, _ = t.GetMetadata("result")
						_ = t.GetErr()
					}
					time.Sleep(500 * time.Microsecond)
				}
			}
		}()
	}

	runner.Run(context.Background())
	close(stopChan)
	wg.Wait()

	for i, task := range tasks {
		if s := task.GetState(); s != StateCompleted {
			t.Errorf("task %d had state %s, want Completed", i, s)
		}
	}
}

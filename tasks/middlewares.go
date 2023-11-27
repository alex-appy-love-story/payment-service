package tasks

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/hibiken/asynq"
)

func LoggingMiddleware(h asynq.Handler) asynq.Handler {
	return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
		fmt.Println()
		start := time.Now()
		log.Printf("Start processing %q", t.Type())
		err := h.ProcessTask(ctx, t)
		if err != nil {
			return err
		}
		log.Printf("Finished processing %q: Elapsed Time = %v", t.Type(), time.Since(start))
		return nil
	})
}

func CircuitBreakerMiddleware(h asynq.Handler) asynq.Handler {
	return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
		cb := GetTaskContext(ctx).CircuitBreaker
		err := h.ProcessTask(ctx, t)

		fmt.Println("Task error: ", err)

		if cb.IsState("open") {
			return err
		}

		cb.Mutex().Lock()
		defer cb.Mutex().Unlock()

		if err == nil {
			cb.SetFails(0)
			cb.SetState("closed")
			return nil
		}

		if cb.IsState("half-open") {
			cb.SetState("open")
			cb.OpenChannel() <- struct{}{}
			return err
		}

		cb.IncrementFails()
		if cb.FailsExcceededThreshold() {
			cb.SetState("open")
			cb.OpenChannel() <- struct{}{}
		}

		return err
	})
}

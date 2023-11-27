package tasks

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hibiken/asynq"
)

//----------------------------------------------
// Task payload.
//---------------------------------------------

const (
	PERFORM = "perform"
	REVERT  = "revert"
)

type SagaPayload struct {
	Action string // "perform" or "revert"
}

//---------------------------------------------------------------
// Write a function HandleXXXTask to handle the input task.
// Note that it satisfies the asynq.HandlerFunc interface.
//
// Handler doesn't need to be a function. You can define a type
// that satisfies asynq.Handler interface.
//---------------------------------------------------------------

func HandlePerformStepTask(ctx context.Context, t *asynq.Task) error {
	taskContext := GetTaskContext(ctx)

	// Immediately send back default response if CB is open
	if taskContext.CircuitBreaker.IsState("open") {
		return fmt.Errorf("Default response.")
	}

	var p StepPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}

	// Error channel. This can either catch context cancellation or if an error occured within the task.
	c := make(chan error, 1)

	go func() {
		c <- Perform(p, taskContext)
	}()

	select {
	case <-ctx.Done():
		// cancelation signal received, abandon this work.
		return ctx.Err()
	case res := <-c:
		return res
	}
}

func HandleRevertStepTask(ctx context.Context, t *asynq.Task) error {
	var p StepPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}

	taskContext := GetTaskContext(ctx)

	// Error channel. This can either catch context cancellation or if an error occured within the task.
	c := make(chan error, 1)

	go func() {
		c <- Revert(p, taskContext)
	}()

	select {
	case <-ctx.Done():
		// cancelation signal received, abandon this work.
		return ctx.Err()
	case res := <-c:
		return res
	}
}

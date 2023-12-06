package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/alex-appy-love-story/db-lib/models/order"
	"github.com/hibiken/asynq"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
)

// SpanContext contains identifying trace information about a Span.
type SpanContext struct {
	TraceID string `json:"trace_id"`
	SpanID  string `json:"span_id"`
}

//----------------------------------------------
// Task payload.
//---------------------------------------------

const (
	PERFORM = "perform"
	REVERT  = "revert"
)

type SagaPayload struct {
	FailTrigger string `json:"fail_trigger"` // Name of the service to fail.

	// For Otel
	TraceCarrier propagation.MapCarrier `json:"trace_carrier,omitempty"`
}

var (
	tracer = otel.Tracer(os.Getenv("SERVER_QUEUE"))
)

//---------------------------------------------------------------
// Write a function HandleXXXTask to handle the input task.
// Note that it satisfies the asynq.HandlerFunc interface.
//
// Handler doesn't need to be a function. You can define a type
// that satisfies asynq.Handler interface.
//---------------------------------------------------------------

func fetchSpan(p *StepPayload, ctx context.Context, taskCtx *TaskContext, job string) {

	// Create the propagator.
	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

	// Try to fetch the span context.
	if len(p.TraceCarrier) > 0 {
		log.Println("Found parent trace!")

		parentCtx := propagator.Extract(context.Background(), p.TraceCarrier)
		_, taskCtx.Span = tracer.Start(parentCtx, fmt.Sprintf("%s.%s", taskCtx.ServerQueue, job))

	} else {
		log.Println("Creating a new trace!")

		// Create a new trace.
		ctx, taskCtx.Span = tracer.Start(ctx, fmt.Sprintf("%s.%s", taskCtx.ServerQueue, job))

		p.TraceCarrier = make(propagation.MapCarrier)

		// Serialize the context into carrier
		propagator.Inject(ctx, p.TraceCarrier)

		// This carrier is sent accros the process
		fmt.Println("Carrier:", p.TraceCarrier)
	}
}

func HandlePerformStepTask(ctx context.Context, t *asynq.Task) error {
	taskContext := GetTaskContext(ctx)

	var p StepPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}
	log.Printf("Payload: %+v\n", p)

	fetchSpan(&p, ctx, taskContext, "perform")
	defer taskContext.Span.End()

    // Immediately send back default response if CB is open
    var err error
    if taskContext.CircuitBreaker.IsState("open") {
        err = fmt.Errorf("Default response")
        taskContext.TaskFailed(err)
        err := SetOrderStatus(taskContext.OrderSvcAddr, p.OrderID, order.DEFAULT_RESPONSE)
        if err != nil {
            return fmt.Errorf("Failed to set order status")
        }
        RevertPrevious(p, map[string]interface{}{"order_id": p.OrderID}, taskContext)
        return err
    }

	if p.FailTrigger == taskContext.ServerQueue {
		err = fmt.Errorf("Forced to fail")
		taskContext.TaskFailed(err)
		err := SetOrderStatus(taskContext.OrderSvcAddr, p.OrderID, order.FORCED_FAIL)
		if err != nil {
			return fmt.Errorf("Failed to set order status")
		}
		RevertPrevious(p, map[string]interface{}{"order_id": p.OrderID}, taskContext)
		return err
	}

	// Error channel. This can either catch context cancellation or if an error occured within the task.
	c := make(chan error, 1)

	// ctx = context.WithValue(ctx)

	go func() {
		c <- Perform(p, taskContext)
	}()

	select {
	case <-ctx.Done():
		// cancelation signal received, abandon this work.
		err = ctx.Err()
	case res := <-c:
		err = res
	}

	if err != nil {
		taskContext.TaskFailed(err)
	} else {
		taskContext.Span.SetStatus(codes.Ok, "")
	}

	return err
}

func HandleRevertStepTask(ctx context.Context, t *asynq.Task) error {
	taskContext := GetTaskContext(ctx)

	var p StepPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}
	log.Printf("Payload: %+v\n", p)

	fetchSpan(&p, ctx, taskContext, "revert")
	defer taskContext.Span.End()

	// Error channel. This can either catch context cancellation or if an error occured within the task.
	c := make(chan error, 1)

	go func() {
		c <- Revert(p, taskContext)
	}()

	var err error
	select {
	case <-ctx.Done():
		// cancelation signal received, abandon this work.
		err = ctx.Err()
	case res := <-c:
		err = res
	}

	taskContext.AddSpanStateEvent()

	if err != nil {
		taskContext.TaskFailed(err)
	} else {
		taskContext.Span.SetStatus(codes.Ok, "")
	}

	return err
}

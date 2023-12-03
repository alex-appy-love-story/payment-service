package tasks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/alex-appy-love-story/db-lib/models/order"
	"github.com/alex-appy-love-story/worker-template/circuitbreaker"
	"github.com/hibiken/asynq"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

const (
	TASK_NOT_FOUND = "asynq: task not found"
	TIMEOUT        = time.Second * 10
)

type TaskState int

const (
	Done TaskState = iota
	Failed
	Expired
)

type TaskContext struct {
	GormClient     *gorm.DB
	AsynqClient    *asynq.Client
	AsynqInspector *asynq.Inspector
	NextQueue      string
	ServerQueue    string
	PreviousQueue  string
	CircuitBreaker *circuitbreaker.CB
	OrderSvcAddr   string
	Span           trace.Span
	TaskState      TaskState
}

func (t TaskContext) TaskFailed(err error) {
	t.Span.RecordError(err)
	t.Span.SetStatus(codes.Error, err.Error())
}

func (t TaskContext) AddSpanStateEvent() {
	switch t.TaskState {
	case Expired:
		t.Span.AddEvent(fmt.Sprintf("Timeout: %s not picking up job", t.NextQueue))
		t.Span.SetStatus(codes.Error, "timeout")
	case Failed:
		t.Span.AddEvent("Next task failed")
		t.Span.SetStatus(codes.Error, "timeout")
	default:
		// Do nothing...
	}
}

func GetTaskContext(ctx context.Context) *TaskContext {
	taskCtx := &TaskContext{}

	taskCtx.GormClient = nil
	taskCtx.AsynqClient = nil

	if val := ctx.Value("asynq_client"); val != nil {
		taskCtx.AsynqClient = val.(*asynq.Client)
	}

	if val := ctx.Value("asynq_inspector"); val != nil {
		taskCtx.AsynqInspector = val.(*asynq.Inspector)
	}

	if val := ctx.Value("db_client"); val != nil {
		taskCtx.GormClient = val.(*gorm.DB)
	}

	if val := ctx.Value("next_queue"); val != nil {
		taskCtx.NextQueue = val.(string)
	}

	if val := ctx.Value("previous_queue"); val != nil {
		taskCtx.PreviousQueue = val.(string)
	}

	if val := ctx.Value("server_queue"); val != nil {
		taskCtx.ServerQueue = val.(string)
	}

	if val := ctx.Value("circuit_breaker"); val != nil {
		taskCtx.CircuitBreaker = val.(*circuitbreaker.CB)
	}

	if val := ctx.Value("circuit_breaker"); val != nil {
		taskCtx.CircuitBreaker = val.(*circuitbreaker.CB)
	}

	if val := ctx.Value("order_svc_addr"); val != nil {
		taskCtx.OrderSvcAddr = val.(string)
	}

	return taskCtx
}

func GetTaskState(doIn time.Duration, taskID string, ctx *TaskContext) (TaskState, error) {

	time.Sleep(doIn)

	taskInfo, err := ctx.AsynqInspector.GetTaskInfo(ctx.NextQueue, taskID)

	if err != nil {
		if err.Error() == TASK_NOT_FOUND {
			// Task no longer inside the current server queue. (It was taken)
			return Done, nil
		} else {
			// Some error occured inside the task.
			ctx.CircuitBreaker.IncrementFails()
			return Failed, fmt.Errorf(taskInfo.LastErr)
		}

	} else {
		if taskInfo.State.String() == "active" {
			fmt.Println("Job is being processed.")
			return Done, nil
		} else if taskInfo.State.String() == "archived" {
			fmt.Println("Task failed!")
			ctx.CircuitBreaker.IncrementFails()
			ctx.AsynqInspector.DeleteTask(ctx.NextQueue, taskID)
			return Failed, fmt.Errorf(taskInfo.LastErr)
		} else {
			fmt.Println("Job is still in the queue. Cancel!", taskID)
			ctx.CircuitBreaker.IncrementFails()
			err = ctx.AsynqInspector.DeleteTask(ctx.NextQueue, taskID)

			if err != nil {
				log.Println("Failed to delete task:", taskID)
			}

			// Job is still pending. We got timeout.
			return Expired, fmt.Errorf("Task: Expired")
		}
	}
}

func PerformNext(stepPayload StepPayload, payload map[string]interface{}, ctx *TaskContext) error {
	payload["trace_carrier"] = stepPayload.TraceCarrier
	payload["fail_trigger"] = stepPayload.FailTrigger

	p, err := json.Marshal(payload)
	if err != nil {
		fmt.Println("Failed to marshal")
		RevertSelf(stepPayload, ctx)
		return err
	}

	task := asynq.NewTask("task:perform", p, asynq.MaxRetry(0))

	// Process the task immediately.
	taskInfo, err := ctx.AsynqClient.Enqueue(task, asynq.Queue(ctx.NextQueue), asynq.MaxRetry(0))
	if err != nil {
		fmt.Println("Failed to enqueue task to next")
		RevertSelf(stepPayload, ctx)
		return err
	}

	ctx.TaskState, err = GetTaskState(TIMEOUT, taskInfo.ID, ctx)
	ctx.AddSpanStateEvent()

	switch ctx.TaskState {
	case Expired:
		// NOTE(Appy): The task is not taken. No servers taking the task.
		RevertSelf(stepPayload, ctx)
		fallthrough
	case Failed:
		// NOTE(Appy): The next failed process would call revert on us.
		return err
	default:
		return nil
	}
}

func RevertSelf(stepPayload StepPayload, ctx *TaskContext) error {
	log.Printf("Calling revert self with payload: %+v\n", stepPayload)
	p, err := json.Marshal(stepPayload)

	if err != nil {
		return err
	}

	task := asynq.NewTask("task:revert", p, asynq.MaxRetry(0))

	// Process the task immediately.
	_, err = ctx.AsynqClient.Enqueue(task, asynq.Queue(ctx.ServerQueue), asynq.MaxRetry(0))
	if err != nil {

		// Failed to queue.
		return err
	}

	return nil
}

func RevertPrevious(stepPayload StepPayload, payload map[string]interface{}, ctx *TaskContext) error {
	if len(ctx.PreviousQueue) == 0 {
		return nil
	}

	payload["trace_carrier"] = stepPayload.TraceCarrier
	p, err := json.Marshal(payload)

	if err != nil {
		return err
	}

	task := asynq.NewTask("task:revert", p, asynq.MaxRetry(0))

	// Process the task immediately.
	_, err = ctx.AsynqClient.Enqueue(task, asynq.Queue(ctx.PreviousQueue), asynq.MaxRetry(0))
	if err != nil {

		// Failed to queue.
		return err
	}

	return nil
}

func SetOrderStatus(orderSvcAddr string, orderID uint, status order.OrderStatus) error {
	// Create client.
	client := &http.Client{}
	requestURL := fmt.Sprintf("http://%s/fail/%d", orderSvcAddr, orderID)

	payloadBuf := new(bytes.Buffer)
	if err := json.NewEncoder(payloadBuf).Encode(map[string]interface{}{
		"order_status": status,
	}); err != nil {
		return err
	}

	// Set up request.
	req, err := http.NewRequest("PUT", requestURL, payloadBuf)
	if err != nil {
		fmt.Printf("error making http request: %s\n", err)
		return err
	}

	// Fetch Request.
	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return err
	}
	defer resp.Body.Close()

	return nil
}

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
}

func GetTaskContext(ctx context.Context) (taskCtx TaskContext) {

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

	return
}

func GetTaskState(doIn time.Duration, taskID string, ctx TaskContext) (TaskState, error) {

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
			fmt.Println("Job is still in the queue. Cancel!")
			ctx.CircuitBreaker.IncrementFails()
			ctx.AsynqInspector.DeleteTask(ctx.NextQueue, taskID)
		}
		// Job is still pending. We got timeout.
		return Expired, fmt.Errorf("Task: Expired")
	}
}

func PerformNext(stepPayload StepPayload, payload map[string]interface{}, ctx TaskContext) error {
	p, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	task := asynq.NewTask("task:perform", p, asynq.MaxRetry(0))

	// Process the task immediately.
	taskInfo, err := ctx.AsynqClient.Enqueue(task, asynq.Queue(ctx.NextQueue))
	if err != nil {
		return err
	}

	state, err := GetTaskState(TIMEOUT, taskInfo.ID, ctx)

	switch state {
	case Expired:
		// NOTE(Appy): The task is not taken. No servers taking the task.
		Revert(stepPayload, ctx)
		fallthrough
	case Failed:
		// NOTE(Appy): The next failed process would call revert on us.
		return err
	default:
		return nil
	}
}

func RevertPrevious(stepPayload StepPayload, payload map[string]interface{}, ctx TaskContext) error {
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

	log.Println("Queued previous revert")

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

package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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

	return
}

func GetTaskState(doIn time.Duration, taskID string, ctx TaskContext) (TaskState, error) {

	time.Sleep(doIn)

	taskInfo, err := ctx.AsynqInspector.GetTaskInfo(ctx.ServerQueue, taskID)

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
		} else {
			fmt.Println("Job is still in the queue. Cancel!")
			ctx.CircuitBreaker.IncrementFails()
			ctx.AsynqInspector.DeleteTask(ctx.ServerQueue, taskID)
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

	task := asynq.NewTask("task:perform", p)

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

	task := asynq.NewTask("task:revert", p)

	// Process the task immediately.
	_, err = ctx.AsynqClient.Enqueue(task, asynq.Queue(ctx.PreviousQueue), asynq.MaxRetry(5))
	if err != nil {

		// Failed to queue.
		return err
	}

	return nil
}

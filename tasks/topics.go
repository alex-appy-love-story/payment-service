package tasks

import (
	"github.com/hibiken/asynq"
)

func RegisterTopic(mux *asynq.ServeMux) {
	// Register tasks here...
	mux.HandleFunc("task:perform", HandlePerformStepTask)
	mux.HandleFunc("task:revert", HandleRevertStepTask)
}

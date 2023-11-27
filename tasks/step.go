package tasks

import "fmt"

//----------------------------------------------
// Task payload.
//---------------------------------------------

type StepPayload struct {
	SagaPayload

	// Define members here...
}

func Perform(p StepPayload, ctx TaskContext) error {
	if p.Action == "err" {
		return fmt.Errorf("Test error!!!")
	} else {
		return nil
	}
}

func Revert(p StepPayload, ctx TaskContext) error {
	return nil
}

package circuitbreaker

import (
	"fmt"
	"sync"
	"time"
)

type State string

const (
	open     State = "open"
	closed   State = "closed"
	halfOpen State = "half-open"
)

var (
	states = map[string]State{
		"open":      open,
		"closed":    closed,
		"half-open": halfOpen,
	}
)

type CB struct {
	maxConsecutiveFails uint64
	openInterval        time.Duration

	// fails is the number of failed requets for the current "Closed" state,
	// resets after a successful transition from half-open to closed.
	fails uint64

	// current state of the circuit
	state State

	// openChannel handles the event transfer mechanism for the open state
	openChannel chan struct{}

	mutex *sync.Mutex
}

type CBOptions struct {
	// MaxConsecutiveFails specifies the maximum consecutive fails which are allowed
	// in the "Closed" state before the state is changed to "Open".
	MaxConsecutiveFails *uint64

	OpenInterval *time.Duration
}

func NewCircuitBreaker(opts ...CBOptions) *CB {
	var opt CBOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	if opt.MaxConsecutiveFails == nil {
		opt.MaxConsecutiveFails = IntToPointer(uint64(5))
	}

	if opt.OpenInterval == nil {
		opt.OpenInterval = TimeToPointer(2 * time.Second)
	}

	cb := &CB{
		maxConsecutiveFails: *opt.MaxConsecutiveFails,
		openInterval:        *opt.OpenInterval,
		openChannel:         make(chan struct{}),
		mutex:               &sync.Mutex{},
	}

	cb.state = closed

	go cb.OpenWatcher()

	return cb
}

func (cb *CB) FailsExcceededThreshold() bool {
	return cb.fails >= cb.maxConsecutiveFails
}

func (cb *CB) OpenWatcher() {
	for range cb.openChannel {
		time.Sleep(cb.openInterval)
		cb.mutex.Lock()
		cb.state = halfOpen
		cb.fails = 0
		cb.mutex.Unlock()
	}
}

// Getters
func (cb *CB) Fails() uint64 {
	return cb.fails
}
func (cb *CB) MaxFails() uint64 {
	return cb.maxConsecutiveFails
}
func (cb *CB) State() State {
	return cb.state
}
func (cb *CB) Mutex() *sync.Mutex {
	return cb.mutex
}
func (cb *CB) OpenChannel() chan struct{} {
	return cb.openChannel
}

// Setters
func (cb *CB) SetFails(num uint64) uint64 {
	cb.fails = num
	return cb.fails
}
func (cb *CB) SetState(stateStr string) State {
	tmpState, ok := StringToState(stateStr)
	if ok != true {
		fmt.Println("State doesn't exist: ", stateStr)
	}
	cb.state = tmpState
	return cb.state
}

func (cb *CB) IncrementFails() uint64 {
	cb.fails++
	return cb.fails
}

func (cb *CB) IsState(stateStr string) bool {
	tmpState, ok := StringToState(stateStr)
	if ok != true {
		fmt.Println("State doesn't exist: ", stateStr)
	}
	return cb.state == tmpState
}

package circuitbreaker

import (
	"strings"
	"time"
)

func IntToPointer(x uint64) *uint64 {
	return &x
}

func TimeToPointer(x time.Duration) *time.Duration {
	return &x
}

func StringToState(str string) (State, bool) {
    c, ok := states[strings.ToLower(str)]
    return c, ok
}

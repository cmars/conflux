package recon

import (
	"log"
	"sync"
)

type State string

var (
	StateIdle       = State("")
	StateServing    = State("serving")
	StateGossipping = State("gossipping")
)

func (s State) String() string {
	if s == StateIdle {
		return "idle"
	}
	return string(s)
}

type Tracker struct {
	mu         sync.Mutex
	state      State
	execOnIdle []func() error
}

func (t *Tracker) Done() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.state = StateIdle

	for _, execIdle := range t.execOnIdle {
		err := execIdle()
		if err != nil {
			log.Println(err)
		}
	}
	t.execOnIdle = nil
}

func (t *Tracker) Begin(state State) (State, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state == StateIdle {
		t.state = state
	}
	return t.state, t.state == state
}

func (t *Tracker) ExecIdle(f func() error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state == StateIdle {
		f()
	} else {
		t.execOnIdle = append(t.execOnIdle, f)
	}
}

/*
   conflux - Distributed database synchronization library
	Based on the algorithm described in
		"Set Reconciliation with Nearly Optimal	Communication Complexity",
			Yaron Minsky, Ari Trachtenberg, and Richard Zippel, 2004.

   Copyright (c) 2012-2015  Casey Marshall <cmars@cmarstech.com>

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published by
   the Free Software Foundation, version 3.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package recon

import (
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
	execOnIdle []func()
}

func (t *Tracker) Done() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.state = StateIdle

	for _, execIdle := range t.execOnIdle {
		execIdle()
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

func (t *Tracker) ExecIdle(f func() error, errh ErrorHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()

	runner := func() {
		errh(f())
	}
	if t.state == StateIdle {
		runner()
	} else {
		t.execOnIdle = append(t.execOnIdle, runner)
	}
}

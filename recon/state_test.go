/*
   conflux - Distributed database synchronization library
	Based on the algorithm described in
		"Set Reconciliation with Nearly Optimal	Communication Complexity",
			Yaron Minsky, Ari Trachtenberg, and Richard Zippel, 2004.

   Copyright (C) 2012  Casey Marshall <casey.marshall@gmail.com>

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
	gc "gopkg.in/check.v1"
)

type StateSuite struct{}

var _ = gc.Suite(&StateSuite{})

func (s *StateSuite) TestLockedUntilDone(c *gc.C) {
	var t Tracker
	c.Assert(t.state, gc.Equals, StateIdle)

	st, ok := t.Begin(StateServing)
	c.Assert(ok, gc.Equals, true)
	c.Assert(st, gc.Equals, StateServing)
	c.Assert(t.state, gc.Equals, StateServing)

	st, ok = t.Begin(StateGossipping)
	c.Assert(ok, gc.Equals, false)
	c.Assert(st, gc.Equals, StateServing)
	c.Assert(t.state, gc.Equals, StateServing)

	t.Done()

	st, ok = t.Begin(StateGossipping)
	c.Assert(ok, gc.Equals, true)
	c.Assert(st, gc.Equals, StateGossipping)
	c.Assert(t.state, gc.Equals, StateGossipping)
}

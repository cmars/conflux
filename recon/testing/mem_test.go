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

package testing

import (
	"flag"
	"testing"
	"time"

	gc "gopkg.in/check.v1"

	"gopkg.in/hockeypuck/conflux.v2/recon"
)

var long = flag.Bool("long", false, "run long-running tests")

func Test(t *testing.T) { gc.TestingT(t) }

type MemReconSuite struct {
	*ReconSuite
}

var _ = gc.Suite(&MemReconSuite{
	ReconSuite: &ReconSuite{
		Factory: func() (recon.PrefixTree, Cleanup, error) {
			ptree := &recon.MemPrefixTree{}
			ptree.Init()
			return ptree, func() {}, nil
		},
	},
})

func (s *MemReconSuite) TestOneSidedMedium(c *gc.C) {
	s.RunOneSided(c, 250, true, 30*time.Second)
	s.RunOneSided(c, 250, false, 30*time.Second)
}

func (s *MemReconSuite) TestOneSidedLarge(c *gc.C) {
	if !*long {
		c.Skip("long running test")
	}
	s.RunOneSided(c, 15000, true, 60*time.Second)
	s.RunOneSided(c, 15000, false, 60*time.Second)
}

func (s *MemReconSuite) TestOneSidedRidiculous(c *gc.C) {
	if !*long {
		c.Skip("long running test")
	}
	s.RunOneSided(c, 150000, true, 300*time.Second)
	s.RunOneSided(c, 150000, false, 300*time.Second)
}

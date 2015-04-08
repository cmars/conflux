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

package leveldb

import (
	"flag"
	"path/filepath"
	stdtesting "testing"
	"time"

	gc "gopkg.in/check.v1"

	"gopkg.in/hockeypuck/conflux.v2/recon"
	"gopkg.in/hockeypuck/conflux.v2/recon/testing"
)

var long = flag.Bool("long", false, "run long-running tests")

func Test(t *stdtesting.T) { gc.TestingT(t) }

type LeveldbReconSuite struct {
	*testing.ReconSuite
}

var _ = gc.Suite(&LeveldbReconSuite{})

func (s *LeveldbReconSuite) SetUpTest(c *gc.C) {
	s.ReconSuite = &testing.ReconSuite{
		Factory: func() (recon.PrefixTree, testing.Cleanup, error) {
			path := filepath.Join(c.MkDir(), "db")
			ptree, err := New(recon.DefaultSettings().PTreeConfig, path)
			c.Assert(err, gc.IsNil)
			err = ptree.Create()
			c.Assert(err, gc.IsNil)
			return ptree, func() {
				ptree.Drop()
			}, nil
		},
	}
}

func (s *LeveldbReconSuite) TestOneSidedMedium(c *gc.C) {
	s.RunOneSided(c, 250, true, 30*time.Second)
	s.RunOneSided(c, 250, false, 30*time.Second)
}

func (s *LeveldbReconSuite) TestOneSidedLarge(c *gc.C) {
	if !*long {
		c.Skip("long running test")
	}
	s.RunOneSided(c, 15000, true, 60*time.Second)
	s.RunOneSided(c, 15000, false, 60*time.Second)
}

func (s *LeveldbReconSuite) TestOneSidedRidiculous(c *gc.C) {
	if !*long {
		c.Skip("long running test")
	}
	s.RunOneSided(c, 150000, true, 300*time.Second)
	s.RunOneSided(c, 150000, false, 300*time.Second)
}

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

package testing

import (
	"testing"
	"time"

	gc "gopkg.in/check.v1"

	"github.com/cmars/conflux/recon"
)

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

func (s *MemReconSuite) TestOneSidedMediumLeft(c *gc.C) {
	s.RunOneSided(c, false, 250, 30*time.Second)
}

func (s *MemReconSuite) TestOneSidedMediumRight(c *gc.C) {
	s.RunOneSided(c, true, 250, 30*time.Second)
}

func (s *MemReconSuite) TestOneSidedMedium2Right(c *gc.C) {
	s.RunOneSided(c, true, 5000, 45*time.Second)
}

func (s *MemReconSuite) TestOneSidedLargeLeft(c *gc.C) {
	s.RunOneSided(c, false, 15000, 60*time.Second)
}

func (s *MemReconSuite) TestOneSidedLargeRight(c *gc.C) {
	s.RunOneSided(c, true, 15000, 60*time.Second)
}

func (s *MemReconSuite) TestOneSidedRidiculousLeft(c *gc.C) {
	s.RunOneSided(c, false, 150000, 300*time.Second)
}

func (s *MemReconSuite) TestOneSidedRidiculousRight(c *gc.C) {
	s.RunOneSided(c, true, 150000, 300*time.Second)
}

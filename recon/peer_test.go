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
	"net"

	gc "gopkg.in/check.v1"
)

type PeerSuite struct{}

var _ = gc.Suite(&PeerSuite{})

func (s *PeerSuite) TestResolveRecoverAddr(c *gc.C) {
	for _, testHostPort := range []string{"147.26.10.11:11370", "[fe80::d0dd:7dff:fefc:a828]:11370"} {
		reconAddr, err := net.ResolveTCPAddr("tcp", testHostPort)
		c.Assert(err, gc.IsNil)

		testHost, _, err := net.SplitHostPort(testHostPort)
		c.Assert(err, gc.IsNil)

		c.Assert(reconAddr.Port, gc.Equals, 11370)
		r := &Recover{
			RemoteAddr: reconAddr,
			RemoteConfig: &Config{
				HTTPPort: 8080,
			},
		}

		hkpHostPort, err := r.HkpAddr()
		c.Assert(err, gc.IsNil)

		hkpAddr, err := net.ResolveTCPAddr("tcp", hkpHostPort)
		c.Assert(err, gc.IsNil)

		hkpHost, _, err := net.SplitHostPort(hkpHostPort)
		c.Assert(err, gc.IsNil)

		c.Assert(hkpAddr.Port, gc.Equals, 8080)
		c.Assert(reconAddr.IP, gc.DeepEquals, hkpAddr.IP)
		c.Assert(testHost, gc.Equals, hkpHost)
	}
}

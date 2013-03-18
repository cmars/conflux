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
	"github.com/bmizerany/assert"
	"net"
	"testing"
	"time"
)

func TestJustOneSync(t *testing.T) {
	peer1ReconAddr, err := net.ResolveTCPAddr("tcp", "localhost:22743")
	assert.Equal(t, err, nil)
	peer2ReconAddr, err := net.ResolveTCPAddr("tcp", "localhost:22745")
	assert.Equal(t, err, nil)
	peer1 := NewMemPeer()
	peer1.Settings.(*DefaultSettings).httpPort = 22742
	peer1.Settings.(*DefaultSettings).reconPort = 22743
	peer1.Settings.(*DefaultSettings).gossipIntervalSecs = 1
	peer1.Settings.(*DefaultSettings).partners = []net.Addr{peer2ReconAddr}
	peer2 := NewMemPeer()
	peer2.Settings.(*DefaultSettings).httpPort = 22744
	peer2.Settings.(*DefaultSettings).reconPort = 22745
	peer2.Settings.(*DefaultSettings).gossipIntervalSecs = 1
	peer2.Settings.(*DefaultSettings).partners = []net.Addr{peer1ReconAddr}
	peer1.Start()
	peer2.Start()
	// Give peers time to sync
	time.Sleep(3 * time.Second)
	peer1.Stop()
	peer2.Stop()
}

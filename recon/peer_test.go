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
	. "github.com/cmars/conflux"
	"log"
	"net"
	"testing"
	"time"
)

func TestJustOneSync(t *testing.T) {
	peer1ReconAddr, err := net.ResolveTCPAddr("tcp", "localhost:22743")
	assert.Equal(t, err, nil)
	//peer2ReconAddr, err := net.ResolveTCPAddr("tcp", "localhost:22745")
	//assert.Equal(t, err, nil)
	peer1 := NewMemPeer()
	peer1.Settings.(*DefaultSettings).httpPort = 22742
	peer1.Settings.(*DefaultSettings).reconPort = 22743
	peer1.Settings.(*DefaultSettings).gossipIntervalSecs = 1
	//peer1.Settings.(*DefaultSettings).partners = []net.Addr{peer2ReconAddr}
	peer1.PrefixTree.Insert(Zi(P_SKS, 65537))
	peer2 := NewMemPeer()
	peer2.Settings.(*DefaultSettings).httpPort = 22744
	peer2.Settings.(*DefaultSettings).reconPort = 22745
	peer2.Settings.(*DefaultSettings).gossipIntervalSecs = 1
	peer2.Settings.(*DefaultSettings).partners = []net.Addr{peer1ReconAddr}
	peer2.PrefixTree.Insert(Zi(P_SKS, 65539))
	peer1.Start()
	peer2.Start()
	timer := time.NewTimer(time.Duration(120) * time.Second)
POLLING:
	for {
		select {
		case r1, ok := <-peer1.RecoverChan:
			t.Logf("Peer1 recover: %v", r1)
			log.Println("Peer1 recover:", r1)
			if !ok {
				break POLLING
			}
		case r2, ok := <-peer2.RecoverChan:
			t.Logf("Peer2 recover: %v", r2)
			log.Println("Peer2 recover:", r2)
			if !ok {
				break POLLING
			}
		case _ = <-timer.C:
			break POLLING
		}
	}
	peer1.Stop()
	peer2.Stop()
}

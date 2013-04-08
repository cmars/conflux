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

// Test full node sync.
func TestFullSync(t *testing.T) {
	peer1ReconAddr, err := net.ResolveTCPAddr("tcp", "localhost:22743")
	assert.Equal(t, err, nil)
	peer2ReconAddr, err := net.ResolveTCPAddr("tcp", "localhost:22745")
	assert.Equal(t, err, nil)
	peer1 := NewMemPeer()
	peer1.Settings.(*DefaultSettings).httpPort = 22742
	peer1.Settings.(*DefaultSettings).reconPort = 22743
	peer1.Settings.(*DefaultSettings).gossipIntervalSecs = 1
	peer1.Settings.(*DefaultSettings).partners = []net.Addr{peer2ReconAddr}
	peer1.PrefixTree.Insert(Zi(P_SKS, 65537))
	peer1.PrefixTree.Insert(Zi(P_SKS, 65539))
	root, _ := peer1.PrefixTree.Root()
	log.Println(root.(*MemPrefixNode).elements)
	peer2 := NewMemPeer()
	peer2.Settings.(*DefaultSettings).httpPort = 22744
	peer2.Settings.(*DefaultSettings).reconPort = 22745
	peer2.Settings.(*DefaultSettings).gossipIntervalSecs = 1
	peer2.Settings.(*DefaultSettings).partners = []net.Addr{peer1ReconAddr}
	peer2.PrefixTree.Insert(Zi(P_SKS, 65537))
	peer2.PrefixTree.Insert(Zi(P_SKS, 65541))
	root, _ = peer2.PrefixTree.Root()
	log.Println(root.(*MemPrefixNode).elements)
	peer1.Start()
	peer2.Start()
	timer := time.NewTimer(time.Duration(120) * time.Second)
POLLING:
	for {
		select {
		case r1, ok := <-peer1.RecoverChan:
			t.Logf("Peer1 recover: %v", r1)
			log.Println("Peer1 recover:", r1)
			for _, zp := range r1.RemoteElements {
				assert.T(t, zp != nil)
				peer1.PrefixTree.Insert(zp)
			}
			if !ok {
				break POLLING
			}
		case r2, ok := <-peer2.RecoverChan:
			t.Logf("Peer2 recover: %v", r2)
			log.Println("Peer2 recover:", r2)
			for _, zp := range r2.RemoteElements {
				assert.T(t, zp != nil)
				peer2.PrefixTree.Insert(zp)
			}
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

// Test sync with polynomial interpolation.
func TestPolySyncMbar(t *testing.T) {
	peer1ReconAddr, err := net.ResolveTCPAddr("tcp", "localhost:22743")
	assert.Equal(t, err, nil)
	peer2ReconAddr, err := net.ResolveTCPAddr("tcp", "localhost:22745")
	assert.Equal(t, err, nil)
	peer1 := NewMemPeer()
	peer1.Settings.(*DefaultSettings).httpPort = 22742
	peer1.Settings.(*DefaultSettings).reconPort = 22743
	peer1.Settings.(*DefaultSettings).gossipIntervalSecs = 1
	peer1.Settings.(*DefaultSettings).partners = []net.Addr{peer2ReconAddr}
	for i := 1; i < 100; i++ {
		peer1.PrefixTree.Insert(Zi(P_SKS, 65537*i))
	}
	// Four extra samples
	for i := 1; i < 5; i++ {
		peer1.PrefixTree.Insert(Zi(P_SKS, 68111*i))
	}
	root, _ := peer1.PrefixTree.Root()
	log.Println(root.(*MemPrefixNode).elements)
	peer2 := NewMemPeer()
	peer2.Settings.(*DefaultSettings).httpPort = 22744
	peer2.Settings.(*DefaultSettings).reconPort = 22745
	peer2.Settings.(*DefaultSettings).gossipIntervalSecs = 1
	peer2.Settings.(*DefaultSettings).partners = []net.Addr{peer1ReconAddr}
	for i := 1; i < 100; i++ {
		peer2.PrefixTree.Insert(Zi(P_SKS, 65537*i))
	}
	// One extra sample
	for i := 1; i < 2; i++ {
		peer2.PrefixTree.Insert(Zi(P_SKS, 70001*i))
	}
	root, _ = peer2.PrefixTree.Root()
	log.Println(root.(*MemPrefixNode).elements)
	peer1.Start()
	peer2.Start()
	timer := time.NewTimer(time.Duration(120) * time.Second)
POLLING:
	for {
		select {
		case r1, ok := <-peer1.RecoverChan:
			t.Logf("Peer1 recover: %v", r1)
			log.Println("Peer1 recover:", r1)
			for _, zp := range r1.RemoteElements {
				assert.T(t, zp != nil)
				peer1.Insert(zp)
			}
			if !ok {
				break POLLING
			}
		case r2, ok := <-peer2.RecoverChan:
			t.Logf("Peer2 recover: %v", r2)
			log.Println("Peer2 recover:", r2)
			for _, zp := range r2.RemoteElements {
				assert.T(t, zp != nil)
				peer2.Insert(zp)
			}
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

// Test sync with polynomial interpolation.
func TestPolySyncLowMBar(t *testing.T) {
	//peer1ReconAddr, err := net.ResolveTCPAddr("tcp", "localhost:22743")
	//assert.Equal(t, err, nil)
	peer2ReconAddr, err := net.ResolveTCPAddr("tcp", "localhost:22745")
	assert.Equal(t, err, nil)
	peer1 := NewMemPeer()
	peer1.Settings.(*DefaultSettings).httpPort = 22742
	peer1.Settings.(*DefaultSettings).reconPort = 22743
	peer1.Settings.(*DefaultSettings).logName = "peer1"
	peer1.Settings.(*DefaultSettings).gossipIntervalSecs = 1
	peer1.Settings.(*DefaultSettings).partners = []net.Addr{peer2ReconAddr}
	for i := 1; i < 100; i++ {
		peer1.PrefixTree.Insert(Zi(P_SKS, 65537*i))
	}
	// Four extra samples
	for i := 1; i < 50; i++ {
		peer1.PrefixTree.Insert(Zi(P_SKS, 68111*i))
	}
	root1, _ := peer1.PrefixTree.Root()
	log.Println(root1.(*MemPrefixNode).Elements())
	peer2 := NewMemPeer()
	peer2.Settings.(*DefaultSettings).httpPort = 22744
	peer2.Settings.(*DefaultSettings).reconPort = 22745
	peer2.Settings.(*DefaultSettings).logName = "peer2"
	peer2.Settings.(*DefaultSettings).gossipIntervalSecs = 1
	//peer2.Settings.(*DefaultSettings).partners = []net.Addr{peer1ReconAddr}
	for i := 1; i < 100; i++ {
		peer2.PrefixTree.Insert(Zi(P_SKS, 65537*i))
	}
	// One extra sample
	for i := 1; i < 20; i++ {
		peer2.PrefixTree.Insert(Zi(P_SKS, 70001*i))
	}
	root2, _ := peer2.PrefixTree.Root()
	log.Println(root2.(*MemPrefixNode).Elements())
	peer1.Start()
	peer2.Start()
	timer := time.NewTimer(time.Duration(120) * time.Second)
POLLING:
	for {
		select {
		case r1, ok := <-peer1.RecoverChan:
			if !ok {
				break POLLING
			}
			items := r1.RemoteElements
			log.Println("Peer1 recover:", items)
			for _, zp := range items {
				assert.T(t, zp != nil)
				log.Println("Peer1 insert:", zp)
				peer1.Insert(zp)
			}
		case r2, ok := <-peer2.RecoverChan:
			if !ok {
				break POLLING
			}
			items := r2.RemoteElements
			/*
				peer2.execCmd(func() error{
					root2, err := peer2.Root()
					log.Println("Peer2 has:", root2.Elements())
					return err
				})
			*/
			log.Println("Peer2 recover:", items)
			for _, zp := range items {
				assert.T(t, zp != nil)
				log.Println("Peer2 insert:", zp)
				peer2.Insert(zp)
			}
		case _ = <-timer.C:
			break POLLING
		}
	}
	peer1.Stop()
	peer2.Stop()
}

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

// Package testing provides some unit-testing support functions.
package testing

import (
	"github.com/bmizerany/assert"
	. "github.com/cmars/conflux"
	. "github.com/cmars/conflux/recon"
	"log"
	"testing"
	"time"
)

type PeerManager interface {
	CreatePeer() (*Peer, string)
	DestroyPeer(*Peer, string)
}

// Test full node sync.
func RunFullSync(t *testing.T, peerMgr PeerManager) {
	peer1ReconAddr := "localhost:22743"
	peer2ReconAddr := "localhost:22745"
	peer1, peer1Path := peerMgr.CreatePeer()
	defer peerMgr.DestroyPeer(peer1, peer1Path)
	peer1.Settings.Set("conflux.recon.logname", "peer1")
	peer1.Settings.Set("conflux.recon.httpPort", 22742)
	peer1.Settings.Set("conflux.recon.reconPort", 22743)
	peer1.Settings.Set("conflux.recon.gossipIntervalSecs", 30)
	peer1.Settings.Set("conflux.recon.partners", []interface{}{peer2ReconAddr})
	peer1.Settings.Set("conflux.recon.readTimeout", 10)
	peer1.Settings.Set("conflux.recon.connTimeout", 3)
	peer1.PrefixTree.Insert(Zi(P_SKS, 65537))
	peer1.PrefixTree.Insert(Zi(P_SKS, 65539))
	root, _ := peer1.PrefixTree.Root()
	log.Println(root.Elements())
	peer2, peer2Path := peerMgr.CreatePeer()
	defer peerMgr.DestroyPeer(peer2, peer2Path)
	peer2.Settings.Set("conflux.recon.logname", "peer2")
	peer2.Settings.Set("conflux.recon.httpPort", 22744)
	peer2.Settings.Set("conflux.recon.reconPort", 22745)
	peer2.Settings.Set("conflux.recon.gossipIntervalSecs", 30)
	peer2.Settings.Set("conflux.recon.partners", []interface{}{peer1ReconAddr})
	peer2.Settings.Set("conflux.recon.readTimeout", 10)
	peer2.Settings.Set("conflux.recon.connTimeout", 3)
	peer2.PrefixTree.Insert(Zi(P_SKS, 65537))
	peer2.PrefixTree.Insert(Zi(P_SKS, 65541))
	root, _ = peer2.PrefixTree.Root()
	log.Println(root.Elements())
	peer1.Start()
	peer2.Start()
	timer := time.NewTimer(time.Duration(120) * time.Second)
	var zs1 *ZSet = NewZSet()
	var zs2 *ZSet = NewZSet()
POLLING:
	for {
		select {
		case r1, ok := <-peer1.RecoverChan:
			if !ok {
				break POLLING
			}
			t.Logf("Peer1 recover: %v", r1)
			log.Println("Peer1 recover:", r1)
			for _, zp := range r1.RemoteElements {
				assert.T(t, zp != nil)
				peer1.Insert(zp)
			}
			peer1.ExecCmd(func() error {
				root1, err := peer1.Root()
				assert.Equal(t, err, nil)
				zs1 = NewZSet(root1.Elements()...)
				return err
			})
		case r2, ok := <-peer2.RecoverChan:
			if !ok {
				break POLLING
			}
			t.Logf("Peer2 recover: %v", r2)
			log.Println("Peer2 recover:", r2)
			for _, zp := range r2.RemoteElements {
				assert.T(t, zp != nil)
				peer2.Insert(zp)
			}
			peer2.ExecCmd(func() error {
				root2, err := peer2.Root()
				assert.Equal(t, err, nil)
				zs2 = NewZSet(root2.Elements()...)
				return err
			})
		case _ = <-timer.C:
			t.FailNow()
		}
		if zs1.Equal(zs2) {
			return
		}
	}
}

// Test sync with polynomial interpolation.
func RunPolySyncMBar(t *testing.T, peerMgr PeerManager) {
	//peer1ReconAddr := "localhost:22743"
	peer2ReconAddr := "localhost:22745"
	peer1, peer1Path := peerMgr.CreatePeer()
	defer peerMgr.DestroyPeer(peer1, peer1Path)
	peer1.Settings.Set("conflux.recon.logname", "peer1")
	peer1.Settings.Set("conflux.recon.httpPort", 22742)
	peer1.Settings.Set("conflux.recon.reconPort", 22743)
	peer1.Settings.Set("conflux.recon.gossipIntervalSecs", 30)
	peer1.Settings.Set("conflux.recon.partners", []interface{}{peer2ReconAddr})
	peer1.Settings.Set("conflux.recon.readTimeout", 10)
	peer1.Settings.Set("conflux.recon.connTimeout", 3)
	for i := 1; i < 100; i++ {
		peer1.PrefixTree.Insert(Zi(P_SKS, 65537*i))
	}
	// Four extra samples
	for i := 1; i < 5; i++ {
		peer1.PrefixTree.Insert(Zi(P_SKS, 68111*i))
	}
	root, _ := peer1.PrefixTree.Root()
	log.Println(root.Elements())
	peer2, peer2Path := peerMgr.CreatePeer()
	defer peerMgr.DestroyPeer(peer2, peer2Path)
	peer2.Settings.Set("conflux.recon.logname", "peer2")
	peer2.Settings.Set("conflux.recon.httpPort", 22744)
	peer2.Settings.Set("conflux.recon.reconPort", 22745)
	peer2.Settings.Set("conflux.recon.gossipIntervalSecs", 30)
	peer2.Settings.Set("conflux.recon.partners", []interface{}{ /*peer2ReconAddr*/})
	peer2.Settings.Set("conflux.recon.readTimeout", 10)
	peer2.Settings.Set("conflux.recon.connTimeout", 3)
	for i := 1; i < 100; i++ {
		peer2.PrefixTree.Insert(Zi(P_SKS, 65537*i))
	}
	// One extra sample
	for i := 1; i < 2; i++ {
		peer2.PrefixTree.Insert(Zi(P_SKS, 70001*i))
	}
	root, _ = peer2.PrefixTree.Root()
	log.Println(root.Elements())
	peer2.Start()
	peer1.Start()
	timer := time.NewTimer(time.Duration(120) * time.Second)
	var zs1 *ZSet = NewZSet()
	var zs2 *ZSet = NewZSet()
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
			peer1.ExecCmd(func() error {
				root1, err := peer1.Root()
				assert.Equal(t, err, nil)
				zs1 = NewZSet(root1.Elements()...)
				return err
			})
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
			peer2.ExecCmd(func() error {
				root2, err := peer2.Root()
				assert.Equal(t, err, nil)
				zs2 = NewZSet(root2.Elements()...)
				return err
			})
		case _ = <-timer.C:
			t.FailNow()
		}
		if zs1.Equal(zs2) {
			return
		}
	}
}

// Test sync with polynomial interpolation.
func RunPolySyncLowMBar(t *testing.T, peerMgr PeerManager) {
	peer2ReconAddr := "localhost:22745"
	peer1, peer1Path := peerMgr.CreatePeer()
	defer peerMgr.DestroyPeer(peer1, peer1Path)
	peer1.Settings.Set("conflux.recon.logname", "peer1")
	peer1.Settings.Set("conflux.recon.httpPort", 22742)
	peer1.Settings.Set("conflux.recon.reconPort", 22743)
	peer1.Settings.Set("conflux.recon.gossipIntervalSecs", 30)
	peer1.Settings.Set("conflux.recon.partners", []interface{}{peer2ReconAddr})
	peer1.Settings.Set("conflux.recon.readTimeout", 10)
	peer1.Settings.Set("conflux.recon.connTimeout", 3)
	for i := 1; i < 100; i++ {
		peer1.PrefixTree.Insert(Zi(P_SKS, 65537*i))
	}
	// Four extra samples
	for i := 1; i < 50; i++ {
		peer1.PrefixTree.Insert(Zi(P_SKS, 68111*i))
	}
	root1, _ := peer1.PrefixTree.Root()
	log.Println(root1.Elements())
	peer2, peer2Path := peerMgr.CreatePeer()
	defer peerMgr.DestroyPeer(peer2, peer2Path)
	peer2.Settings.Set("conflux.recon.logname", "peer2")
	peer2.Settings.Set("conflux.recon.httpPort", 22744)
	peer2.Settings.Set("conflux.recon.reconPort", 22745)
	peer2.Settings.Set("conflux.recon.gossipIntervalSecs", 30)
	peer2.Settings.Set("conflux.recon.partners", []interface{}{ /*peer2ReconAddr*/})
	peer2.Settings.Set("conflux.recon.readTimeout", 10)
	peer2.Settings.Set("conflux.recon.connTimeout", 3)
	for i := 1; i < 100; i++ {
		peer2.PrefixTree.Insert(Zi(P_SKS, 65537*i))
	}
	// One extra sample
	for i := 1; i < 20; i++ {
		peer2.PrefixTree.Insert(Zi(P_SKS, 70001*i))
	}
	root2, _ := peer2.PrefixTree.Root()
	log.Println(root2.Elements())
	peer2.Start()
	peer1.Start()
	timer := time.NewTimer(time.Duration(120) * time.Second)
	var zs1 *ZSet = NewZSet()
	var zs2 *ZSet = NewZSet()
POLLING:
	for {
		select {
		case r1, ok := <-peer1.RecoverChan:
			if !ok {
				break POLLING
			}
			peer1.ExecCmd(func() error {
				root1, err := peer1.Root()
				assert.Equal(t, err, nil)
				log.Println("Peer1 has", len(root1.Elements()))
				return nil
			})
			items := r1.RemoteElements
			log.Println("Peer1 recover:", items)
			for _, zp := range items {
				assert.T(t, zp != nil)
				log.Println("Peer1 insert:", zp)
				peer1.Insert(zp)
			}
			peer1.ExecCmd(func() error {
				root1, err := peer1.Root()
				assert.Equal(t, err, nil)
				zs1 = NewZSet(root1.Elements()...)
				return err
			})
		case r2, ok := <-peer2.RecoverChan:
			if !ok {
				break POLLING
			}
			peer2.ExecCmd(func() error {
				root2, err := peer2.Root()
				assert.Equal(t, err, nil)
				log.Println("Peer2 has", len(root2.Elements()))
				return nil
			})
			items := r2.RemoteElements
			log.Println("Peer2 recover:", items)
			for _, zp := range items {
				assert.T(t, zp != nil)
				log.Println("Peer2 insert:", zp)
				peer2.Insert(zp)
			}
			peer2.ExecCmd(func() error {
				root2, err := peer2.Root()
				assert.Equal(t, err, nil)
				zs2 = NewZSet(root2.Elements()...)
				return err
			})
		case _ = <-timer.C:
			t.FailNow()
		}
		if zs1.Equal(zs2) {
			return
		}
	}
}

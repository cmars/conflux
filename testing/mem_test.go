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
	. "github.com/cmars/conflux/recon"
	"os"
	"testing"
)

type memPeerManager struct{}

func (mpm *memPeerManager) CreatePeer() (peer *Peer, path string) {
	peer = NewMemPeer()
	go peer.HandleCmds()
	return peer, ""
}

func (mpm *memPeerManager) DestroyPeer(peer *Peer, path string) {
	if peer != nil {
		peer.Stop()
	}
	if path != "" {
		os.RemoveAll(path)
	}
}

var memPeerMgr *memPeerManager = &memPeerManager{}

// Test full node sync.
func TestFullSync(t *testing.T) {
	RunFullSync(t, memPeerMgr)
}

// Test sync with polynomial interpolation.
func TestPolySyncMBar(t *testing.T) {
	RunPolySyncMBar(t, memPeerMgr)
}

// Test sync with polynomial interpolation.
func TestPolySyncLowMBar(t *testing.T) {
	RunPolySyncLowMBar(t, memPeerMgr)
}

func TestOneSidedMedium(t *testing.T) {
	RunOneSided(t, memPeerMgr, false, 250, 30)
	RunOneSided(t, memPeerMgr, true, 250, 30)
}

func TestOneSidedLarge(t *testing.T) {
	RunOneSided(t, memPeerMgr, false, 15000, 60)
	RunOneSided(t, memPeerMgr, true, 15000, 60)
}

func TestOneSidedRidiculous(t *testing.T) {
	RunOneSided(t, memPeerMgr, false, 150000, 180)
	RunOneSided(t, memPeerMgr, true, 150000, 180)
}

func TestSplits85(t *testing.T) {
	RunSplits85(t, memPeerMgr)
}

func TestSplits15k(t *testing.T) {
	RunSplits15k(t, memPeerMgr)
}

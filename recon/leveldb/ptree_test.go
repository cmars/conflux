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

package leveldb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/bmizerany/assert"

	. "github.com/cmars/conflux"
	"github.com/cmars/conflux/recon"
)

const TEST_DB = "recon_test"

func createTestPeer(t *testing.T) *recon.Peer {
	settings := DefaultSettings()
	tmpdir, err := ioutil.TempDir("", "leveldb_test")
	assert.Equal(t, err, nil)
	settings.Set("conflux.recon.leveldb.path", tmpdir)
	t.Log(tmpdir)
	ptree, err := New(settings)
	assert.Equal(t, err, nil)
	err = ptree.Create()
	assert.Equal(t, err, nil)
	peer := recon.NewPeer(settings.Settings, ptree)
	go peer.HandleCmds()
	return peer
}

func destroyTestPeer(peer *recon.Peer) {
	os.RemoveAll(peer.Settings.GetString("conflux.recon.leveldb.path", ""))
}

func TestInsertNodesNoSplit(t *testing.T) {
	peer := createTestPeer(t)
	defer destroyTestPeer(peer)
	peer.PrefixTree.Insert(Zi(P_SKS, 100))
	peer.PrefixTree.Insert(Zi(P_SKS, 300))
	peer.PrefixTree.Insert(Zi(P_SKS, 500))
	root, err := peer.PrefixTree.Root()
	assert.Equal(t, nil, err)
	assert.Equal(t, 3, len(root.Elements()))
	assert.T(t, root.IsLeaf())
	peer.PrefixTree.Remove(Zi(P_SKS, 100))
	peer.PrefixTree.Remove(Zi(P_SKS, 300))
	peer.PrefixTree.Remove(Zi(P_SKS, 500))
	root, err = peer.PrefixTree.Root()
	assert.Equal(t, 0, len(root.Elements()))
	for _, sv := range root.SValues() {
		assert.Equal(t, 0, sv.Cmp(Zi(P_SKS, 1)))
	}
}

func TestJustOneKey(t *testing.T) {
	peer := createTestPeer(t)
	defer destroyTestPeer(peer)
	tree := peer.PrefixTree
	tree.Init()
	root, err := tree.Root()
	assert.Equal(t, err, nil)
	tree.Insert(Zs(P_SKS, "224045810486609649306292620830306652473"))
	expect := NewZSet()
	for _, sv := range []string{
		"306467079064992673198834899522272784866",
		"306467079064992673198834899522272784865",
		"306467079064992673198834899522272784867",
		"306467079064992673198834899522272784864",
		"306467079064992673198834899522272784868",
		"306467079064992673198834899522272784863"} {
		expect.Add(Zs(P_SKS, sv))
	}
	assert.Equal(t, err, nil)
	root, err = tree.Root()
	for _, sv := range root.SValues() {
		assert.Tf(t, expect.Has(sv), "Unexpected svalue: %v", sv)
		expect.Remove(sv)
	}
	assert.Equal(t, 0, len(expect.Items()))
}

func TestInsertRemoveProtection(t *testing.T) {
	peer := createTestPeer(t)
	defer destroyTestPeer(peer)
	tree := peer.PrefixTree
	tree.Init()
	root, err := tree.Root()
	// Snapshot original svalues
	origSValues := root.SValues()
	assert.Equal(t, err, nil)
	// Add an element, should succeed
	err = tree.Insert(Zs(P_SKS, "224045810486609649306292620830306652473"))
	assert.Equal(t, err, nil)
	// Snapshot svalues with one element added
	root, err = tree.Root()
	assert.Equal(t, err, nil)
	oneSValues := root.SValues()
	for i, sv := range oneSValues {
		assert.NotEqual(t, origSValues[i], sv)
	}
	// Attempt to insert duplicate element, should fail
	err = tree.Insert(Zs(P_SKS, "224045810486609649306292620830306652473"))
	assert.T(t, err != nil)
	// After attempt to insert duplicate, svalues should be unchanged
	root, err = tree.Root()
	assert.Equal(t, err, nil)
	oneDupSValues := root.SValues()
	for i, sv := range oneSValues {
		assert.Equal(t, oneDupSValues[i], sv)
	}
	// Remove element, should be back to original svalues
	err = tree.Remove(Zs(P_SKS, "224045810486609649306292620830306652473"))
	assert.Equal(t, err, nil)
	root, err = tree.Root()
	assert.Equal(t, err, nil)
	rmNotExist := root.SValues()
	for i, sv := range rmNotExist {
		assert.Equal(t, origSValues[i], sv)
	}
	// Remove non-existent element, svalues should be unchanged
	err = tree.Remove(Zs(P_SKS, "224045810486609649306292620830306652473"))
	assert.T(t, err != nil)
	root, err = tree.Root()
	assert.Equal(t, err, nil)
	for i, sv := range root.SValues() {
		assert.Equal(t, origSValues[i], sv)
	}
}

func TestInsertDups(t *testing.T) {
	var err error
	peer := createTestPeer(t)
	defer destroyTestPeer(peer)
	tree := peer.PrefixTree
	tree.Init()
	items := []*Zp{}
	for i := 0; i < tree.SplitThreshold()*4; i++ {
		z := Zrand(P_SKS)
		items = append(items, z)
		err := tree.Insert(z)
		assert.Equal(t, err, nil)
		for j := 0; j < 100; j++ {
			err = tree.Insert(z)
			assert.T(t, err != nil)
		}
	}
	tree.Close()
	// Re-open and insert same keys, should be dups
	tree, err = New(tree.(*prefixTree).Settings)
	assert.Equal(t, err, nil)
	err = tree.Create()
	assert.Equal(t, err, nil)
	for _, z := range items {
		err = tree.Insert(z)
		assert.T(t, err != nil)
	}
}

func TestInsertNodeSplit(t *testing.T) {
	peer := createTestPeer(t)
	defer destroyTestPeer(peer)
	tree := peer.PrefixTree
	tree.Init()
	root, err := tree.Root()
	for _, sv := range root.SValues() {
		t.Log("SV:", sv)
		assert.Equal(t, 0, sv.Cmp(Zi(P_SKS, 1)))
	}
	// Add a bunch of nodes, enough to cause splits
	for i := 0; i < tree.SplitThreshold()*4; i++ {
		z := Zi(P_SKS, i+65536)
		t.Log("Insert:", z)
		tree.Insert(z)
	}
	// Remove a bunch of nodes, enough to cause joins
	for i := 0; i < tree.SplitThreshold()*4; i++ {
		z := Zi(P_SKS, i+65536)
		t.Log("Remove:", z)
		tree.Remove(z)
	}
	root, err = tree.Root()
	assert.Equal(t, err, nil)
	// Insert/Remove reversible after splitting & joining?
	for _, sv := range root.SValues() {
		t.Log("SV:", sv)
		assert.Equal(t, 0, sv.Cmp(Zi(P_SKS, 1)))
	}
	assert.Equal(t, 0, len(root.Children()))
	assert.Equal(t, 0, len(root.Elements()))
}

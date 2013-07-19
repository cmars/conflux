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
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"github.com/bmizerany/assert"
	. "github.com/cmars/conflux"
	"github.com/cmars/conflux/recon"
	"os"
	"path/filepath"
	"testing"
)

const TEST_DB = "recon_test"

func createTestPeer(t *testing.T) (*recon.Peer, string) {
	tag := make([]byte, 32)
	rand.Read(tag)
	suffix := base64.URLEncoding.EncodeToString(tag)
	testDbDir := filepath.Join(os.TempDir(), fmt.Sprintf("conflux-cask-test.%v", suffix))
	err := os.MkdirAll(testDbDir, 0755)
	assert.Equal(t, err, nil)
	settings := DefaultSettings()
	settings.Set("conflux.recon.leveldb.path", testDbDir)
	peer, err := NewPeer(settings)
	assert.Equal(t, err, nil)
	return peer, testDbDir
}

func destroyTestPeer(peer *recon.Peer, path string) {
	peer.Stop()
	peer.PrefixTree.(*prefixTree).ptree.Close()
	//levigo.DestroyDatabase(path, peer.PrefixTree.(*prefixTree).options)
	os.RemoveAll(path)
}

func TestInsertNodesNoSplit(t *testing.T) {
	peer, path := createTestPeer(t)
	defer destroyTestPeer(peer, path)
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
	peer, path := createTestPeer(t)
	defer destroyTestPeer(peer, path)
	tree := peer.PrefixTree
	tree.Init()
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
	root, err := tree.Root()
	assert.Equal(t, err, nil)
	for _, sv := range root.SValues() {
		assert.T(t, expect.Has(sv))
		expect.Remove(sv)
	}
	assert.Equal(t, 0, len(expect.Items()))
}

func TestInsertNodeSplit(t *testing.T) {
	peer, path := createTestPeer(t)
	defer destroyTestPeer(peer, path)
	tree := peer.PrefixTree
	tree.Init()
	// Add a bunch of nodes, enough to cause splits
	for i := 0; i < tree.SplitThreshold()*4; i++ {
		z := Zi(P_SKS, i+65536)
		//t.Log("Insert:", z)
		tree.Insert(z)
	}
	// Remove a bunch of nodes, enough to cause joins
	for i := 0; i < tree.SplitThreshold()*4; i++ {
		z := Zi(P_SKS, i+65536)
		//t.Log("Remove:", z)
		tree.Remove(z)
	}
	root, err := tree.Root()
	assert.Equal(t, err, nil)
	// Insert/Remove reversible after splitting & joining?
	for _, sv := range root.SValues() {
		assert.Equal(t, 0, sv.Cmp(Zi(P_SKS, 1)))
	}
	assert.Equal(t, 0, len(root.Children()))
	assert.Equal(t, 0, len(root.Elements()))
}

/*
// Test key consistency
func TestKeyMatch(t *testing.T) {
	tree1 := new(MemPrefixTree)
	tree1.Init()
	for i := 1; i < 100; i++ {
		tree1.Insert(Zi(P_SKS, 65537*i+i))
	}
	// Some extra samples
	for i := 1; i < 50; i++ {
		tree1.Insert(Zi(P_SKS, 68111*i))
	}
	tree2 := new(MemPrefixTree)
	tree2.Init()
	for i := 1; i < 100; i++ {
		tree2.Insert(Zi(P_SKS, 65537*i))
	}
	// One extra sample
	for i := 1; i < 20; i++ {
		tree2.Insert(Zi(P_SKS, 70001*i))
	}
	for i := 1; i < 100; i++ {
		zi := Zi(P_SKS, 65537*i)
		bs := NewBitstring(P_SKS.BitLen())
		bs.SetBytes(ReverseBytes(zi.Bytes()))
		node1, err := Find(tree1, zi)
		assert.Equal(t, err, nil)
		node2, err := Find(tree2, zi)
		assert.Equal(t, err, nil)
		t.Logf("node1=%v, node2=%v (%b) full=%v", node1.Key(), node2.Key(), zi.Int64(), bs)
		// If keys are different, one must prefix the other.
		assert.T(t, strings.HasPrefix(node1.Key().String(), node2.Key().String()) ||
			strings.HasPrefix(node2.Key().String(), node1.Key().String()))
	}
}
*/

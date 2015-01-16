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
	. "github.com/cmars/conflux"
	"strings"

	gc "gopkg.in/check.v1"
)

type PtreeSuite struct{}

var _ = gc.Suite(&PtreeSuite{})

func (s *PtreeSuite) TestInsertNodesNoSplit(c *gc.C) {
	tree := new(MemPrefixTree)
	tree.Init()
	tree.Insert(Zi(P_SKS, 100))
	tree.Insert(Zi(P_SKS, 300))
	tree.Insert(Zi(P_SKS, 500))
	root, err := tree.Root()
	c.Assert(err, gc.IsNil)
	c.Assert(MustElements(root), gc.HasLen, 3)
	c.Assert(root.IsLeaf(), gc.Equals, true)
	tree.Remove(Zi(P_SKS, 100))
	tree.Remove(Zi(P_SKS, 300))
	tree.Remove(Zi(P_SKS, 500))
	c.Assert(MustElements(root), gc.HasLen, 0)
	for _, sv := range root.SValues() {
		c.Assert(sv.Cmp(Zi(P_SKS, 1)), gc.Equals, 0)
	}
}

func (s *PtreeSuite) TestJustOneKey(c *gc.C) {
	tree := new(MemPrefixTree)
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
	c.Assert(err, gc.IsNil)
	for _, sv := range root.SValues() {
		c.Assert(expect.Has(sv), gc.Equals, true)
		expect.Remove(sv)
	}
	c.Assert(expect.Items(), gc.HasLen, 0)
}

func (s *PtreeSuite) TestInsertNodeSplit(c *gc.C) {
	tree := new(MemPrefixTree)
	tree.Init()
	// Add a bunch of nodes, enough to cause splits
	for i := 0; i < tree.SplitThreshold()*4; i++ {
		tree.Insert(Zi(P_SKS, i+65536))
	}
	// Remove a bunch of nodes, enough to cause joins
	for i := 0; i < tree.SplitThreshold()*4; i++ {
		tree.Remove(Zi(P_SKS, i+65536))
	}
	root, err := tree.Root()
	c.Assert(err, gc.IsNil)
	// Insert/Remove reversible after splitting & joining?
	for _, sv := range root.SValues() {
		c.Assert(sv.Cmp(Zi(P_SKS, 1)), gc.Equals, 0)
	}
	c.Assert(tree.root.children, gc.HasLen, 0)
	c.Assert(tree.root.elements, gc.HasLen, 0)
}

// TestKeyMatch tests key consistency
func (s *PtreeSuite) TestKeyMatch(c *gc.C) {
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
		bs := NewZpBitstring(zi)
		node1, err := Find(tree1, zi)
		c.Assert(err, gc.IsNil)
		node2, err := Find(tree2, zi)
		c.Assert(err, gc.IsNil)
		c.Logf("node1=%v, node2=%v (%b) full=%v", node1.Key(), node2.Key(), zi.Int64(), bs)
		// If keys are different, one must prefix the other.
		c.Assert(strings.HasPrefix(node1.Key().String(), node2.Key().String()) ||
			strings.HasPrefix(node2.Key().String(), node1.Key().String()), gc.Equals, true)
	}
}

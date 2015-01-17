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

package leveldb

import (
	"path/filepath"

	gc "gopkg.in/check.v1"

	cf "gopkg.in/hockeypuck/conflux.v2"
	"gopkg.in/hockeypuck/conflux.v2/recon"
)

const TEST_DB = "recon_test"

type PtreeSuite struct {
	config recon.PTreeConfig
	path   string

	ptree recon.PrefixTree
}

var _ = gc.Suite(&PtreeSuite{})

func (s *PtreeSuite) SetUpTest(c *gc.C) {
	s.config = recon.DefaultSettings().PTreeConfig
	s.path = filepath.Join(c.MkDir(), "db")
	ptree, err := New(s.config, s.path)
	c.Assert(err, gc.IsNil)
	err = ptree.Create()
	c.Assert(err, gc.IsNil)
	s.ptree = ptree
}

func (s *PtreeSuite) TearDownTest(c *gc.C) {
	if s.ptree != nil {
		s.ptree.Drop()
	}
}

func (s *PtreeSuite) TestInsertNodesNoSplit(c *gc.C) {
	s.ptree.Insert(cf.Zi(cf.P_SKS, 100))
	s.ptree.Insert(cf.Zi(cf.P_SKS, 300))
	s.ptree.Insert(cf.Zi(cf.P_SKS, 500))
	root, err := s.ptree.Root()
	c.Assert(err, gc.IsNil)
	c.Assert(recon.MustElements(root), gc.HasLen, 3)
	c.Assert(root.IsLeaf(), gc.Equals, true)
	s.ptree.Remove(cf.Zi(cf.P_SKS, 100))
	s.ptree.Remove(cf.Zi(cf.P_SKS, 300))
	s.ptree.Remove(cf.Zi(cf.P_SKS, 500))
	root, err = s.ptree.Root()
	c.Assert(recon.MustElements(root), gc.HasLen, 0)
	for _, sv := range root.SValues() {
		c.Assert(sv.Cmp(cf.Zi(cf.P_SKS, 1)), gc.Equals, 0)
	}
}

func (s *PtreeSuite) TestJustOneKey(c *gc.C) {
	root, err := s.ptree.Root()
	c.Assert(err, gc.IsNil)
	s.ptree.Insert(cf.Zs(cf.P_SKS, "224045810486609649306292620830306652473"))
	expect := cf.NewZSet()
	for _, sv := range []string{
		"306467079064992673198834899522272784866",
		"306467079064992673198834899522272784865",
		"306467079064992673198834899522272784867",
		"306467079064992673198834899522272784864",
		"306467079064992673198834899522272784868",
		"306467079064992673198834899522272784863"} {
		expect.Add(cf.Zs(cf.P_SKS, sv))
	}
	c.Assert(err, gc.IsNil)
	root, err = s.ptree.Root()
	for _, sv := range root.SValues() {
		c.Assert(expect.Has(sv), gc.Equals, true, gc.Commentf("Unexpected svalue: %v", sv))
		expect.Remove(sv)
	}
	c.Assert(expect.Items(), gc.HasLen, 0)
}

func (s *PtreeSuite) TestInsertRemoveProtection(c *gc.C) {
	root, err := s.ptree.Root()
	// Snapshot original svalues
	origSValues := root.SValues()
	c.Assert(err, gc.IsNil)
	// Add an element, should succeed
	err = s.ptree.Insert(cf.Zs(cf.P_SKS, "224045810486609649306292620830306652473"))
	c.Assert(err, gc.IsNil)
	// Snapshot svalues with one element added
	root, err = s.ptree.Root()
	c.Assert(err, gc.IsNil)
	oneSValues := root.SValues()
	for i, sv := range oneSValues {
		c.Assert(origSValues[i].String(), gc.Not(gc.Equals), sv.String())
	}
	// Attempt to insert duplicate element, should fail
	err = s.ptree.Insert(cf.Zs(cf.P_SKS, "224045810486609649306292620830306652473"))
	c.Assert(err, gc.NotNil)
	// After attempt to insert duplicate, svalues should be unchanged
	root, err = s.ptree.Root()
	c.Assert(err, gc.IsNil)
	oneDupSValues := root.SValues()
	for i, sv := range oneSValues {
		c.Assert(oneDupSValues[i].String(), gc.Equals, sv.String())
	}
	// Remove element, should be back to original svalues
	err = s.ptree.Remove(cf.Zs(cf.P_SKS, "224045810486609649306292620830306652473"))
	c.Assert(err, gc.IsNil)
	root, err = s.ptree.Root()
	c.Assert(err, gc.IsNil)
	rmNotExist := root.SValues()
	for i, sv := range rmNotExist {
		c.Assert(origSValues[i].String(), gc.Equals, sv.String())
	}
	// Remove non-existent element, svalues should be unchanged
	err = s.ptree.Remove(cf.Zs(cf.P_SKS, "224045810486609649306292620830306652473"))
	c.Assert(err, gc.NotNil)
	root, err = s.ptree.Root()
	c.Assert(err, gc.IsNil)
	for i, sv := range root.SValues() {
		c.Assert(origSValues[i].String(), gc.Equals, sv.String())
	}
}

func (s *PtreeSuite) TestInsertDups(c *gc.C) {
	items := []*cf.Zp{}
	for i := 0; i < s.config.SplitThreshold()*4; i++ {
		z := cf.Zrand(cf.P_SKS)
		items = append(items, z)
		err := s.ptree.Insert(z)
		c.Assert(err, gc.IsNil)
		for j := 0; j < 100; j++ {
			err = s.ptree.Insert(z)
			c.Assert(err, gc.NotNil)
		}
	}
	s.ptree.Close()
	// Re-open and insert same keys, should be dups
	var err error
	s.ptree, err = New(s.ptree.(*prefixTree).PTreeConfig, s.path)
	c.Assert(err, gc.IsNil)
	err = s.ptree.Create()
	c.Assert(err, gc.IsNil)
	for _, z := range items {
		err = s.ptree.Insert(z)
		c.Assert(err, gc.NotNil)
	}
}

func (s *PtreeSuite) TestInsertNodeSplit(c *gc.C) {
	root, err := s.ptree.Root()
	for _, sv := range root.SValues() {
		c.Log("SV:", sv)
		c.Assert(sv.Cmp(cf.Zi(cf.P_SKS, 1)), gc.Equals, 0)
	}
	// Add a bunch of nodes, enough to cause splits
	for i := 0; i < s.config.SplitThreshold()*4; i++ {
		z := cf.Zi(cf.P_SKS, i+65536)
		c.Log("Insert:", z)
		s.ptree.Insert(z)
	}
	// Remove a bunch of nodes, enough to cause joins
	for i := 0; i < s.config.SplitThreshold()*4; i++ {
		z := cf.Zi(cf.P_SKS, i+65536)
		c.Log("Remove:", z)
		s.ptree.Remove(z)
	}
	root, err = s.ptree.Root()
	c.Assert(err, gc.IsNil)
	// Insert/Remove reversible after splitting & joining?
	for _, sv := range root.SValues() {
		c.Log("SV:", sv)
		c.Assert(sv.Cmp(cf.Zi(cf.P_SKS, 1)), gc.Equals, 0)
	}
	c.Assert(recon.MustChildren(root), gc.HasLen, 0)
	c.Assert(recon.MustElements(root), gc.HasLen, 0)
}

func (s *PtreeSuite) TestNewChildIndex(c *gc.C) {
	root, err := s.ptree.Root()
	c.Assert(err, gc.IsNil)
	rootNode := root.(*prefixNode)
	child00 := rootNode.newChildNode(rootNode, 0)
	c.Assert(child00.Key().Get(0), gc.Equals, 0)
	c.Assert(child00.Key().Get(1), gc.Equals, 0)
	child00.upsertNode()
	child11 := rootNode.newChildNode(rootNode, 3)
	child11.upsertNode()
	c.Assert(child11.Key().Get(0), gc.Equals, 1)
	c.Assert(child11.Key().Get(1), gc.Equals, 1)
}

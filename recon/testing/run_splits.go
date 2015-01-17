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

package testing

import (
	"fmt"

	gc "gopkg.in/check.v1"

	"gopkg.in/hockeypuck/conflux.v2/recon"
)

func lookupNode(key string, start recon.PrefixNode) (recon.PrefixNode, error) {
	node := start
	for len(key) > 0 {
		if node.IsLeaf() {
			return nil, fmt.Errorf("unexpected leaf node")
		}
		if len(key) < node.Config().BitQuantum {
			return nil, fmt.Errorf("bitstring alignment error, must be multiple of bitquantum")
		}
		childIndex := 0
		if key[0] == '1' {
			childIndex |= 0x1
		}
		if key[1] == '1' {
			childIndex |= 0x2
		}
		node = recon.MustChildren(node)[childIndex]
		key = key[2:]
	}
	return node, nil
}

func (s *ReconSuite) TestSplits85(c *gc.C) {
	ptree, cleanup, err := s.Factory()
	c.Assert(err, gc.IsNil)
	defer cleanup()

	for _, z := range PtreeSplits85 {
		err = ptree.Insert(z)
		c.Assert(err, gc.IsNil)
	}
	root, err := ptree.Root()
	c.Assert(err, gc.IsNil)
	c.Assert(85, gc.Equals, root.Size())
	for i, child := range recon.MustChildren(root) {
		c.Log("child#", i, ":", child.Key())
	}

	for _, svalue := range root.SValues() {
		c.Log("root svalue:", svalue)
	}

	for _, node := range recon.MustChildren(root) {
		c.Log("child:", node.Key(), "has", node.Size())
	}

	node, err := lookupNode("00", root)
	c.Assert(err, gc.IsNil)
	c.Assert(17, gc.Equals, node.Size())
	node, err = lookupNode("01", root)
	c.Assert(err, gc.IsNil)
	c.Assert(19, gc.Equals, node.Size())
	node, err = lookupNode("10", root)
	c.Assert(err, gc.IsNil)
	c.Assert(21, gc.Equals, node.Size())
	node, err = lookupNode("11", root)
	c.Assert(err, gc.IsNil)
	c.Assert(28, gc.Equals, node.Size())
}

func (s *ReconSuite) RunSplits15k(c *gc.C) {
	ptree, cleanup, err := s.Factory()
	c.Assert(err, gc.IsNil)
	defer cleanup()

	for _, z := range PtreeSplits15k {
		err = ptree.Insert(z)
		c.Assert(err, gc.IsNil)
	}
	root, err := ptree.Root()
	c.Assert(err, gc.IsNil)
	c.Assert(15000, gc.Equals, root.Size())
	node, err := lookupNode("11", root)
	c.Assert(err, gc.IsNil)
	c.Assert(15000, gc.Equals, node.Size())
	node, err = lookupNode("11011011", root)
	c.Assert(err, gc.IsNil)
	c.Assert(12995, gc.Equals, node.Size())
	node, err = lookupNode("1101101011", root)
	c.Assert(err, gc.IsNil)
	c.Assert(2005, gc.Equals, node.Size())
}

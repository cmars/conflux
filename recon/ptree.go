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
	"errors"
	. "github.com/cmars/conflux"
)

type PrefixTree interface {
	Node(key *Bitstring) (PrefixNode, error)
	Root() (PrefixNode, error)
	Points() []*Zp
	SplitThreshold() int
	JoinThreshold() int
	BitQuantum() int
	Insert(z *Zp) error
	Remove(z *Zp) error
}

type PrefixNode interface {
	Key() *Bitstring
	Elements() []*Zp
	Children() []*Bitstring
	Size() int
	SValues() []*Zp
	Add(z *Zp, marray []*Zp)
	IsLeaf() bool
}

var NodeNotFoundErr error = errors.New("Node not found")
var IndexOutOfRangeErr error = errors.New("Index out of range")

const memBitQuantum = 2
const memMbar = 5
const memThreshMult = 10
const memNumSamples = memMbar + 1

var memRootKey *Bitstring = NewBitstring(memBitQuantum)

type memPrefixTree struct {
	nodes  map[string]*memPrefixNode
	points []*Zp
}

type memPrefixNode struct {
	*memPrefixTree
	key      *Bitstring
	elements []*Zp
	isLeaf   bool
	svalues  []*Zp
}

func NewMemPrefixTree() PrefixTree {
	t := &memPrefixTree{}
	t.nodes[string(memRootKey.Bytes())] = &memPrefixNode{
		memPrefixTree: t, key: memRootKey}
	t.points = Zpoints(P_SKS, memNumSamples)
	return t
}

func (t *memPrefixTree) Node(key *Bitstring) (PrefixNode, error) {
	node, has := t.nodes[string(key.Bytes())]
	if has {
		return node, nil
	}
	return nil, NodeNotFoundErr
}

func (t *memPrefixTree) Root() (PrefixNode, error) {
	return t.Node(memRootKey)
}

func (t *memPrefixTree) Points() []*Zp { return t.points }

func (t *memPrefixTree) SplitThreshold() int { return memThreshMult * memMbar }

func (t *memPrefixTree) JoinThreshold() int { return t.SplitThreshold() / 2 }

func (t *memPrefixTree) BitQuantum() int { return memBitQuantum }

func (t *memPrefixTree) Insert(z *Zp) (err error) {
	var marray []*Zp
	for _, point := range t.Points() {
		marray = append(marray, Z(z.P).Add(z, point))
	}
	err = t.insertAtDepth(z, marray)
	if err != nil {
		return
	}
	err = t.prune()
	return
}

func (t *memPrefixTree) insertAtDepth(z *Zp, marray []*Zp) (err error) {
	prefixNode, err := t.Root()
	if err != nil {
		return
	}
	node := prefixNode.(*memPrefixNode)
	for depth := 0; ; depth++ {
		// Add to node
		node.Add(z, marray)
		if node.isLeaf {
			// Split if number of elements beyond threshold
			if len(node.Elements()) > t.SplitThreshold() {
				t.splitAtDepth(node, z, depth)
			}
			return nil
		} else {
			// Keep adding to node until leaf is reached
			cIndex := t.stringIndex(z, depth)
			prefixNode, err = t.loadChild(node, cIndex)
			if err != nil {
				return err
			}
			node = prefixNode.(*memPrefixNode)
		}
	}
	panic("unreachable")
}

func (n *memPrefixNode) Add(z *Zp, marray []*Zp) {
	if len(marray) != len(n.Points()) {
		panic("array sizes do not match")
	}
	for i := 0; i < len(marray); i++ {
		n.svalues[i] = Z(z.P).Mul(n.svalues[i], marray[i])
	}
	// TODO: if not leaf, check that element does not already exist at node
	n.elements = append(n.elements, z)
}

func (t *memPrefixTree) splitAtDepth(node PrefixNode, z *Zp, depth int) {
	if !node.IsLeaf() {
		panic("Cannot split non-leaf node")
	}
	panic("TODO")
}

func rmask(i int) int { return 0xff << uint(8-i) }

func lmask(i int) int { return 0xff >> uint(8-i) }

func (t *memPrefixTree) stringIndex(z *Zp, depth int) int {
	lowBit := depth * t.BitQuantum()
	highBit := lowBit + t.BitQuantum() - 1
	lowByte := lowBit / 8
	lowBit = lowBit % 8
	highByte := highBit / 8
	highBit = highBit % 8
	if lowByte == highByte {
		result := int(z.Bytes()[lowByte])
		return (result >> uint(7-highBit)) & lmask(highBit-lowBit+1)
	}
	b1 := int(z.Bytes()[lowByte])
	b2 := int(z.Bytes()[highByte])
	key1 := (b1 & lmask(8-lowBit)) << uint(highBit+1)
	key2 := (b2 & rmask(highBit+1)) >> uint(7-highBit)
	return key1 | key2
}

func (t *memPrefixTree) loadChild(node PrefixNode, cIndex int) (PrefixNode, error) {
	if cIndex < len(node.Children()) {
		key := node.Children()[cIndex]
		return t.Node(key)
	}
	return nil, IndexOutOfRangeErr
}

func (t *memPrefixTree) prune() error {
	panic("todo")
}

func (t *memPrefixTree) Remove(z *Zp) error { panic("todo") }

func (n *memPrefixNode) Key() *Bitstring {
	return n.key
}

func (n *memPrefixNode) IsLeaf() bool {
	return n.isLeaf
}

func (n *memPrefixNode) Elements() []*Zp {
	return n.elements
}

func (n *memPrefixNode) Children() []*Bitstring {
	children := make([]*Bitstring, 1<<uint(memBitQuantum))
	for i := 0; i < len(children); i++ {
		child := NewBitstring(n.key.BitLen() + memBitQuantum)
		child.SetBytes(n.key.Bytes())
		for j := 0; j < memBitQuantum; j++ {
			if i&(1<<uint(j)) != 0 {
				child.Set(n.key.BitLen() + j)
			} else {
				child.Unset(n.key.BitLen() + j)
			}
		}
		children[i] = child
	}
	return children
}

func (n *memPrefixNode) Size() int {
	return len(n.elements)
}

func (n *memPrefixNode) SValues() []*Zp {
	return n.svalues
}

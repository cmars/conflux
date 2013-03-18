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
	Init()
	SplitThreshold() int
	JoinThreshold() int
	BitQuantum() int
	MBar() int
	NumSamples() int
	Points() []*Zp
	Root() (PrefixNode, error)
	Node(key *Bitstring) (PrefixNode, error)
	Insert(z *Zp) error
	Delete(z *Zp) error
}

type PrefixNode interface {
	Parent() (PrefixNode, bool)
	Key() *Bitstring
	Elements() []*Zp
	Size() int
	Children() []PrefixNode
	SValues() []*Zp
	IsLeaf() bool
}

const DefaultThreshMult = 10
const DefaultBitQuantum = 2
const DefaultMBar = 5
const DefaultSplitThreshold = DefaultThreshMult * DefaultMBar
const DefaultJoinThreshold = DefaultSplitThreshold / 2
const DefaultNumSamples = DefaultMBar + 1

type MemPrefixTree struct {
	// Tree configuration options
	splitThreshold int
	joinThreshold  int
	bitQuantum     int
	mBar           int
	numSamples     int
	// Sample data points for interpolation
	points []*Zp
	// Tree's root node
	root *MemPrefixNode
}

func (t *MemPrefixTree) SplitThreshold() int { return t.splitThreshold }
func (t *MemPrefixTree) JoinThreshold() int  { return t.joinThreshold }
func (t *MemPrefixTree) BitQuantum() int     { return t.bitQuantum }
func (t *MemPrefixTree) MBar() int           { return t.mBar }
func (t *MemPrefixTree) NumSamples() int     { return t.numSamples }
func (t *MemPrefixTree) Points() []*Zp       { return t.points }
func (t *MemPrefixTree) Root() PrefixNode    { return t.root }

// Init configures the tree with default settings if not already set,
// and initializes the internal state with sample data points, root node, etc.
func (t *MemPrefixTree) Init() {
	if t.bitQuantum == 0 {
		t.bitQuantum = DefaultBitQuantum
	}
	if t.splitThreshold == 0 {
		t.splitThreshold = DefaultSplitThreshold
	}
	if t.joinThreshold == 0 {
		t.joinThreshold = DefaultJoinThreshold
	}
	if t.mBar == 0 {
		t.mBar = DefaultMBar
	}
	if t.numSamples == 0 {
		t.numSamples = DefaultNumSamples
	}
	t.points = Zpoints(P_SKS, t.numSamples)
	t.root = new(MemPrefixNode)
	t.root.init(t)
}

func (t *MemPrefixTree) Node(bs *Bitstring) (PrefixNode, error) {
	node := t.root
	for i := 0; i < bs.BitLen() && !node.IsLeaf(); i += t.BitQuantum() {
		childIndex := 0
		for j := 0; j < t.BitQuantum(); j++ {
			childIndex |= bs.Get(i) << uint(j)
		}
		if node.IsLeaf() {
			return nil, errors.New("Unexpected leaf node")
		}
		node = node.children[childIndex]
	}
	return node, nil
}

func (t *MemPrefixTree) addElementArray(z *Zp) (marray []*Zp) {
	marray = make([]*Zp, len(t.points))
	for i := 0; i < len(t.points); i++ {
		marray[i] = Z(z.P).Sub(t.points[i], z)
		if marray[i].IsZero() {
			panic("Sample point added to elements")
		}
	}
	return
}

func (t *MemPrefixTree) delElementArray(z *Zp) (marray []*Zp) {
	marray = make([]*Zp, len(t.points))
	for i := 0; i < len(t.points); i++ {
		marray[i] = Z(z.P).Sub(t.points[i], z).Inv()
	}
	return
}

// Insert a Z/Zp integer into the prefix tree
func (t *MemPrefixTree) Insert(z *Zp) error {
	bs := NewBitstring(P_SKS.BitLen())
	bs.SetBytes(ReverseBytes(z.Bytes()))
	return t.root.insert(z, t.addElementArray(z), bs, 0)
}

// Remove a Z/Zp integer from the prefix tree
func (t *MemPrefixTree) Remove(z *Zp) error {
	bs := NewBitstring(P_SKS.BitLen())
	bs.SetBytes(ReverseBytes(z.Bytes()))
	return t.root.remove(z, t.delElementArray(z), bs, 0)
}

type MemPrefixNode struct {
	// All nodes share the tree definition as a common context
	*MemPrefixTree
	// Parent of this node. Root's parent == nil
	parent *MemPrefixNode
	// Key in parent's children collection (0..(1<<bitquantum))
	key int
	// Child nodes, indexed by bitstring counting order
	// Each node will have 2**bitquantum children when leaf == false
	children []*MemPrefixNode
	// Zp elements stored at this node, if it's a leaf node
	elements []*Zp
	// Number of total elements at or below this node
	numElements int
	// Sample values at this node
	svalues []*Zp
}

func (n *MemPrefixNode) Parent() (PrefixNode, bool) { return n.parent, n.parent != nil }

func (n *MemPrefixNode) Key() *Bitstring {
	var keys []int
	for cur := n; cur != nil && cur.parent != nil; cur = cur.parent {
		keys = append(keys, cur.key)
	}
	bs := NewBitstring(len(keys) / n.BitQuantum())
	bitNum := 0
	for i := len(keys) - 1; i >= 0; i-- {
		for j := 0; j < n.BitQuantum(); j++ {
			if (keys[i]>>uint(j))&0x01 == 1 {
				bs.Set(bitNum)
			} else {
				bs.Unset(bitNum)
			}
		}
	}
	return bs
}

func (n *MemPrefixNode) Children() (result []PrefixNode) {
	for _, child := range n.children {
		result = append(result, child)
	}
	return
}
func (n *MemPrefixNode) Elements() []*Zp { return n.elements }
func (n *MemPrefixNode) Size() int       { return n.numElements }
func (n *MemPrefixNode) SValues() []*Zp  { return n.svalues }

func (n *MemPrefixNode) init(t *MemPrefixTree) {
	n.MemPrefixTree = t
	n.svalues = make([]*Zp, t.NumSamples())
	for i := 0; i < len(n.svalues); i++ {
		n.svalues[i] = Zi(P_SKS, 1)
	}
}

func (n *MemPrefixNode) IsLeaf() bool {
	return len(n.children) == 0
}

func (n *MemPrefixNode) insert(z *Zp, marray []*Zp, bs *Bitstring, depth int) error {
	n.updateSvalues(z, marray)
	n.numElements++
	if n.IsLeaf() {
		if len(n.elements) > n.SplitThreshold() {
			n.split(depth)
		} else {
			n.elements = append(n.elements, z)
			return nil
		}
	}
	child := n.nextChild(bs, depth)
	return child.insert(z, marray, bs, depth+1)
}

func (n *MemPrefixNode) split(depth int) {
	// Create child nodes
	numChildren := 1 << uint(n.BitQuantum())
	for i := 0; i < numChildren; i++ {
		child := &MemPrefixNode{parent: n}
		child.init(n.MemPrefixTree)
		n.children = append(n.children, child)
	}
	// Move elements into child nodes
	for _, element := range n.elements {
		bs := NewBitstring(P_SKS.BitLen())
		bs.SetBytes(ReverseBytes(element.Bytes()))
		child := n.nextChild(bs, depth)
		child.insert(element, n.addElementArray(element), bs, depth+1)
	}
	n.elements = nil
}

func (n *MemPrefixNode) nextChild(bs *Bitstring, depth int) *MemPrefixNode {
	childIndex := 0
	for i := 0; i < n.BitQuantum(); i++ {
		childIndex |= (bs.Get(i) << uint((depth*n.BitQuantum())+i))
	}
	return n.children[childIndex]
}

func (n *MemPrefixNode) updateSvalues(z *Zp, marray []*Zp) {
	if len(marray) != len(n.points) {
		panic("Inconsistent NumSamples size")
	}
	for i := 0; i < len(marray); i++ {
		n.svalues[i] = Z(z.P).Mul(n.svalues[i], marray[i])
	}
}

func (n *MemPrefixNode) remove(z *Zp, marray []*Zp, bs *Bitstring, depth int) error {
	n.updateSvalues(z, marray)
	n.numElements--
	if !n.IsLeaf() {
		if n.numElements <= n.JoinThreshold() {
			n.join()
		} else {
			child := n.nextChild(bs, depth)
			return child.remove(z, marray, bs, depth+1)
		}
	}
	n.elements = withRemoved(n.elements, z)
	return nil
}

func (n *MemPrefixNode) join() {
	var childNode *MemPrefixNode
	for len(n.children) > 0 {
		childNode, n.children = n.children[0], n.children[1:]
		n.elements = append(n.elements, childNode.elements...)
		n.children = append(n.children, childNode.children...)
		childNode.children = nil
	}
	n.children = nil
}

func withRemoved(elements []*Zp, z *Zp) (result []*Zp) {
	var has bool
	for _, element := range elements {
		if element.Cmp(z) != 0 {
			result = append(result, element)
		} else {
			has = true
		}
	}
	if !has {
		panic("Remove non-existent element from node")
	}
	return
}

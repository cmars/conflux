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

package recon

import (
	"errors"
	"fmt"

	cf "gopkg.in/hockeypuck/conflux.v2"
)

type PrefixTree interface {
	Init()
	Create() error
	Drop() error
	Close() error
	Points() []*cf.Zp
	Root() (PrefixNode, error)
	Node(key *cf.Bitstring) (PrefixNode, error)
	Insert(z *cf.Zp) error
	Remove(z *cf.Zp) error
}

type PrefixNode interface {
	Config() *PTreeConfig
	Parent() (PrefixNode, bool, error)
	Key() *cf.Bitstring
	Elements() ([]*cf.Zp, error)
	Size() int
	Children() ([]PrefixNode, error)
	SValues() []*cf.Zp
	IsLeaf() bool
}

func MustElements(node PrefixNode) []*cf.Zp {
	elements, err := node.Elements()
	if err != nil {
		panic(err)
	}
	return elements
}

func MustChildren(node PrefixNode) []PrefixNode {
	children, err := node.Children()
	if err != nil {
		panic(err)
	}
	return children
}

var ErrSamplePointElement = errors.New("sample point added to elements")
var ErrUnexpectedLeafNode = errors.New("unexpected leaf node")

type MemPrefixTree struct {
	PTreeConfig

	// points are the sample data points for interpolation.
	points []*cf.Zp

	// Tree's root node
	root *MemPrefixNode

	allElements *cf.ZSet
}

func (t *MemPrefixTree) Points() []*cf.Zp          { return t.points }
func (t *MemPrefixTree) Root() (PrefixNode, error) { return t.root, nil }

// Init configures the tree with default settings if not already set,
// and initializes the internal state with sample data points, root node, etc.
func (t *MemPrefixTree) Init() {
	t.PTreeConfig = defaultPTreeConfig
	t.points = cf.Zpoints(cf.P_SKS, t.NumSamples())
	t.allElements = cf.NewZSet()
	t.Create()
}

func (t *MemPrefixTree) Create() error {
	t.root = &MemPrefixNode{}
	t.root.init(t)
	return nil
}

func (t *MemPrefixTree) Drop() error {
	t.root = &MemPrefixNode{}
	t.root.init(t)
	return nil
}

func (t *MemPrefixTree) Close() error { return nil }

func Find(t PrefixTree, z *cf.Zp) (PrefixNode, error) {
	bs := cf.NewZpBitstring(z)
	return t.Node(bs)
}

func AddElementArray(t PrefixTree, z *cf.Zp) (marray []*cf.Zp, err error) {
	points := t.Points()
	marray = make([]*cf.Zp, len(points))
	for i := 0; i < len(points); i++ {
		marray[i] = cf.Z(z.P).Sub(points[i], z)
		if marray[i].IsZero() {
			err = ErrSamplePointElement
			return
		}
	}
	return
}

func DelElementArray(t PrefixTree, z *cf.Zp) (marray []*cf.Zp) {
	points := t.Points()
	marray = make([]*cf.Zp, len(points))
	for i := 0; i < len(points); i++ {
		marray[i] = cf.Z(z.P).Sub(points[i], z).Inv()
	}
	return
}

func (t *MemPrefixTree) Node(bs *cf.Bitstring) (PrefixNode, error) {
	node := t.root
	nbq := t.BitQuantum
	for i := 0; i < bs.BitLen() && !node.IsLeaf(); i += nbq {
		childIndex := 0
		for j := 0; j < nbq; j++ {
			mask := 1 << uint(j)
			if bs.Get(i+j) == 1 {
				childIndex |= mask
			}
		}
		node = node.children[childIndex]
	}
	return node, nil
}

// Insert a Z/Zp integer into the prefix tree
func (t *MemPrefixTree) Insert(z *cf.Zp) error {
	if t.allElements.Has(z) {
		return fmt.Errorf("duplicate: %q", z.String())
	}
	bs := cf.NewZpBitstring(z)
	marray, err := AddElementArray(t, z)
	if err != nil {
		return err
	}
	err = t.root.insert(z, marray, bs, 0)
	if err != nil {
		return err
	}
	t.allElements.Add(z)
	return nil
}

// Remove a Z/Zp integer from the prefix tree
func (t *MemPrefixTree) Remove(z *cf.Zp) error {
	bs := cf.NewZpBitstring(z)
	err := t.root.remove(z, DelElementArray(t, z), bs, 0)
	if err != nil {
		return err
	}
	t.allElements.Remove(z)
	return nil
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
	elements []*cf.Zp
	// Number of total elements at or below this node
	numElements int
	// Sample values at this node
	svalues []*cf.Zp
}

func (n *MemPrefixNode) Config() *PTreeConfig {
	return &n.PTreeConfig
}

func (n *MemPrefixNode) Parent() (PrefixNode, bool, error) {
	return n.parent, n.parent != nil, nil
}

func (n *MemPrefixNode) Key() *cf.Bitstring {
	var keys []int
	for cur := n; cur != nil && cur.parent != nil; cur = cur.parent {
		keys = append([]int{cur.key}, keys...)
	}
	bs := cf.NewBitstring(len(keys) * n.BitQuantum)
	for i := len(keys) - 1; i >= 0; i-- {
		for j := 0; j < n.BitQuantum; j++ {
			if ((keys[i] >> uint(j)) & 0x01) == 1 {
				bs.Set(i*n.BitQuantum + j)
			} else {
				bs.Clear(i*n.BitQuantum + j)
			}
		}
	}
	return bs
}

func (n *MemPrefixNode) Children() ([]PrefixNode, error) {
	var result []PrefixNode
	for _, child := range n.children {
		result = append(result, child)
	}
	return result, nil
}

func (n *MemPrefixNode) Elements() ([]*cf.Zp, error) {
	if n.IsLeaf() {
		return n.elements, nil
	}
	var result []*cf.Zp
	for _, child := range n.children {
		elements, err := child.Elements()
		if err != nil {
			return nil, err
		}
		result = append(result, elements...)
	}
	return result, nil
}

func (n *MemPrefixNode) Size() int         { return n.numElements }
func (n *MemPrefixNode) SValues() []*cf.Zp { return n.svalues }

func (n *MemPrefixNode) init(t *MemPrefixTree) {
	n.MemPrefixTree = t
	n.svalues = make([]*cf.Zp, t.NumSamples())
	for i := 0; i < len(n.svalues); i++ {
		n.svalues[i] = cf.Zi(cf.P_SKS, 1)
	}
}

func (n *MemPrefixNode) IsLeaf() bool {
	return len(n.children) == 0
}

func (n *MemPrefixNode) insert(z *cf.Zp, marray []*cf.Zp, bs *cf.Bitstring, depth int) error {
	n.updateSvalues(z, marray)
	n.numElements++
	if n.IsLeaf() {
		if len(n.elements) > n.SplitThreshold() {
			err := n.split(depth)
			if err != nil {
				return err
			}
		} else {
			for _, nz := range n.elements {
				if nz.Cmp(z) == 0 {
					return fmt.Errorf("duplicate: %q", z.String())
				}
			}
			n.elements = append(n.elements, z)
			return nil
		}
	}
	childIndex := NextChild(n, bs, depth)
	children, err := n.Children()
	if err != nil {
		return err
	}
	child := children[childIndex].(*MemPrefixNode)
	return child.insert(z, marray, bs, depth+1)
}

func (n *MemPrefixNode) split(depth int) error {
	// Create child nodes
	numChildren := 1 << uint(n.BitQuantum)
	for i := 0; i < numChildren; i++ {
		child := &MemPrefixNode{parent: n}
		child.key = i
		child.init(n.MemPrefixTree)
		n.children = append(n.children, child)
	}
	// Move elements into child nodes
	for _, element := range n.elements {
		bs := cf.NewZpBitstring(element)
		childIndex := NextChild(n, bs, depth)
		child := n.children[childIndex]
		marray, err := AddElementArray(n.MemPrefixTree, element)
		if err != nil {
			return err
		}
		err = child.insert(element, marray, bs, depth+1)
		if err != nil {
			return err
		}
	}
	n.elements = nil
	return nil
}

func NextChild(n PrefixNode, bs *cf.Bitstring, depth int) int {
	if n.IsLeaf() {
		panic("Cannot dereference child of leaf node")
	}
	childIndex := 0
	nbq := n.Config().BitQuantum
	for i := 0; i < nbq; i++ {
		mask := 1 << uint(i)
		if bs.Get(depth*nbq+i) == 1 {
			childIndex |= mask
		}
	}
	return childIndex
}

func (n *MemPrefixNode) updateSvalues(z *cf.Zp, marray []*cf.Zp) {
	if len(marray) != len(n.points) {
		panic("Inconsistent NumSamples size")
	}
	for i := 0; i < len(marray); i++ {
		n.svalues[i] = cf.Z(z.P).Mul(n.svalues[i], marray[i])
	}
}

func (n *MemPrefixNode) remove(z *cf.Zp, marray []*cf.Zp, bs *cf.Bitstring, depth int) error {
	n.updateSvalues(z, marray)
	n.numElements--
	if !n.IsLeaf() {
		if n.numElements <= n.JoinThreshold() {
			n.join()
		} else {
			childIndex := NextChild(n, bs, depth)
			children, err := n.Children()
			if err != nil {
				return err
			}
			child := children[childIndex].(*MemPrefixNode)
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

func withRemoved(elements []*cf.Zp, z *cf.Zp) (result []*cf.Zp) {
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

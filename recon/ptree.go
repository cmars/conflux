/*
   conflux - Distributed database synchronization library
	Based on the algorithm described in
		"Set Reconciliation with Nearly Optimal	Communication Complexity",
			Yaron Minsky, Ari Trachtenberg, and Richard Zippel, 2004.

   Copyright (C) 2012  Casey Marshall <casey.marshall@gmail.com>

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
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
}

var NodeNotFoundError error = errors.New("Node not found")

const memBitQuantum = 2
var memRootKey *Bitstring = NewBitstring(memBitQuantum)

type memPrefixTree struct {
	nodes map[string]*memPrefixNode
}

type memPrefixNode struct {
	key *Bitstring
	elements []*Zp
	isLeaf bool
	svalues []*Zp
}

func NewMemPrefixTree() PrefixTree {
	t := &memPrefixTree{}
	t.nodes[string(memRootKey.Bytes())] = &memPrefixNode{ key: memRootKey }
	return t
}

func (t *memPrefixTree) Node(key *Bitstring) (PrefixNode, error) {
	node, has := t.nodes[string(key.Bytes())]
	if has {
		return node, nil
	}
	return nil, NodeNotFoundError
}

func (t *memPrefixTree) Root() (PrefixNode, error) {
	return t.Node(memRootKey)
}

func (t *memPrefixTree) Points() []*Zp { panic("todo") }

func (t *memPrefixTree) SplitThreshold() int { panic("todo") }

func (t *memPrefixTree) JoinThreshold() int { panic("todo") }

func (t *memPrefixTree) BitQuantum() int { return memBitQuantum }

func (t *memPrefixTree) Insert(z *Zp) error { panic("todo") }

func (t *memPrefixTree) Remove(z *Zp) error { panic("todo") }

func (n *memPrefixNode) Key() *Bitstring {
	return n.key
}

func (n *memPrefixNode) Elements() []*Zp {
	return n.elements
}

func (n *memPrefixNode) Children() []*Bitstring {
	children := make([]*Bitstring, 1<<uint(memBitQuantum))
	for i := 0; i < len(children); i++ {
		child := NewBitstring(n.key.BitLen()+memBitQuantum)
		child.SetBytes(n.key.Bytes())
		for j := 0; j < memBitQuantum; j++ {
			if i & (1<<uint(j)) != 0 {
				child.Set(n.key.BitLen()+j)
			} else {
				child.Unset(n.key.BitLen()+j)
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

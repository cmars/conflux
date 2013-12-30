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

// Package leveldb provides a key-value storage implementation of the
// recon prefix tree interface.
package leveldb

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	. "github.com/cmars/conflux"
	"github.com/cmars/conflux/recon"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
)

type prefixTree struct {
	*Settings
	root   *prefixNode
	db     *leveldb.DB
	points []*Zp
}

type prefixNode struct {
	*prefixTree
	NodeKey      []byte
	NodeSValues  []byte
	NumElements  int
	Leaf         bool
	NodeElements [][]byte
}

func mustEncodeBitstring(bs *Bitstring) []byte {
	w := bytes.NewBuffer(nil)
	err := recon.WriteBitstring(w, bs)
	if err != nil {
		panic(err)
	}
	return w.Bytes()
}

func mustDecodeBitstring(buf []byte) *Bitstring {
	bs, err := recon.ReadBitstring(bytes.NewBuffer(buf))
	if err != nil {
		panic(err)
	}
	return bs
}

func mustEncodeZZarray(arr []*Zp) []byte {
	w := bytes.NewBuffer(nil)
	err := recon.WriteZZarray(w, arr)
	if err != nil {
		panic(err)
	}
	return w.Bytes()
}

func mustDecodeZZarray(buf []byte) []*Zp {
	arr, err := recon.ReadZZarray(bytes.NewBuffer(buf))
	if err != nil {
		panic(err)
	}
	return arr
}

const COLLECTION_NAME = "conflux.recon"

func New(settings *Settings) (ptree recon.PrefixTree, err error) {
	tree := &prefixTree{
		Settings: settings,
		points:   Zpoints(P_SKS, settings.NumSamples())}
	ptree = tree
	return
}

func (t *prefixTree) Create() (err error) {
	if t.db, err = leveldb.OpenFile(t.Settings.Path(), nil); err != nil {
		return
	}
	return t.ensureRoot()
}

func (t *prefixTree) Drop() error {
	if t.db != nil {
		t.db.Close()
	}
	return os.Remove(t.Settings.Path())
}

func (t *prefixTree) Close() (err error) {
	return t.db.Close()
}

func (t *prefixTree) Init() {
}

func (t *prefixTree) ensureRoot() (err error) {
	_, err = t.Root()
	if err != recon.PNodeNotFound {
		return
	}
	root := t.newChildNode(nil, 0)
	return root.upsertNode()
}

func (t *prefixTree) Points() []*Zp { return t.points }

func (t *prefixTree) Root() (recon.PrefixNode, error) {
	return t.Node(NewBitstring(0))
}

func (t *prefixTree) hasKey(key []byte) bool {
	_, err := t.db.Get(key, nil)
	return err == nil
}

func (t *prefixTree) getNode(key []byte) (node *prefixNode, err error) {
	var val []byte
	if val, err = t.db.Get(key, nil); err != nil {
		if err == leveldb.ErrNotFound {
			err = recon.PNodeNotFound
		}
		return
	}
	if len(val) == 0 {
		err = recon.PNodeNotFound
		return
	}
	dec := gob.NewDecoder(bytes.NewBuffer(val))
	node = new(prefixNode)
	err = dec.Decode(node)
	node.prefixTree = t
	return
}

func (t *prefixTree) Node(bs *Bitstring) (node recon.PrefixNode, err error) {
	nbq := t.BitQuantum()
	key := bs
	nodeKey := mustEncodeBitstring(key)
	for {
		node, err = t.getNode(nodeKey)
		if err != recon.PNodeNotFound || key.BitLen() == 0 {
			break
		}
		key = NewBitstring(key.BitLen() - nbq)
		key.SetBytes(bs.Bytes())
		nodeKey = mustEncodeBitstring(key)
	}
	return node, err
}

func (n *prefixNode) insert(z *Zp, marray []*Zp, bs *Bitstring, depth int) error {
	for {
		n.updateSvalues(z, marray)
		n.NumElements++
		var err error
		if n.IsLeaf() {
			if len(n.NodeElements) > n.SplitThreshold() {
				err = n.split(depth)
				if err != nil {
					return err
				}
			} else {
				err = n.insertElement(z)
				if err != nil {
					return err
				}
				return n.upsertNode()
			}
		}
		err = n.upsertNode()
		if err != nil {
			return err
		}
		childIndex := recon.NextChild(n, bs, depth)
		n = n.Children()[childIndex].(*prefixNode)
		depth++
	}
}

func (n *prefixNode) deleteNode() (err error) {
	err = n.db.Delete(n.NodeKey, nil)
	return
}

func (n *prefixNode) deleteElements() error {
	n.NodeElements = nil
	return n.upsertNode()
}

func (n *prefixNode) deleteElement(element *Zp) error {
	elementBytes := element.Bytes()
	var elements [][]byte
	var removed bool
	for _, element := range n.NodeElements {
		if bytes.Equal(element, elementBytes) {
			removed = true
		} else {
			elements = append(elements, element)
		}
	}
	if !removed {
		return ErrElementNotFound(element)
	}
	n.NodeElements = elements
	return n.upsertNode()
}

func (n *prefixNode) insertElement(element *Zp) error {
	n.NodeElements = append(n.NodeElements, element.Bytes())
	return n.upsertNode()
}

func (n *prefixNode) split(depth int) (err error) {
	splitElements := n.NodeElements
	n.Leaf = false
	n.NodeElements = nil
	err = n.upsertNode()
	if err != nil {
		return err
	}
	// Create child nodes
	numChildren := 1 << uint(n.BitQuantum())
	var children []*prefixNode
	for i := 0; i < numChildren; i++ {
		// Create new empty child node
		child := n.newChildNode(n, i)
		err = child.upsertNode()
		if err != nil {
			return err
		}
		children = append(children, child)
	}
	// Move elements into child nodes
	for _, element := range splitElements {
		z := Zb(P_SKS, element)
		bs := NewZpBitstring(z)
		childIndex := recon.NextChild(n, bs, depth)
		child := children[childIndex]
		marray, err := recon.AddElementArray(child, z)
		if err != nil {
			return err
		}
		err = child.insert(z, marray, bs, depth+1)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *prefixNode) remove(z *Zp, marray []*Zp, bs *Bitstring, depth int) error {
	var err error
	for {
		n.updateSvalues(z, marray)
		n.NumElements--
		if n.IsLeaf() {
			break
		} else {
			if n.NumElements <= n.JoinThreshold() {
				err = n.join()
				if err != nil {
					return err
				}
				break
			} else {
				err = n.upsertNode()
				if err != nil {
					return err
				}
				childIndex := recon.NextChild(n, bs, depth)
				n = n.Children()[childIndex].(*prefixNode)
				depth++
			}
		}
	}
	err = n.deleteElement(z)
	if err != nil {
		return err
	}
	return n.upsertNode()
}

func (n *prefixNode) join() error {
	var elements [][]byte
	for _, child := range n.Children() {
		elements = append(elements, child.(*prefixNode).NodeElements...)
		if err := child.(*prefixNode).deleteNode(); err != nil {
			return err
		}
	}
	n.NodeElements = elements
	n.Leaf = true
	return n.upsertNode()
}

func ErrDuplicateElement(z *Zp) error {
	return errors.New(fmt.Sprintf("Attempt to insert duplicate element %v", z))
}

func ErrElementNotFound(z *Zp) error {
	return errors.New(fmt.Sprintf("Expected element %v was not found", z))
}

func (t *prefixTree) Insert(z *Zp) error {
	_, lookupErr := t.db.Get(z.Bytes(), nil)
	if lookupErr == nil {
		return ErrDuplicateElement(z)
	} else if lookupErr != leveldb.ErrNotFound {
		return lookupErr
	}
	bs := NewZpBitstring(z)
	root, err := t.Root()
	if err != nil {
		return err
	}
	marray, err := recon.AddElementArray(t, z)
	if err != nil {
		return err
	}
	err = root.(*prefixNode).insert(z, marray, bs, 0)
	if err != nil {
		return err
	}
	return t.db.Put(z.Bytes(), []byte{}, nil)
}

func (t *prefixTree) Remove(z *Zp) error {
	_, lookupErr := t.db.Get(z.Bytes(), nil)
	if lookupErr != nil {
		return lookupErr
	}
	bs := NewZpBitstring(z)
	root, err := t.Root()
	if err != nil {
		return err
	}
	marray := recon.DelElementArray(t, z)
	err = root.(*prefixNode).remove(z, marray, bs, 0)
	if err != nil {
		return err
	}
	return t.db.Delete(z.Bytes(), nil)
}

func (t *prefixTree) newChildNode(parent *prefixNode, childIndex int) *prefixNode {
	n := &prefixNode{prefixTree: t, Leaf: true}
	var key *Bitstring
	if parent != nil {
		parentKey := parent.Key()
		key = NewBitstring(parentKey.BitLen() + t.BitQuantum())
		key.SetBytes(parentKey.Bytes())
		for j := 0; j < parent.BitQuantum(); j++ {
			if (1<<uint(j))&childIndex == 0 {
				key.Unset(parentKey.BitLen() + j)
			} else {
				key.Set(parentKey.BitLen() + j)
			}
		}
	} else {
		key = NewBitstring(0)
	}
	n.NodeKey = mustEncodeBitstring(key)
	svalues := make([]*Zp, t.NumSamples())
	for i := 0; i < len(svalues); i++ {
		svalues[i] = Zi(P_SKS, 1)
	}
	n.NodeSValues = mustEncodeZZarray(svalues)
	return n
}

func (n *prefixNode) upsertNode() (err error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err = enc.Encode(n); err != nil {
		return
	}
	return n.db.Put(n.NodeKey, buf.Bytes(), nil)
}

func (n *prefixNode) IsLeaf() bool {
	return n.Leaf
}

func (n *prefixNode) Children() (result []recon.PrefixNode) {
	if n.IsLeaf() {
		return nil
	}
	key := n.Key()
	numChildren := 1 << uint(n.BitQuantum())
	for i := 0; i < numChildren; i++ {
		childKey := NewBitstring(key.BitLen() + n.BitQuantum())
		childKey.SetBytes(key.Bytes())
		for j := 0; j < n.BitQuantum(); j++ {
			if (1<<uint(j))&i == 0 {
				childKey.Unset(key.BitLen() + j)
			} else {
				childKey.Set(key.BitLen() + j)
			}
		}
		child, err := n.Node(childKey)
		if err != nil {
			panic(fmt.Sprintf("Children failed on child#%v, key=%v: %v", i, childKey, err))
		}
		result = append(result, child)
		//fmt.Println("Node", n.Key(), "Child:", child.Key())
	}
	return
}

func (n *prefixNode) Elements() (result []*Zp) {
	if n.IsLeaf() {
		for _, element := range n.NodeElements {
			result = append(result, Zb(P_SKS, element))
		}
	} else {
		// TODO: Eliminate recursion
		for _, child := range n.Children() {
			result = append(result, child.Elements()...)
		}
	}
	return
}

func (n *prefixNode) Size() int { return n.NumElements }

func (n *prefixNode) SValues() []*Zp {
	return mustDecodeZZarray(n.NodeSValues)
}

func (n *prefixNode) Key() *Bitstring {
	return mustDecodeBitstring(n.NodeKey)
}

func (n *prefixNode) Parent() (recon.PrefixNode, bool) {
	key := n.Key()
	if key.BitLen() == 0 {
		return nil, false
	}
	parentKey := NewBitstring(key.BitLen() - n.BitQuantum())
	parentKey.SetBytes(key.Bytes())
	parent, err := n.Node(parentKey)
	if err != nil {
		panic(fmt.Sprintf("Failed to get parent: %v", err))
	}
	return parent, true
}

func (n *prefixNode) updateSvalues(z *Zp, marray []*Zp) {
	if len(marray) != len(n.points) {
		panic("Inconsistent NumSamples size")
	}
	svalues := mustDecodeZZarray(n.NodeSValues)
	for i := 0; i < len(marray); i++ {
		svalues[i] = Z(z.P).Mul(svalues[i], marray[i])
	}
	n.NodeSValues = mustEncodeZZarray(svalues)
}

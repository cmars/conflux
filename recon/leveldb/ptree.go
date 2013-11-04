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
	ChildKeys    []int
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

func (t *prefixTree) Node(bs *Bitstring) (recon.PrefixNode, error) {
	nodeKey := mustEncodeBitstring(bs)
	node, err := t.getNode(nodeKey)
	if err != nil {
		return nil, err
	}
	return node, err
}

type elementOperation func() (bool, error)

type changeElement struct {
	// Current node in prefix tree descent
	cur *prefixNode
	// Element to be changed (added or removed)
	element *Zp
	// Mask used to update sample values
	marray []*Zp
	// Target prefix tree key to shoot for
	target *Bitstring
	// Current depth in descent
	depth int
}

func (ch *changeElement) descend(op elementOperation) error {
	for {
		ch.cur.updateSvalues(ch.element, ch.marray)
		done, err := op()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func (ch *changeElement) insert() (done bool, err error) {
	ch.cur.NumElements++
	if ch.cur.IsLeaf() {
		if len(ch.cur.NodeElements)+1 > ch.cur.SplitThreshold() {
			err = ch.split()
			if err != nil {
				return
			}
		} else {
			err = ch.cur.upsertNode()
			if err != nil {
				return true, err
			}
			err = ch.cur.insertElement(ch.element)
			return true, err
		}
	}
	err = ch.cur.upsertNode()
	childIndex := recon.NextChild(ch.cur, ch.target, ch.depth)
	ch.cur = ch.cur.Children()[childIndex].(*prefixNode)
	ch.depth++
	return false, err
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

func (ch *changeElement) split() (err error) {
	// Create child nodes
	numChildren := 1 << uint(ch.cur.BitQuantum())
	var children []*prefixNode
	for i := 0; i < numChildren; i++ {
		// Create new empty child node
		child := ch.cur.newChildNode(ch.cur, i)
		err = child.upsertNode()
		if err != nil {
			return err
		}
		ch.cur.ChildKeys = append(ch.cur.ChildKeys, i)
		children = append(children, child)
	}
	err = ch.cur.upsertNode()
	if err != nil {
		return err
	}
	// Move elements into child nodes
	for _, element := range ch.cur.NodeElements {
		z := Zb(P_SKS, element)
		bs := NewZpBitstring(z)
		childIndex := recon.NextChild(ch.cur, bs, ch.depth)
		child := children[childIndex]
		child.NodeElements = append(child.NodeElements, element)
		marray, err := recon.AddElementArray(child, z)
		if err != nil {
			return err
		}
		ch.cur.updateSvalues(z, marray)
	}
	for _, child := range children {
		err = child.upsertNode()
		if err != nil {
			return err
		}
	}
	return
}

func (ch *changeElement) remove() (done bool, err error) {
	ch.cur.NumElements--
	if !ch.cur.IsLeaf() {
		if ch.cur.NumElements-1 <= ch.cur.JoinThreshold() {
			err = ch.join()
			if err != nil {
				return
			}
		} else {
			err = ch.cur.upsertNode()
			if err != nil {
				return
			}
			childIndex := recon.NextChild(ch.cur, ch.target, ch.depth)
			ch.cur = ch.cur.Children()[childIndex].(*prefixNode)
			ch.depth++
			return false, err
		}
	}
	if err = ch.cur.upsertNode(); err != nil {
		return
	}
	err = ch.cur.deleteElement(ch.element)
	return true, err
}

func (ch *changeElement) join() error {
	var elements [][]byte
	for _, child := range ch.cur.Children() {
		elements = append(elements, child.(*prefixNode).NodeElements...)
		if err := child.(*prefixNode).deleteNode(); err != nil {
			return err
		}
	}
	ch.cur.NodeElements = elements
	ch.cur.ChildKeys = nil
	return ch.cur.upsertNode()
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
	ch := &changeElement{
		cur:     root.(*prefixNode),
		element: z,
		marray:  marray,
		target:  bs}
	err = ch.descend(ch.insert)
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
	ch := &changeElement{
		cur:     root.(*prefixNode),
		element: z,
		marray:  recon.DelElementArray(t, z),
		target:  bs}
	err = ch.descend(ch.remove)
	if err != nil {
		return err
	}
	return t.db.Delete(z.Bytes(), nil)
}

func (t *prefixTree) newChildNode(parent *prefixNode, childIndex int) *prefixNode {
	n := &prefixNode{prefixTree: t}
	var key *Bitstring
	if parent != nil {
		parentKey := parent.Key()
		key = NewBitstring(parentKey.BitLen() + t.BitQuantum())
		key.SetBytes(parentKey.Bytes())
		for j := 0; j < parent.BitQuantum(); j++ {
			if (childIndex>>uint(j))&0x1 == 1 {
				key.Set(parentKey.BitLen() + j)
			} else {
				key.Unset(parentKey.BitLen() + j)
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
	return len(n.ChildKeys) == 0
}

func (n *prefixNode) Children() (result []recon.PrefixNode) {
	key := n.Key()
	for _, i := range n.ChildKeys {
		childKey := NewBitstring(key.BitLen() + n.BitQuantum())
		childKey.SetBytes(key.Bytes())
		for j := 0; j < n.BitQuantum(); j++ {
			if (i>>uint(j))&0x1 == 1 {
				childKey.Set(key.BitLen() + j)
			} else {
				childKey.Unset(key.BitLen() + j)
			}
		}
		child, err := n.Node(childKey)
		if err != nil {
			panic(fmt.Sprintf("Children failed on child#%v, key=%v: %v", i, childKey, err))
		}
		result = append(result, child)
	}
	return
}

func (n *prefixNode) Elements() (result []*Zp) {
	for _, element := range n.NodeElements {
		result = append(result, Zb(P_SKS, element))
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

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

// Package diskv provides a key-value storage implementation of the
// recon prefix tree interface.
package diskv

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	. "github.com/cmars/conflux"
	"github.com/cmars/conflux/recon"
	"github.com/peterbourgon/diskv"
	"io"
)

type prefixTree struct {
	*Settings
	root   *prefixNode
	dv     *diskv.Diskv
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

func balancedTransform(s string) (path []string) {
	for i, n, l := 0, 1, len(s); i+n < l && n < 8; {
		path = append(path, s[l-(i+n):l-i])
		i += n
		n *= 2
	}
	return
}

func New(settings *Settings) (ptree recon.PrefixTree, err error) {
	if settings.BasePath() == "" {
		settings.Set("conflux.recon.diskv.basePath", "conflux-ptree")
	}
	tree := &prefixTree{
		Settings: settings,
		points:   Zpoints(P_SKS, settings.NumSamples())}
	tree.dv = diskv.New(diskv.Options{
		BasePath:     settings.BasePath(),
		Transform:    balancedTransform,
		CacheSizeMax: uint64(settings.CacheSizeMax())})
	ptree = tree
	return
}

func (t *prefixTree) Create() error {
	return t.ensureRoot()
}

func (t *prefixTree) Drop() error {
	return t.dv.EraseAll()
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

func (t *prefixTree) getNode(key []byte) (node *prefixNode, err error) {
	keyStr := hex.EncodeToString(key)
	if !t.dv.Has("n" + keyStr) {
		err = recon.PNodeNotFound
		return
	}
	var r io.ReadCloser
	if r, err = t.dv.ReadStream("n" + keyStr); err != nil {
		return
	}
	defer r.Close()
	dec := gob.NewDecoder(r)
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
		if done || err != nil {
			return err
		}
	}
}

func (ch *changeElement) insert() (done bool, err error) {
	ch.cur.NumElements++
	if ch.cur.IsLeaf() {
		if len(ch.cur.NodeElements) > ch.cur.SplitThreshold() {
			err = ch.split()
			if err != nil {
				return
			}
		} else {
			ch.cur.upsertNode()
			err = ch.cur.insertElement(ch.element)
			return err == nil, err
		}
	}
	ch.cur.upsertNode()
	childIndex := recon.NextChild(ch.cur, ch.target, ch.depth)
	ch.cur = ch.cur.Children()[childIndex].(*prefixNode)
	ch.depth++
	return false, err
}

func (n *prefixNode) deleteNode() error {
	return n.dv.Erase("n" + hex.EncodeToString(n.NodeKey))
}

func (n *prefixNode) deleteElements() error {
	n.NodeElements = nil
	return n.upsertNode()
}

func (n *prefixNode) deleteElement(element *Zp) error {
	elementBytes := element.Bytes()
	var elements [][]byte
	for _, element := range n.NodeElements {
		if !bytes.Equal(element, elementBytes) {
			elements = append(elements, element)
		}
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
		child.updateSvalues(z, marray)
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
		if ch.cur.NumElements <= ch.cur.JoinThreshold() {
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
	return err == nil, err
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

func (t *prefixTree) HasElement(z *Zp) (bool, error) {
	return t.dv.Has("z" + hex.EncodeToString(z.Bytes())), nil
}

func ErrDuplicateElement(z *Zp) error {
	return errors.New(fmt.Sprintf("Attempt to insert duplicate element %v", z))
}

func (t *prefixTree) Insert(z *Zp) error {
	if has, err := t.HasElement(z); has {
		return ErrDuplicateElement(z)
	} else if err != nil {
		return err
	}
	if err := t.dv.Write("z"+hex.EncodeToString(z.Bytes()), []byte{}); err != nil {
		return err
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
	return ch.descend(ch.insert)
}

func (t *prefixTree) Remove(z *Zp) error {
	if has, err := t.HasElement(z); !has {
		return recon.PNodeNotFound
	} else if err != nil {
		return err
	}
	if err := t.dv.Erase("z" + hex.EncodeToString(z.Bytes())); err != nil {
		return err
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
	return ch.descend(ch.remove)
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
	return n.dv.WriteStream("n"+hex.EncodeToString(n.NodeKey), bytes.NewBuffer(buf.Bytes()), false)
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

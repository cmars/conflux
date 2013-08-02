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

package pqptree

import (
	"bytes"
	"database/sql"
	"encoding/ascii85"
	"fmt"
	. "github.com/cmars/conflux"
	"github.com/cmars/conflux/recon"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"text/template"
)

type PNode struct {
	NodeKey     string `db:"node_key"`
	SValues     []byte `db:"svalues"`
	NumElements int    `db:"num_elements"`
	ChildKeys   []int  `db:"child_keys"`
	elements    []PElement
}

type PElement struct {
	NodeKey string `db:"node_key"`
	Element []byte `db:"element"`
}

type pqPrefixTree struct {
	*Settings
	Namespace string
	root      *PNode
	db        *sqlx.DB
	points    []*Zp
}

type pqPrefixNode struct {
	*pqPrefixTree
	*PNode
}

func mustEncodeBitstring(bs *Bitstring) string {
	buf := bytes.NewBuffer(nil)
	w := ascii85.NewEncoder(buf)
	err := recon.WriteBitstring(w, bs)
	if err != nil {
		panic(err)
	}
	err = w.Close()
	if err != nil {
		panic(err)
	}
	return buf.String()
}

func mustDecodeBitstring(enc string) *Bitstring {
	buf := bytes.NewBufferString(enc)
	r := ascii85.NewDecoder(buf)
	bs, err := recon.ReadBitstring(r)
	if err != nil {
		panic(err)
	}
	return bs
}

func mustEncodeZZarray(arr []*Zp) []byte {
	buf := bytes.NewBuffer(nil)
	w := ascii85.NewEncoder(buf)
	err := recon.WriteZZarray(w, arr)
	if err != nil {
		panic(err)
	}
	err = w.Close()
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func mustDecodeZZarray(enc []byte) []*Zp {
	buf := bytes.NewBuffer(enc)
	r := ascii85.NewDecoder(buf)
	arr, err := recon.ReadZZarray(r)
	if err != nil {
		panic(err)
	}
	return arr
}

func New(namespace string, db *sqlx.DB, settings *Settings) (ptree recon.PrefixTree, err error) {
	tree := &pqPrefixTree{
		Settings:  settings,
		Namespace: namespace,
		db:        db,
		points:    Zpoints(P_SKS, settings.NumSamples())}
	err = tree.createTables()
	if err != nil {
		return
	}
	err = tree.ensureRoot()
	if err != nil {
		return
	}
	ptree = tree
	return
}

func (t *pqPrefixTree) SqlTemplate(sql string) string {
	result := bytes.NewBuffer(nil)
	err := template.Must(template.New("sql").Parse(sql)).Execute(result, t)
	if err != nil {
		panic(err)
	}
	return result.String()
}

func (t *pqPrefixTree) createTables() (err error) {
	if _, err = t.db.Execv(t.SqlTemplate(CreateTable_PNode)); err != nil {
		return
	}
	_, err = t.db.Execv(t.SqlTemplate(CreateTable_PElement))
	return
}

func (t *pqPrefixTree) Init() {
}

func (t *pqPrefixTree) ensureRoot() (err error) {
	_, err = t.Root()
	if err != recon.PNodeNotFound {
		return
	}
	root := t.newChildNode(nil, 0)
	tx, err := t.db.Beginx()
	if err != nil {
		return err
	}
	err = root.upsertNode(tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (t *pqPrefixTree) Points() []*Zp { return t.points }

func (t *pqPrefixTree) Root() (recon.PrefixNode, error) {
	return t.Node(NewBitstring(0))
}

func (t *pqPrefixTree) Node(bs *Bitstring) (recon.PrefixNode, error) {
	nodeKey := mustEncodeBitstring(bs)
	node := &pqPrefixNode{PNode: &PNode{}, pqPrefixTree: t}
	err := t.db.Get(node.PNode, t.SqlTemplate(
		"SELECT * FROM {{.Namespace}}_pnode WHERE node_key = $1"), nodeKey)
	if err == sql.ErrNoRows {
		return nil, recon.PNodeNotFound
	} else if err != nil {
		return nil, err
	}
	err = t.db.Select(&node.PNode.elements, t.SqlTemplate(
		"SELECT * FROM {{.Namespace}}_pelement WHERE node_key = $1"), nodeKey)
	if err == sql.ErrNoRows {
		err = nil
	}
	return node, err
}

type elementOperation func() (bool, error)

type changeElement struct {
	// Database transaction in which element change occurs
	*sqlx.Tx
	// Current node in prefix tree descent
	cur *pqPrefixNode
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
		if len(ch.cur.elements) > ch.cur.SplitThreshold() {
			err = ch.split()
			if err != nil {
				return
			}
		} else {
			ch.cur.upsertNode(ch.Tx)
			err = ch.cur.insertElement(ch.Tx, ch.element)
			return err == nil, err
		}
	}
	ch.cur.upsertNode(ch.Tx)
	ch.cur = recon.NextChild(ch.cur, ch.target, ch.depth).(*pqPrefixNode)
	ch.depth++
	return false, err
}

func (n *pqPrefixNode) deleteNode(tx *sqlx.Tx) error {
	err := n.deleteElements(tx)
	if err != nil {
		return err
	}
	_, err = tx.Execv(n.SqlTemplate(`
DELETE FROM {{.Namespace}}_pnode WHERE node_key = $1`), n.NodeKey)
	return err
}

func (n *pqPrefixNode) deleteElements(tx *sqlx.Tx) error {
	_, err := tx.Execv(n.SqlTemplate(`
DELETE FROM {{.Namespace}}_pelement WHERE node_key = $1`), n.NodeKey)
	return err
}

func (n *pqPrefixNode) deleteElement(tx *sqlx.Tx, element *Zp) error {
	_, err := tx.Execv(n.SqlTemplate(`
DELETE FROM {{.Namespace}}_pelement WHERE element = $1
RETURNING *`), element.Bytes())
	return err
}

func (n *pqPrefixNode) insertElement(tx *sqlx.Tx, element *Zp) error {
	_, err := tx.Execv(n.SqlTemplate(`
INSERT INTO {{.Namespace}}_pelement (node_key, element)
VALUES ($1, $2)`), n.NodeKey, element.Bytes())
	return err
}

func (ch *changeElement) split() (err error) {
	// Create child nodes
	numChildren := 1 << uint(ch.cur.BitQuantum())
	for i := 0; i < numChildren; i++ {
		// Create new empty child node
		ch.cur.newChildNode(ch.cur, i)
		ch.cur.ChildKeys = append(ch.cur.ChildKeys, i)
	}
	// Move elements into child nodes
	for _, element := range ch.cur.elements {
		bs := NewBitstring(P_SKS.BitLen())
		bs.SetBytes(ReverseBytes(element.Element))
		child := recon.NextChild(ch.cur, ch.target, ch.depth).(*pqPrefixNode)
		childCh := &changeElement{
			Tx:      ch.Tx,
			cur:     child,
			element: ch.element,
			marray:  ch.marray,
			target:  ch.target,
			depth:   ch.depth + 1}
		err = childCh.descend(childCh.insert)
		if err != nil {
			return err
		}
	}
	err = ch.cur.deleteElements(ch.Tx)
	return
}

func (ch *changeElement) remove() (done bool, err error) {
	ch.cur.NumElements--
	if !ch.cur.IsLeaf() {
		if ch.cur.NumElements <= ch.cur.JoinThreshold() {
			ch.join()
		} else {
			err = ch.cur.upsertNode(ch.Tx)
			if err != nil {
				return
			}
			ch.cur = recon.NextChild(ch.cur, ch.target, ch.depth).(*pqPrefixNode)
			ch.depth++
			return false, err
		}
	}
	if err = ch.cur.upsertNode(ch.Tx); err != nil {
		return
	}
	err = ch.cur.deleteElement(ch.Tx, ch.element)
	return err == nil, err
}

func (ch *changeElement) join() {
	var elements []PElement
	for _, child := range ch.cur.Children() {
		elements = append(elements, child.(*pqPrefixNode).elements...)
		child.(*pqPrefixNode).deleteNode(ch.Tx)
	}
	ch.cur.ChildKeys = nil
	ch.cur.deleteElements(ch.Tx)
	for _, element := range elements {
		ch.cur.insertElement(ch.Tx, Zb(P_SKS, element.Element))
	}
}

func (t *pqPrefixTree) Insert(z *Zp) error {
	bs := NewBitstring(P_SKS.BitLen())
	bs.SetBytes(ReverseBytes(z.Bytes()))
	root, err := t.Root()
	if err != nil {
		return err
	}
	tx, err := t.db.Beginx()
	if err != nil {
		return err
	}
	ch := &changeElement{
		Tx:      tx,
		cur:     root.(*pqPrefixNode),
		element: z,
		marray:  recon.AddElementArray(t, z),
		target:  bs}
	err = ch.descend(ch.insert)
	if err != nil {
		tx.Rollback()
		return err
	} else {
		return tx.Commit()
	}
}

func (t *pqPrefixTree) Remove(z *Zp) error {
	bs := NewBitstring(P_SKS.BitLen())
	bs.SetBytes(ReverseBytes(z.Bytes()))
	root, err := t.Root()
	if err != nil {
		return err
	}
	tx, err := t.db.Beginx()
	if err != nil {
		return err
	}
	ch := &changeElement{
		Tx:      tx,
		cur:     root.(*pqPrefixNode),
		element: z,
		marray:  recon.DelElementArray(t, z),
		target:  bs}
	err = ch.descend(ch.remove)
	if err != nil {
		tx.Rollback()
		return err
	} else {
		return tx.Commit()
	}
}

func (t *pqPrefixTree) newChildNode(parent *pqPrefixNode, childIndex int) *pqPrefixNode {
	n := &pqPrefixNode{pqPrefixTree: t, PNode: &PNode{}}
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
	n.PNode.NodeKey = mustEncodeBitstring(key)
	svalues := make([]*Zp, t.NumSamples())
	for i := 0; i < len(svalues); i++ {
		svalues[i] = Zi(P_SKS, 1)
	}
	n.PNode.SValues = mustEncodeZZarray(svalues)
	return n
}

func (n *pqPrefixNode) upsertNode(tx *sqlx.Tx) error {
	rs, err := tx.Execv(n.SqlTemplate(`
INSERT INTO {{.Namespace}}_pnode (node_key, svalues, num_elements, child_keys)
SELECT $1, $2, $3, $4 WHERE NOT EXISTS (
    SELECT 1 FROM {{.Namespace}}_pnode WHERE node_key = $1)
RETURNING *`),
		n.NodeKey, n.PNode.SValues, n.NumElements, n.ChildKeys)
	if err != nil {
		return err
	}
	nrows, err := rs.RowsAffected()
	if err != nil {
		return err
	}
	if nrows > 0 {
		_, err = tx.Execv(n.SqlTemplate(`
UPDATE {{.Namespace}}_pnode
SET svalues = $2, num_elements = $3, child_keys = $4
WHERE node_key = $1`),
			n.NodeKey, n.SValues, n.NumElements, n.ChildKeys)
	}
	return err
}

func (n *pqPrefixNode) IsLeaf() bool {
	return len(n.ChildKeys) == 0
}

func (n *pqPrefixNode) Children() (result []recon.PrefixNode) {
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
			panic(fmt.Sprintf("Children failed on child#%v: %v", i, err))
		}
		result = append(result, child)
	}
	return
}

func (n *pqPrefixNode) Elements() (result []*Zp) {
	for _, element := range n.elements {
		result = append(result, Zb(P_SKS, element.Element))
	}
	return
}

func (n *pqPrefixNode) Size() int { return n.NumElements }

func (n *pqPrefixNode) SValues() []*Zp {
	return mustDecodeZZarray(n.PNode.SValues)
}

func (n *pqPrefixNode) Key() *Bitstring {
	return mustDecodeBitstring(n.NodeKey)
}

func (n *pqPrefixNode) Parent() (recon.PrefixNode, bool) {
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

func (n *pqPrefixNode) updateSvalues(z *Zp, marray []*Zp) {
	if len(marray) != len(n.points) {
		panic("Inconsistent NumSamples size")
	}
	svalues := mustDecodeZZarray(n.PNode.SValues)
	for i := 0; i < len(marray); i++ {
		svalues[i] = Z(z.P).Mul(svalues[i], marray[i])
	}
	n.PNode.SValues = mustEncodeZZarray(svalues)
}

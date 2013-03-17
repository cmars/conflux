package recon

import (
	//"fmt"
	"testing"
	"github.com/bmizerany/assert"
	. "github.com/cmars/conflux"
)

func TestInsertNodesNoSplit(t *testing.T) {
	tree := new(PrefixTree)
	tree.Init()
	tree.Insert(Zi(P_SKS, 100))
	tree.Insert(Zi(P_SKS, 300))
	tree.Insert(Zi(P_SKS, 500))
	assert.Equal(t, 3, len(tree.Root().Elements()))
	assert.T(t, tree.Root().IsLeaf())
	tree.Remove(Zi(P_SKS, 100))
	tree.Remove(Zi(P_SKS, 300))
	tree.Remove(Zi(P_SKS, 500))
	assert.Equal(t, 0, len(tree.Root().Elements()))
	for _, sv := range tree.Root().SValues() {
		assert.Equal(t, 0, sv.Cmp(Zi(P_SKS, 1)))
	}
}

func TestJustOneKey(t *testing.T) {
	tree := new(PrefixTree)
	tree.Init()
	tree.Insert(Zs(P_SKS, "224045810486609649306292620830306652473"))
	expect := NewZSet()
	for _, sv := range []string{
			"306467079064992673198834899522272784866",
			"306467079064992673198834899522272784865",
			"306467079064992673198834899522272784867",
			"306467079064992673198834899522272784864",
			"306467079064992673198834899522272784868",
			"306467079064992673198834899522272784863" } {
		expect.Add(Zs(P_SKS, sv))
	}
	for _, sv := range tree.Root().SValues() {
		assert.T(t, expect.Has(sv))
		expect.Remove(sv)
	}
	assert.Equal(t, 0, len(expect.Items()))
}

func TestInsertNodeSplit(t *testing.T) {
	tree := new(PrefixTree)
	tree.Init()
	// Add a bunch of nodes, enough to cause splits
	for i := 0; i < tree.SplitThreshold() * 4; i++ {
		tree.Insert(Zi(P_SKS, i+65536))
	}
	// Remove a bunch of nodes, enough to cause joins
	for i := 0; i < tree.SplitThreshold() * 4; i++ {
		tree.Remove(Zi(P_SKS, i+65536))
	}
	// Insert/Remove reversible after splitting & joining?
	for _, sv := range tree.Root().SValues() {
		assert.Equal(t, 0, sv.Cmp(Zi(P_SKS, 1)))
	}
	assert.Equal(t, 0, len(tree.root.children))
	assert.Equal(t, 0, len(tree.root.elements))
}

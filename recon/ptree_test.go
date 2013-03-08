package recon

import (
	"testing"
	//"github.com/bmizerany/assert"
	. "github.com/cmars/conflux"
)

func TestInsertNodes(t *testing.T) {
	tree := NewMemPrefixTree()
	tree.Insert(Zi(P_SKS, 1))
	tree.Insert(Zi(P_SKS, 3))
	tree.Insert(Zi(P_SKS, 5))
	tree.Delete(Zi(P_SKS, 1))
	tree.Delete(Zi(P_SKS, 3))
	tree.Delete(Zi(P_SKS, 5))
}

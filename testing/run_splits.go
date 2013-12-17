package testing

import (
	"fmt"
	"testing"

	"github.com/bmizerany/assert"
	. "github.com/cmars/conflux/recon"
)

func LookupNode(key string, start PrefixNode) (PrefixNode, error) {
	node := start
	for len(key) > 0 {
		if node.IsLeaf() {
			return nil, fmt.Errorf("Unexpected leaf node")
		}
		if len(key) < node.BitQuantum() {
			return nil, fmt.Errorf("Bitstring alignment error, must be multiple of bitquantum")
		}
		childIndex := 0
		if key[0] == '1' {
			childIndex |= 0x1
		}
		if key[1] == '1' {
			childIndex |= 0x2
		}
		fmt.Println("childIndex:", childIndex)
		node = node.Children()[childIndex]
		key = key[2:]
	}
	return node, nil
}

func RunSplits15k(t *testing.T, peerMgr PeerManager) {
	peer, peerPath := peerMgr.CreatePeer()
	defer peerMgr.DestroyPeer(peer, peerPath)
	ptree := peer.PrefixTree
	var err error
	for _, z := range PtreeSplits15k {
		err = ptree.Insert(z)
		assert.Equal(t, nil, err)
	}
	root, err := ptree.Root()
	assert.Equal(t, nil, err)
	assert.Equal(t, 15000, root.Size())
	node, err := LookupNode("11", root)
	assert.Equal(t, nil, err)
	assert.Equal(t, 15000, node.Size())
	node, err = LookupNode("11011011", root)
	assert.Equal(t, nil, err)
	assert.Equal(t, 12995, node.Size())
	node, err = LookupNode("1101101011", root)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2005, node.Size())
}

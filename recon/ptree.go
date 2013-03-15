package recon

import (
	. "github.com/cmars/conflux"
)

const DefaultBitQuantum = 2
const DefaultSplitThreshold = 2
const DefaultJoinThreshold = 2
const DefaultMBar = 5
const DefaultNumSamples = DefaultMBar + 1

type PrefixTree struct {
	// Tree configuration options
	SplitThreshold int
	JoinThreshold int
	BitQuantum int
	MBar int
	NumSamples int
	// Sample data points for interpolation
	points []*Zp
	// Tree's root node
	root *PrefixNode
}

// Init configures the tree with default settings if not already set,
// and initializes the internal state with sample data points, root node, etc.
func (t *PrefixTree) Init() {
	if t.BitQuantum == 0 {
		t.BitQuantum = DefaultBitQuantum
	}
	if t.SplitThreshold == 0 {
		t.SplitThreshold = DefaultSplitThreshold
	}
	if t.JoinThreshold == 0 {
		t.JoinThreshold = DefaultJoinThreshold
	}
	if t.MBar == 0 {
		t.MBar = DefaultMBar
	}
	if t.NumSamples == 0 {
		t.NumSamples = DefaultNumSamples
	}
	t.points = Zpoints(P_SKS, t.NumSamples)
	t.root = new(PrefixNode)
	t.root.init(t)
}

func (t *PrefixTree) addElementArray(z *Zp) (marray []*Zp) {
	for _, point := range t.points {
		marray = append(marray, Z(z.P).Add(z, point))
	}
	return
}

func (t *PrefixTree) delElementArray(z *Zp) (marray []*Zp) {
	for _, point := range t.points {
		marray = append(marray, Z(z.P).Add(z, point).Inv())
	}
	return
}

// Insert a Z/Zp integer into the prefix tree
func (t *PrefixTree) Insert(z *Zp) error {
	return t.root.insert(z, t.addElementArray(z))
}

// Remove a Z/Zp integer from the prefix tree
func (t *PrefixTree) Remove(z *Zp) error {
	return t.root.remove(z, t.delElementArray(z))
}

type PrefixNode struct {
	// All nodes share the tree definition as a common context
	*PrefixTree
	// Parent of this node. Root's parent == nil
	parent *PrefixNode
	// Child nodes, indexed by bitstring counting order
	// Each node will have 2**bitquantum children when leaf == false
	children []*PrefixNode
	// Zp elements stored at this node, if it's a leaf node
	elements []*Zp
	// Sample values at this node
	svalues []*Zp
}

func (n *PrefixNode) init(t *PrefixTree) {
	n.PrefixTree = t
	n.svalues = make([]*Zp, t.NumSamples)
	for i := 0; i < t.NumSamples; i++ {
		n.svalues[i] = Zi(P_SKS, 1)
	}
}

func (n *PrefixNode) IsLeaf() bool {
	return len(n.children) == 0
}

func (n *PrefixNode) insert(z *Zp, marray []*Zp) error {
	n.updateSvalues(z, marray)
	if n.IsLeaf() {
		if len(n.elements) > n.SplitThreshold {
			n.split()
		} else {
			n.elements = append(n.elements, z)
			return nil
		}
	}
	child := n.nextChild(z)
	return child.insert(z, marray)
}

func (n *PrefixNode) split() {
	panic("TODO")
}

func (n *PrefixNode) nextChild(z *Zp) *PrefixNode {
	panic("TODO")
}

func (n *PrefixNode) updateSvalues(z *Zp, marray []*Zp) {
	if len(marray) != len(n.points) {
		panic("Inconsistent NumSamples size")
	}
	for i := 0; i < len(marray); i++ {
		n.svalues[i] = Z(z.P).Mul(n.svalues[i], marray[i])
	}
}

func (n *PrefixNode) remove(z *Zp, marray []*Zp) error {
	n.updateSvalues(z, marray)
	if !n.IsLeaf() {
		if len(n.elements) <= n.JoinThreshold {
			n.join()
		} else {
			child := n.nextChild(z)
			return child.remove(z, marray)
		}
	}
	n.elements = withRemoved(n.elements, z)
	return nil
}

func (n *PrefixNode) join() {
	panic("TODO")
}

func withRemoved(elements []*Zp, z *Zp) (result []*Zp) {
	for _, element := range elements {
		if element.Cmp(z) != 0 {
			result = append(result, element)
		}
	}
	return
}

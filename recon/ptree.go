package recon

import (
	. "github.com/cmars/conflux"
)

const DefaultThreshMult = 10
const DefaultBitQuantum = 2
const DefaultMBar = 5
const DefaultSplitThreshold = DefaultThreshMult * DefaultMBar
const DefaultJoinThreshold = DefaultSplitThreshold / 2
const DefaultNumSamples = DefaultMBar + 1

type PrefixTree struct {
	// Tree configuration options
	splitThreshold int
	joinThreshold int
	bitQuantum int
	mBar int
	numSamples int
	// Sample data points for interpolation
	points []*Zp
	// Tree's root node
	root *PrefixNode
}

func (t *PrefixTree) SplitThreshold() int { return t.splitThreshold }
func (t *PrefixTree) JoinThreshold() int { return t.joinThreshold }
func (t *PrefixTree) BitQuantum() int { return t.bitQuantum }
func (t *PrefixTree) MBar() int { return t.mBar }
func (t *PrefixTree) NumSamples() int { return t.numSamples }
func (t *PrefixTree) Points() []*Zp { return t.points }
func (t *PrefixTree) Root() *PrefixNode { return t.root }

// Init configures the tree with default settings if not already set,
// and initializes the internal state with sample data points, root node, etc.
func (t *PrefixTree) Init() {
	if t.bitQuantum == 0 {
		t.bitQuantum = DefaultBitQuantum
	}
	if t.splitThreshold == 0 {
		t.splitThreshold = DefaultSplitThreshold
	}
	if t.joinThreshold == 0 {
		t.joinThreshold = DefaultJoinThreshold
	}
	if t.mBar == 0 {
		t.mBar = DefaultMBar
	}
	if t.numSamples == 0 {
		t.numSamples = DefaultNumSamples
	}
	t.points = Zpoints(P_SKS, t.numSamples)
	t.root = new(PrefixNode)
	t.root.init(t)
}

func (t *PrefixTree) addElementArray(z *Zp) (marray []*Zp) {
	marray = make([]*Zp, len(t.points))
	for i := 0; i < len(t.points); i++ {
		marray[i] = Z(z.P).Sub(t.points[i], z)
		if marray[i].IsZero() {
			panic("Sample point added to elements")
		}
	}
	return
}

func (t *PrefixTree) delElementArray(z *Zp) (marray []*Zp) {
	marray = make([]*Zp, len(t.points))
	for i := 0; i < len(t.points); i++ {
		marray[i] = Z(z.P).Sub(t.points[i], z).Inv()
	}
	return
}

// Insert a Z/Zp integer into the prefix tree
func (t *PrefixTree) Insert(z *Zp) error {
	return t.root.insert(z, t.addElementArray(z), 0)
}

// Remove a Z/Zp integer from the prefix tree
func (t *PrefixTree) Remove(z *Zp) error {
	return t.root.remove(z, t.delElementArray(z), 0)
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

func (n *PrefixNode) Parent() *PrefixNode { return n.parent }
func (n *PrefixNode) Children() []*PrefixNode { return n.children }
func (n *PrefixNode) Elements() []*Zp { return n.elements }
func (n *PrefixNode) SValues() []*Zp { return n.svalues }

func (n *PrefixNode) init(t *PrefixTree) {
	n.PrefixTree = t
	n.svalues = make([]*Zp, t.NumSamples())
	for i := 0; i < len(n.svalues); i++ {
		n.svalues[i] = Zi(P_SKS, 1)
	}
}

func (n *PrefixNode) IsLeaf() bool {
	return len(n.children) == 0
}

func (n *PrefixNode) insert(z *Zp, marray []*Zp, depth int) error {
	n.updateSvalues(z, marray)
	if n.IsLeaf() {
		if len(n.elements) > n.SplitThreshold() {
			n.split()
		} else {
			n.elements = append(n.elements, z)
			return nil
		}
	}
	child := n.nextChild(z, depth)
	return child.insert(z, marray, depth+1)
}

func (n *PrefixNode) split() {
	for i := 0; i < n.BitQuantum(); i++ {
		child := &PrefixNode{parent:n}
		child.init(n.PrefixTree)
		n.children = append(n.children)
	}
}

func (n *PrefixNode) nextChild(z *Zp, depth int) *PrefixNode {
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

func (n *PrefixNode) remove(z *Zp, marray []*Zp, depth int) error {
	n.updateSvalues(z, marray)
	if !n.IsLeaf() {
		if len(n.elements) <= n.JoinThreshold() {
			n.join()
		} else {
			child := n.nextChild(z, depth)
			return child.remove(z, marray, depth+1)
		}
	}
	n.elements = withRemoved(n.elements, z)
	return nil
}

func (n *PrefixNode) join() {
	panic("TODO")
}

func withRemoved(elements []*Zp, z *Zp) (result []*Zp) {
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

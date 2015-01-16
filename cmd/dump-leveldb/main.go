package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/cmars/conflux"
	"github.com/cmars/conflux/recon"
	"github.com/cmars/conflux/recon/leveldb"
)

func die(err error) {
	panic(err)
}

func walk(tree recon.PrefixTree) {
	fmt.Println("[")
	var nodes []recon.PrefixNode
	root, err := tree.Root()
	if err != nil {
		die(err)
	}
	nodes = append(nodes, root)
	first := true
	for len(nodes) > 0 {
		if first {
			first = false
		} else {
			fmt.Println(",")
		}
		node := nodes[len(nodes)-1]
		nodes = nodes[:len(nodes)-1]
		visit(node)
		if !node.IsLeaf() {
			nodes = append(recon.MustChildren(node), nodes...)
		}
	}
	fmt.Println("]")
}

func visit(node recon.PrefixNode) {
	render := struct {
		SValues      []*conflux.Zp
		NumElements  int
		Key          string
		Leaf         bool
		Fingerprints []string
		Children     []string
	}{
		node.SValues(),
		node.Size(),
		node.Key().String(),
		node.IsLeaf(),
		[]string{},
		[]string{},
	}
	if node.IsLeaf() {
		for _, element := range recon.MustElements(node) {
			render.Fingerprints = append(render.Fingerprints, fmt.Sprintf("%x", element.Bytes()))
		}
	}
	for _, child := range recon.MustChildren(node) {
		render.Children = append(render.Children, child.Key().String())
	}
	out, err := json.MarshalIndent(render, "", "\t")
	if err != nil {
		die(err)
	}
	os.Stdout.Write(out)
	os.Stdout.Write([]byte("\n"))
}

func main() {
	var dbDir string
	if len(os.Args) < 2 {
		fmt.Println("Usage: <leveldb path>")
		os.Exit(1)
	}
	dbDir = os.Args[1]
	settings := recon.DefaultSettings()
	ptree, err := leveldb.New(settings.PTreeConfig, dbDir)
	if err != nil {
		die(err)
	}
	err = ptree.Create()
	if err != nil {
		die(err)
	}
	walk(ptree)
}

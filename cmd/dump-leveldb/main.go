/*
   conflux - Distributed database synchronization library
	Based on the algorithm described in
		"Set Reconciliation with Nearly Optimal	Communication Complexity",
			Yaron Minsky, Ari Trachtenberg, and Richard Zippel, 2004.

   Copyright (c) 2012-2015  Casey Marshall <cmars@cmarstech.com>

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"encoding/json"
	"fmt"
	"os"

	"gopkg.in/hockeypuck/conflux.v2"
	"gopkg.in/hockeypuck/conflux.v2/recon"
	"gopkg.in/hockeypuck/conflux.v2/recon/leveldb"
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

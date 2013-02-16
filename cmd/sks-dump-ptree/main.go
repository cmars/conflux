/*
   conflux - Distributed database synchronization library
	Based on the algorithm described in
		"Set Reconciliation with Nearly Optimal	Communication Complexity",
			Yaron Minsky, Ari Trachtenberg, and Richard Zippel, 2004.

   Copyright (C) 2012  Casey Marshall <casey.marshall@gmail.com>

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	. "github.com/cmars/conflux"
	"github.com/cmars/conflux/recon"
)

const (
	HeaderState = 0
	DataKeyState = iota
	DataValueState = iota
)

func main() {
	r := bufio.NewReader(os.Stdin)
	state := HeaderState
	//var key []byte
	//var value []byte
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			panic(err)
		}
		line = strings.TrimSpace(line)
		switch {
		case line == "HEADER=END":
			state = DataKeyState
			continue
		case state == HeaderState:
			//fmt.Printf("header: %s\n", line)
			continue
		case line == "DATA=END":
			return
		case state == DataKeyState:
			parseKey(line)
			state = DataValueState
		case state == DataValueState:
			parseValue(line)
			//printNode(key, value)
			state = DataKeyState
		}
	}
}

func parseValue(line string) []byte {
	buf, err := hex.DecodeString(line)
	if err != nil {
		panic(err)
	}
	node, err := unmarshalNode(buf, 2, 6)
	if err != nil {
		return nil
	}
	fmt.Printf("%v\n", node)
	return buf
}

func parseKey(line string) []byte {
	buf, err := hex.DecodeString(line)
	if err != nil {
		return nil
	}
	fmt.Printf("%x\n", buf)
	return buf
}

type Node struct {
	SValues []*Zp
	NumElements int
	Key *Bitstring
	Leaf bool
	Fingerprints []*Zp
	Children []*Bitstring
}

func (n *Node) String() string {
	b := bytes.NewBuffer(nil)
	fmt.Fprintf(b, "Svalues:")
	for _, sv := range n.SValues {
		fmt.Fprintf(b, " %x", sv.Bytes())
	}
	fmt.Fprintf(b, "\n")
	fmt.Fprintf(b, "Key: %v\n", n.Key)
	fmt.Fprintf(b, "Fingerprints:")
	for _, fp := range n.Fingerprints {
		fmt.Fprintf(b, " %x", fp.Bytes())
	}
	fmt.Fprintf(b, "\n")
	fmt.Fprintf(b, "Children:")
	for _, child := range n.Children {
		fmt.Fprintf(b, " %v", child)
	}
	fmt.Fprintf(b, "\n\n")
	return b.String()
}

func unmarshalNode(buf []byte, bitQuantum int, numSamples int) (node *Node, err error) {
	r := bytes.NewBuffer(buf)
	var keyBits, numElements int
	numElements, err = recon.ReadInt(r)
	if err != nil {
		return
	}
	keyBits, err = recon.ReadInt(r)
	if err != nil {
		return
	}
	keyBytes := keyBits / 8
	if keyBits % 8 > 0 {
		keyBytes++
	}
	if keyBytes <= 0 {
		err = errors.New(fmt.Sprintf("Invalid bitstring length == %d", keyBytes))
		return
	}
	keyData := make([]byte, keyBytes)
	_, err = r.Read(keyData)
	if err != nil {
		return
	}
	key := NewBitstring(keyBits)
	key.SetBytes(keyData)
	svalues := make([]*Zp, numSamples)
	for i := 0; i < numSamples; i++ {
		svalues[i], err = recon.ReadZp(r)
		if err != nil {
			return
		}
	}
	b := make([]byte, 1)
	_, err = r.Read(b)
	fmt.Printf("isleaf = %v\n", b)
	if err != nil {
		return
	}
	node = &Node{
		SValues: svalues,
		NumElements: numElements,
		Key: key,
		Leaf: b[0] == 1}
	if node.Leaf {
		var size int
		size, err = recon.ReadInt(r)
		if err != nil {
			return
		}
		node.Fingerprints = make([]*Zp, size)
		for i := 0; i < size; i++ {
			node.Fingerprints[i], _ = recon.ReadZp(r)
		}
	} else {
		for i := 0; i < 1<<uint(bitQuantum); i++ {
			child := NewBitstring(key.BitLen()+bitQuantum)
			child.SetBytes(key.Bytes())
			for j := 0; j < bitQuantum; j++ {
				if i & (1<<uint(j)) != 0 {
					child.Set(key.BitLen()+j)
				} else {
					child.Unset(key.BitLen()+j)
				}
			}
			node.Children = append(node.Children, child)
		}
	}
	return
}

/*
   conflux - Distributed database synchronization library
	Based on the algorithm described in
		"Set Reconciliation with Nearly Optimal	Communication Complexity",
			Yaron Minsky, Ari Trachtenberg, and Richard Zippel, 2004.

   Copyright (c) 2012-2015  Casey Marshall <cmars@cmarstech.com>

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

// sks-dump-ptree is a debugging utility developed to parse and
// reverse engineer the SKS PTree databases.
package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	. "gopkg.in/hockeypuck/conflux.v2"
	"gopkg.in/hockeypuck/conflux.v2/recon"
)

const (
	HeaderState    = 0
	DataKeyState   = iota
	DataValueState = iota
)

func main() {
	r := bufio.NewReader(os.Stdin)
	state := HeaderState
	first := true
	fmt.Println("[")
	for {
		line, err := r.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
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
			break
		case state == DataKeyState:
			parseKey(line)
			state = DataValueState
		case state == DataValueState:
			text := parseValue(line)
			if len(text) > 0 {
				if first {
					first = false
				} else {
					fmt.Println(",")
				}
				fmt.Print(text)
			}
			state = DataKeyState
		}
	}
	fmt.Println("]")
}

func parseValue(line string) string {
	buf, err := hex.DecodeString(line)
	if err != nil {
		panic(err)
	}
	var out bytes.Buffer
	node, err := unmarshalNode(buf, 2, 6)
	if err != nil {
		return ""
	}
	fmt.Fprintf(&out, "%v\n", node)
	return out.String()
}

func parseKey(line string) []byte {
	buf, err := hex.DecodeString(line)
	if err != nil {
		return nil
	}
	return buf
}

type Node struct {
	SValues      []*Zp
	NumElements  int
	Key          string
	Leaf         bool
	Fingerprints []string
	Children     []string
}

func (n *Node) String() string {
	var buf bytes.Buffer
	out, err := json.MarshalIndent(n, "", "\t")
	if err != nil {
		panic(err)
	}
	buf.Write(out)
	return buf.String()
}

func printHex(w io.Writer, buf []byte) {
	for i := 0; i < len(buf); i++ {
		fmt.Fprintf(w, "\\x%x", buf[i])
	}
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
	if keyBits%8 > 0 {
		keyBytes++
	}
	if keyBytes < 0 {
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
	if err != nil {
		return
	}
	node = &Node{
		SValues:     svalues,
		NumElements: numElements,
		Key:         key.String(),
		Leaf:        b[0] == 1}
	if node.Leaf {
		var size int
		size, err = recon.ReadInt(r)
		if err != nil {
			return
		}
		node.Fingerprints = make([]string, size)
		for i := 0; i < size; i++ {
			buf := make([]byte, recon.SksZpNbytes)
			_, err = io.ReadFull(r, buf)
			if err != nil {
				return
			}
			node.Fingerprints[i] = fmt.Sprintf("%x", buf)
		}
	} else {
		for i := 0; i < 1<<uint(bitQuantum); i++ {
			child := NewBitstring(key.BitLen() + bitQuantum)
			child.SetBytes(key.Bytes())
			for j := 0; j < bitQuantum; j++ {
				if i&(1<<uint(j)) != 0 {
					child.Set(key.BitLen() + j)
				} else {
					child.Clear(key.BitLen() + j)
				}
			}
			node.Children = append(node.Children, child.String())
		}
	}
	return
}

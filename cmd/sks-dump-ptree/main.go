package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
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
			fmt.Printf("header: %s\n", line)
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

/*
func printNode(key []byte, value []byte) {
	fmt.Printf("%x : %x\n", key, value)
}
*/

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
	return buf
}

type Bitstring struct {
	Buf []byte
	BitLen int
}

type Node struct {
	SValues []*Zp
	NumElements int
	Key *Bitstring
	Leaf bool
	Fingerprints []*Zp
}

func (n *Node) String() string {
	b := bytes.NewBuffer(nil)
	fmt.Fprintf(b, "Svalues:")
	for _, sv := range n.SValues {
		fmt.Fprintf(b, " %x", sv.Bytes())
	}
	fmt.Fprintf(b, "\n")
	fmt.Fprintf(b, "Key: %x\n", n.Key)
	fmt.Fprintf(b, "Fingerprints:")
	for _, fp := range n.Fingerprints {
		fmt.Fprintf(b, " %x", fp.Bytes())
	}
	fmt.Fprintf(b, "\n\n")
	return b.String()
/*
	s, err := json.Marshal(n)
	if err != nil {
		panic("do you even json?")
	}
	return string(s)
*/
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
	keyData := make([]byte, keyBytes)
	_, err = r.Read(keyData)
	if err != nil {
		return
	}
	key := &Bitstring{ Buf: keyData, BitLen: keyBits }
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
	}
	return
}

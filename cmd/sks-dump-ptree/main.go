package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
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
	fmt.Printf("%x\n", buf)
	return buf
}

type Bitstring struct {
	buf []byte
	bits int
}

func NewBitstring(bits int) *Bitstring {
	n := bits / 8
	if bits % 8 != 0 {
		n++
	}
	return &Bitstring{ buf: make([]byte, n), bits: bits }
}

func (bs *Bitstring) BitLen() int {
	return bs.bits
}

func (bs *Bitstring) ByteLen() int {
	return len(bs.buf)
}

func (bs *Bitstring) bitIndex(bit int) (int, uint) {
	return bit / 8, uint(bit % 8)
}

func (bs *Bitstring) Get(bit int) int {
	bytePos, bitPos := bs.bitIndex(bit)
	if (bs.buf[bytePos] & (byte(1)<<(8-bitPos-1))) != 0 {
		return 1
	}
	return 0
}

func (bs *Bitstring) Set(bit int) {
	bytePos, bitPos := bs.bitIndex(bit)
	bs.buf[bytePos] |= (byte(1)<<(8-bitPos-1))
}

func (bs *Bitstring) Unset(bit int) {
	bytePos, bitPos := bs.bitIndex(bit)
	bs.buf[bytePos] &^= (byte(1)<<(8-bitPos-1))
}

func (bs *Bitstring) Flip(bit int) {
	bytePos, bitPos := bs.bitIndex(bit)
	bs.buf[bytePos] ^= (byte(1)<<(8-bitPos-1))
}

func (bs *Bitstring) SetBytes(buf []byte) {
	for i := 0; i < len(bs.buf); i++ {
		if i < len(buf) {
			bs.buf[i] = buf[i]
		} else {
			bs.buf[i] = byte(0)
		}
	}
	bytePos, bitPos := bs.bitIndex(bs.bits)
	if bitPos != 0 {
		mask := ^((byte(1)<<(8-bitPos))-1)
		bs.buf[bytePos] &= mask
	}
}

func (bs *Bitstring) Lsh(n uint) {
	i := big.NewInt(int64(0)).SetBytes(bs.buf)
	i.Lsh(i, n)
	bs.SetBytes(i.Bytes())
}

func (bs *Bitstring) Rsh(n uint) {
	i := big.NewInt(int64(0)).SetBytes(bs.buf)
	i.Rsh(i, n)
	bs.SetBytes(i.Bytes())
}

func (bs *Bitstring) String() string {
	w := bytes.NewBuffer(nil)
	for i := 0; i < bs.bits; i++ {
		fmt.Fprintf(w, "%d", bs.Get(i))
	}
	return w.String()
}

func (bs *Bitstring) Bytes() []byte {
	w := bytes.NewBuffer(nil)
	w.Write(bs.buf)
	return w.Bytes()
}

/*
func (bs *Bitstring) Copy(newBits int) *Bitstring {
	newByteLen := newBits / 8
	if newBits % 8 != 0 {
		newByteLen++
	}
	newBs := &Bitstring{ buf: make([]byte, newByteLen), bits: newBits }
	for i := 0; i < len(bs.buf); i++ {
		newBs.buf[i] = bs.buf[i]
	}
	maskBits := bs.bits % 8
	if maskBits != 0 {
		mask := ^(byte(1)<<uint(maskBits+1))
		newBs.buf[len(bs.buf)-1] = bs.buf[len(bs.buf)-1] & mask
	}
	return newBs
}
*/

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
	fmt.Fprintf(b, "Key: %v\n", n.Key)
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
	}
	return
}

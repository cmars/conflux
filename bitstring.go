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

// Package conflux provides set reconciliation core functionality
// and the supporting math: polynomial arithmetic over finite fields,
// factoring and rational function interpolation.
package conflux

import (
	"bytes"
	"fmt"
	"math/big"
)

type Bitstring struct {
	buf  []byte
	bits int
}

func NewBitstring(bits int) *Bitstring {
	n := bits / 8
	if bits%8 != 0 {
		n++
	}
	return &Bitstring{buf: make([]byte, n), bits: bits}
}

func NewZpBitstring(zp *Zp) *Bitstring {
	bs := NewBitstring(zp.P.BitLen())
	bs.SetBytes(reverseBytes(zp.Bytes()))
	return bs
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
	if (bs.buf[bytePos] & (byte(1) << (8 - bitPos - 1))) != 0 {
		return 1
	}
	return 0
}

func (bs *Bitstring) Set(bit int) {
	bytePos, bitPos := bs.bitIndex(bit)
	bs.buf[bytePos] |= (byte(1) << (8 - bitPos - 1))
}

func (bs *Bitstring) Unset(bit int) {
	bytePos, bitPos := bs.bitIndex(bit)
	bs.buf[bytePos] &^= (byte(1) << (8 - bitPos - 1))
}

func (bs *Bitstring) Flip(bit int) {
	bytePos, bitPos := bs.bitIndex(bit)
	bs.buf[bytePos] ^= (byte(1) << (8 - bitPos - 1))
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
		mask := ^((byte(1) << (8 - bitPos)) - 1)
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
	if bs == nil {
		return "nil"
	}
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

func reverseBytes(buf []byte) (result []byte) {
	l := len(buf)
	result = make([]byte, l)
	for i := 0; i < l; i++ {
		result[i] = reverseByte(buf[i])
	}
	return
}

func reverseByte(b byte) (r byte) {
	for i := uint(0); i < 8; i++ {
		r |= ((b >> (7 - i)) & 1) << i
	}
	return
}

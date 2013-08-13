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

package conflux

import (
	"github.com/bmizerany/assert"
	"testing"
)

func TestBitstringSet(t *testing.T) {
	var bs *Bitstring
	// bitstring len=1
	bs = NewBitstring(1)
	assert.Equal(t, bs.String(), "0")
	bs.Flip(0)
	assert.Equal(t, bs.String(), "1")
	assert.Equal(t, bs.Bytes()[0], byte(0x80))
	// bitstring len=2
	bs = NewBitstring(2)
	assert.Equal(t, bs.String(), "00")
	bs.Flip(0)
	assert.Equal(t, bs.String(), "10")
	assert.Equal(t, bs.Bytes()[0], byte(0x80))
	bs.Flip(1)
	assert.Equal(t, bs.String(), "11")
	assert.Equal(t, bs.Bytes()[0], byte(0xc0))
	bs.Flip(0)
	assert.Equal(t, bs.String(), "01")
	assert.Equal(t, bs.Bytes()[0], byte(0x40))
	// bitstring len=16
	bs = NewBitstring(16)
	assert.Equal(t, bs.String(), "0000000000000000")
	bs.Set(0)
	bs.Set(15)
	assert.Equal(t, bs.String(), "1000000000000001")
	assert.Equal(t, bs.Bytes()[0], byte(0x80))
	assert.Equal(t, bs.Bytes()[1], byte(0x01))
}

func TestBsBytes(t *testing.T) {
	bs := NewBitstring(16)
	bs.SetBytes([]byte{0x80, 0x00})
	for i := 0; i < bs.BitLen(); i++ {
		switch i {
		case 0:
			assert.Equal(t, 1, bs.Get(i))
		default:
			assert.Equal(t, 0, bs.Get(i))
		}
	}
}

func TestReverseByte(t *testing.T) {
	assert.Equal(t, uint8(0x80), reverseByte(0x01))
	assert.Equal(t, uint8(0x18), reverseByte(0x18))
	assert.Equal(t, uint8(0x41), reverseByte(0x82))
}

func TestReverseBytes(t *testing.T) {
	assert.Equal(t, []byte{0x01}, reverseBytes([]byte{0x80}))
	assert.Equal(t, []byte{0x80}, reverseBytes([]byte{0x01}))
	assert.Equal(t, []byte{0x00, 0x00, 0x01}, reverseBytes([]byte{0x00, 0x00, 0x80}))
	assert.Equal(t, []byte{0xa5, 0xa5}, reverseBytes([]byte{0xa5, 0xa5}))
	assert.Equal(t, []byte{0x41, 0x82}, reverseBytes([]byte{0x82, 0x41}))
	assert.Equal(t, []byte{0xb7, 0xd0}, reverseBytes([]byte{0xed, 0x0b}))
}

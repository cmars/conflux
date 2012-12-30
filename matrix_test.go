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
	"math/big"
	"testing"
)

const TEST_MATRIX_SIZE = 5

func TestMatrixPutGet(t *testing.T) {
	p := big.NewInt(int64(65537))
	m := NewMatrix(TEST_MATRIX_SIZE, TEST_MATRIX_SIZE, Zi(p, 23))
	m.Set(2, 2, Zi(p, 24))
	n := 0
	for i := 0; i < TEST_MATRIX_SIZE; i++ {
		for j := 0; j < TEST_MATRIX_SIZE; j++ {
			n++
			if i == 2 && j == 2 {
				assert.Equal(t, int64(24), m.Get(i, j).Int64())
			} else {
				assert.Equal(t, int64(23), m.Get(i, j).Int64())
			}
		}
	}
	assert.Equal(t, 25, n)
}

func TestSwapRows(t *testing.T) {
	p := big.NewInt(int64(13))
	m := NewMatrix(3, 3, Zi(p, 0))
	for i := 0; i < len(m.cells); i++ {
		m.cells[i] = Zi(p, i)
	}
	m.swapRows(0, 1)
	assert.Equal(t, int64(0), m.Get(0, 1).Int64())
	assert.Equal(t, int64(3), m.Get(0, 0).Int64())
}

func TestScalarMult(t *testing.T) {
	p := big.NewInt(int64(13))
	m := NewMatrix(3, 3, Zi(p, 0))
	for col := 0; col < m.columns; col++ {
		m.Set(col, 0, Zi(p, col))
	}
	m.scmultRow(0, Zi(p, 2))
	assert.Equal(t, int64(0), m.Get(0, 0).Int64())
	assert.Equal(t, int64(2), m.Get(1, 0).Int64())
	assert.Equal(t, int64(4), m.Get(2, 0).Int64())
}

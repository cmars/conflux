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
	m.scmultRow(0, 0, Zi(p, 2))
	assert.Equal(t, int64(0), m.Get(0, 0).Int64())
	assert.Equal(t, int64(2), m.Get(1, 0).Int64())
	assert.Equal(t, int64(4), m.Get(2, 0).Int64())
}

func assertEqualMatrix(t *testing.T, m0 *Matrix, m1 *Matrix) {
	assert.Equal(t, m0.rows, m1.rows)
	assert.Equal(t, m0.columns, m1.columns)
	for i, cell := range m0.cells {
		assert.Equal(t, cell.String(), m1.cells[i].String())
	}
}

func TestRowSub(t *testing.T) {
	//rowsub scol=2 src=2 dst=1 scmult=414193719442635090920821340202167292308
	p := P_SKS
	m0 := NewMatrix(4, 3, Zi(p, 0))
	m1 := NewMatrix(4, 3, Zi(p, 0))
	m0.cells = []*Zp{ Zs(p, "1"), Zs(p, "363071340156750294202717135989109785507"), Zs(p, "0"), Zs(p, "0"),
		Zs(p, "0"), Zs(p, "1"), Zs(p, "414193719442635090920821340202167292308"), Zs(p, "76079263692028389064896590184642256185"),
		Zs(p, "0"), Zs(p, "0"), Zs(p, "1"), Zs(p, "392754565503219928741794743746565516193")}
	m1.cells = []*Zp{ Zs(p, "1"), Zs(p, "363071340156750294202717135989109785507"), Zs(p, "0"), Zs(p, "0"),
		Zs(p, "0"), Zs(p, "1"), Zs(p, "0"), Zs(p, "111558354046471781906126890183463223733"),
		Zs(p, "0"), Zs(p, "0"), Zs(p, "1"), Zs(p, "392754565503219928741794743746565516193")}
	m0.rowsub(2, 2, 1, Zs(p, "414193719442635090920821340202167292308"))
	assertEqualMatrix(t, m0, m1)
}

func TestScmult(t *testing.T) {
	//scmult_row scol=1 j=1 sc=224783190696206154659406780575841590745
	p := P_SKS
	m0 := NewMatrix(4, 3, Zi(p, 0))
	m1 := NewMatrix(4, 3, Zi(p, 0))
	m0.cells = []*Zp{
		Zs(p, "1"), Zs(p, "363071340156750294202717135989109785507"), Zs(p, "0"), Zs(p, "0"),
		Zs(p, "1"), Zs(p, "474133953638754081170510568140374743646"), Zs(p, "56378935912848241334616952212204693693"), Zs(p, "56378935912848241334616952212204693694"),
		Zs(p, "1"), Zs(p, "263058945883592178217015800993817029663"), Zs(p, "263058945883592178217015800993817029663"), Zs(p, "267453943668010144288111719358762407675")}
	m1.cells = []*Zp{
		Zs(p, "1"), Zs(p, "363071340156750294202717135989109785507"), Zs(p, "0"), Zs(p, "0"),
		Zs(p, "1"), Zs(p, "1"), Zs(p, "530512889551602322505127520352579437338"), Zs(p, "224783190696206154659406780575841590744"),
		Zs(p, "1"), Zs(p, "263058945883592178217015800993817029663"), Zs(p, "263058945883592178217015800993817029663"), Zs(p, "267453943668010144288111719358762407675")}
	m0.scmultRow(1, 1, Zs(p, "224783190696206154659406780575841590745"))
	assertEqualMatrix(t, m0, m1)
}

func TestProcessRowForward(t *testing.T) {
	p := P_SKS
	m0 := NewMatrix(4, 3, Zi(p, 0))
	m1 := NewMatrix(4, 3, Zi(p, 0))
	// matrixes captured from printfs in SKS unit tests
	m0.cells = []*Zp{
		Zs(p, "1"), Zs(p, "363071340156750294202717135989109785507"), Zs(p, "0"), Zs(p, "0"),
		Zs(p, "1"), Zs(p, "474133953638754081170510568140374743646"), Zs(p, "56378935912848241334616952212204693693"), Zs(p, "56378935912848241334616952212204693694"),
		Zs(p, "1"), Zs(p, "263058945883592178217015800993817029663"), Zs(p, "263058945883592178217015800993817029663"), Zs(p, "267453943668010144288111719358762407675")}
	m1.cells = []*Zp{
		Zs(p, "1"), Zs(p, "363071340156750294202717135989109785507"), Zs(p, "0"), Zs(p, "0"),
		Zs(p, "0"), Zs(p, "111062613482003786967793432151264958139"), Zs(p, "56378935912848241334616952212204693693"), Zs(p, "56378935912848241334616952212204693694"),
		Zs(p, "0"), Zs(p, "430500495278444206519426185357286681495"), Zs(p, "263058945883592178217015800993817029663"), Zs(p, "267453943668010144288111719358762407675")}
	m0.processRowForward(0)
	assertEqualMatrix(t, m0, m1)
}

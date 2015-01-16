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

package conflux

import (
	"bytes"
	"errors"
	"fmt"

	"gopkg.in/errgo.v1"
)

var ErrMatrixTooNarrow = errors.New("matrix is too narrow to reduce")

// Matrix represents a rectangular array of numbers over a finite field Z(p).
type Matrix struct {
	columns, rows int
	cells         []*Zp
}

// NewMatrix returns a new Matrix of the given dimensions and finite field p.
func NewMatrix(columns, rows int, x *Zp) *Matrix {
	matrix := &Matrix{
		rows:    rows,
		columns: columns,
		cells:   make([]*Zp, columns*rows)}
	for i := 0; i < len(matrix.cells); i++ {
		matrix.cells[i] = x.Copy()
	}
	return matrix
}

// Get returns the value at the given (row, column) location.
func (m *Matrix) Get(i, j int) *Zp {
	return m.cells[i+(j*m.columns)]
}

// Set sets the value at the given (row, column) location.
func (m *Matrix) Set(i, j int, x *Zp) {
	m.cells[i+(j*m.columns)] = x.Copy()
}

// Reduce performs Gaussian elimination on a matrix of coefficients, in-place.
func (m *Matrix) Reduce() error {
	if m.columns < m.rows {
		return errgo.Mask(ErrMatrixTooNarrow)
	}
	for j := 0; j < m.rows; j++ {
		m.processRowForward(j)
	}
	for j := m.rows - 1; j > 0; j-- {
		m.backSubstitute(j)
	}
	return nil
}

func (m *Matrix) backSubstitute(j int) {
	if m.Get(j, j).Int64() == int64(1) {
		last := m.rows - 1
		for j2 := j - 1; j2 >= 0; j2-- {
			scmult := m.Get(j, j2).Copy()
			m.rowsub(last, j, j2, scmult)
			m.Set(j, j2, Zi(scmult.P, 0))
		}
	}
}

func (m *Matrix) processRowForward(j int) {
	v := m.Get(j, j)
	if v.IsZero() {
		jswap := -1
		for jf := j + 1; jf < m.rows; jf++ {
			if !m.Get(j, jf).IsZero() {
				jswap = jf
				break
			}
		}
		if jswap == -1 {
			return
		}
		m.swapRows(j, jswap)
		v = m.Get(j, j)
	}
	if v.Int64() != int64(1) {
		m.scmultRow(j, j, v.Copy().Inv())
	}
	for j2 := j + 1; j2 < m.rows; j2++ {
		m.rowsub(j, j, j2, m.Get(j, j2).Copy())
	}
}

func (m *Matrix) swapRows(j1, j2 int) {
	start1 := j1 * m.columns
	start2 := j2 * m.columns
	for i := 0; i < m.columns; i++ {
		m.cells[start1+i], m.cells[start2+i] = m.cells[start2+i], m.cells[start1+i]
	}
}

func (m *Matrix) scmultRow(scol, j int, sc *Zp) {
	start := j * m.columns
	for i := scol; i < m.columns; i++ {
		v := m.cells[start+i]
		v.Mul(v, sc)
	}
}

func (m *Matrix) rowsub(scol, src, dst int, scmult *Zp) {
	for i := scol; i < m.columns; i++ {
		sval := m.Get(i, src)
		if !sval.IsZero() {
			v := m.Get(i, dst)
			if scmult.Int64() != int64(1) {
				v.Sub(v, Z(scmult.P).Mul(sval, scmult))
			} else {
				v.Sub(v, sval)
			}
		}
	}
}

// String returns a string representation of the matrix.
func (m *Matrix) String() string {
	buf := bytes.NewBuffer(nil)
	for row := 0; row < m.rows; row++ {
		fmt.Fprintf(buf, "| ")
		for col := 0; col < m.columns; col++ {
			fmt.Fprintf(buf, "%v ", m.Get(col, row))
		}
		fmt.Fprintf(buf, "|\n")
	}
	return string(buf.Bytes())
}

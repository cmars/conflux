package conflux

import (
	"errors"
)

type Matrix struct {
	columns, rows int
	cells []*Zp
}

func NewMatrix(columns, rows int, x *Zp) *Matrix {
	matrix := &Matrix{
		columns: columns,
		rows: rows,
		cells: make([]*Zp, columns * rows) }
	for i := 0; i < len(matrix.cells); i++ {
		matrix.cells[i] = x.Copy()
	}
	return matrix
}

func (m *Matrix) Get(col, row int) *Zp {
	return m.cells[col + (row * m.columns)]
}

func (m *Matrix) Set(col, row int, x *Zp) {
	m.cells[col + (row * m.columns)] = x.Copy()
}

var MatrixTooNarrow = errors.New("Matrix is too narrow to reduce")

func (m *Matrix) Reduce() (err error) {
	if m.columns < m.rows {
		return MatrixTooNarrow
	}
	for row := 0; row < m.rows; row++ {
		err = m.processRow(row)
		return
	}
	return
}

var SwapRowNotFound = errors.New("Swap row not found")

func (m *Matrix) processRow(row int) error {
	v := m.Get(row, row)
	if !v.IsZero() {
		rowSwap := -1
		for j := row + 1; j < m.rows; j++ {
			if !m.Get(row, j).IsZero() {
				rowSwap = j
			}
		}
		if rowSwap == -1 {
			return SwapRowNotFound
		}
		m.swapRows(row, rowSwap)
		v = m.Get(row, row)
	}
	if v.Int64() != int64(1) {
		m.scmultRow(row, v.Copy().Inv())
	}
	for j := 0; j < m.rows; j++ {
		if row != j {
			m.rowsub(row, j, m.Get(row, j))
		}
	}
	return nil
}

func (m *Matrix) swapRows(row1, row2 int) {
	start1 := row1 * m.columns
	start2 := row2 * m.columns
	for col := 0; col < m.columns; col++ {
		m.cells[start1 + col], m.cells[start2 + col] = m.cells[start2 + col], m.cells[start1 + col]
	}
}

func (m *Matrix) scmultRow(row int, v *Zp) {
	start := row * m.columns
	for col := 0; col < m.columns; col++ {
		z := m.cells[start + col]
		z.Mul(z, v)
	}
}

func (m *Matrix) rowsub(src, dst int, scmult *Zp) {
	for i := 0; i < m.columns; i++ {
		sval := m.Get(i, src)
		if !sval.IsZero() {
			var newval *Zp
			if scmult.Cmp(Zi(scmult.P, 1)) != 0 {
				newval = Z(scmult.P).Sub(m.Get(i, dst), Z(scmult.P).Mul(sval, scmult))
			} else {
				newval = Z(scmult.P).Sub(m.Get(i, dst), sval)
			}
			m.Set(i, dst, newval)
		}
	}
}

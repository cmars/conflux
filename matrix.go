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

func (m *Matrix) Get(i, j int) *Zp {
	return m.cells[i + (j * m.columns)]
}

func (m *Matrix) Set(i, j int, x *Zp) {
	m.cells[i + (j * m.columns)] = x.Copy()
}

var MatrixTooNarrow = errors.New("Matrix is too narrow to reduce")

func (m *Matrix) Reduce() (err error) {
	if m.columns < m.rows {
		return MatrixTooNarrow
	}
	for j := 0; j < m.rows; j++ {
		err = m.processRow(j)
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
	panic("TODO")
}

func (m *Matrix) scmultRow(row int, v *Zp) {
	panic("TODO")
}

func (m *Matrix) rowsub(row1, row2 int, v *Zp) {
	panic("TODO")
}

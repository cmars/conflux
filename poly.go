packet conflux

import (
)

type Poly struct {
	// A list of the polynomial coefficients,
	// ordered by ascending degree.
	coeff []*Zp
	// Highest degree of the polynomial
	degree int
}

func NewPoly(coeff... *Zp) *Poly {
	p := &Poly{ coeff: coeff }
	for i := len(p.coeff) - 1; i >= 0; i-- {
		if p.coeff[i] != 0 {
			p.degree = i
		}
	}
	return p
}

func (p *Poly) Div(q *Poly) *Poly {
	panic("TODO")
}

type RationalFn struct {
	Num *Poly
	Denom *Poly
}

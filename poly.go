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
	"fmt"
	"math/big"

	"gopkg.in/errgo.v1"
)

// Poly represents a polynomial in a finite field.
type Poly struct {

	// coeff is a list of the polynomial coefficients, ordered by ascending
	// degree.
	coeff []*Zp

	// degree is the highest degree of the polynomial.
	degree int

	// p defines the finite field of all coefficients.
	p *big.Int
}

// NewPoly creates a polynomial with the given coefficients, in ascending
// degree order.
//
// For example, NewPoly(1,-2,3) represents the polynomial 3x^2 - 2x + 1.
func NewPoly(coeff ...*Zp) *Poly {
	p := &Poly{}
	for i := 0; i < len(coeff); i++ {
		if coeff[i] == nil {
			if p.p == nil {
				continue
			} else {
				coeff[i] = Z(p.p)
			}
		}
		c := coeff[i].Copy().Norm()
		p.coeff = append(p.coeff, c)
		if !c.IsZero() {
			p.degree = i
		}
		// All coefficients must be in same field
		if p.p == nil {
			p.p = c.P
			// Initialize prior nils now that we know P
			for j := 0; j <= i; j++ {
				if p.coeff == nil {
					p.coeff[j] = Z(p.p)
				}
			}
		} else {
			c.assertP(p.p)
		}
	}
	return p
}

// String represents a polynomial in a more readable form,
// such as "z^2 + 2z^1 + 1".
func (p *Poly) String() string {
	result := bytes.NewBuffer(nil)
	first := true
	for i := len(p.coeff) - 1; i >= 0; i-- {
		c := p.coeff[i]
		if c.IsZero() {
			continue
		}
		if first {
			first = false
		} else {
			fmt.Fprintf(result, " + ")
		}
		fmt.Fprintf(result, "%v", c.String())
		if i > 0 {
			fmt.Fprintf(result, "z^%d", i)
		}
	}
	return string(result.Bytes())
}

// Degree returns the highest exponent that appears in the polynomial.
// For example, the degree of (x^2 + 1) is 2, the degree of (x^1) is 1.
func (p *Poly) Degree() int {
	return p.degree
}

// Coeff returns the coefficients for each term of the polynomial. Coefficients
// are represented as integers in a finite field Zp.
func (p *Poly) Coeff() []*Zp {
	return p.coeff
}

// P returns the integer P defining the finite field of the polynomial's
// coefficients.
func (p *Poly) P() *big.Int {
	return p.p
}

// Copy returns a deep copy of the polynomial and its term coefficients.
func (p *Poly) Copy() *Poly {
	newP := &Poly{degree: p.degree, p: p.p}
	for i := 0; i <= p.degree; i++ {
		newP.coeff = append(newP.coeff, p.coeff[i].Copy())
	}
	return newP
}

// assertP asserts that the polynomial's integer coefficients are in the finite
// field Z(fp).
func (p *Poly) assertP(fp *big.Int) {
	if p.p.Cmp(fp) != 0 {
		panic(fmt.Sprintf("expected finite field Z(%v), was Z(%v)", fp, p.p))
	}
}

// Equal compares with another polynomial for equality.
func (p *Poly) Equal(q *Poly) bool {
	p.assertP(q.p)
	if p.degree != q.degree {
		return false
	}
	for i := 0; i <= p.degree; i++ {
		if (p.coeff[i] == nil) != (q.coeff[i] == nil) {
			return false
		}
		if p.coeff[i] != nil && p.coeff[i].Cmp(q.coeff[i]) != 0 {
			return false
		}
	}
	return true
}

// Add sets the Poly instance to the sum of two Polys, returning the result.
func (p *Poly) Add(x, y *Poly) *Poly {
	x.assertP(y.p)
	p.p = x.p
	p.degree = x.degree
	if y.degree > p.degree {
		p.degree = y.degree
	}
	p.coeff = make([]*Zp, p.degree+1)
	for i := 0; i <= p.degree; i++ {
		p.coeff[i] = Z(x.p)
		if i <= x.degree && x.coeff[i] != nil {
			p.coeff[i].Add(p.coeff[i], x.coeff[i])
		}
		if i <= y.degree && y.coeff[i] != nil {
			p.coeff[i].Add(p.coeff[i], y.coeff[i])
		}
	}
	p.trim()
	return p
}

func (p *Poly) trim() {
	for p.degree > 0 && p.coeff[p.degree].IsZero() {
		p.degree--
	}
}

// Neg negates the Poly, returning the result.
func (p *Poly) Neg() *Poly {
	for i := 0; i <= p.degree; i++ {
		p.coeff[i].Neg()
	}
	return p
}

// Sub sets the Poly to the difference of two Polys, returning the result.
func (p *Poly) Sub(x, y *Poly) *Poly {
	return p.Add(x, y.Copy().Neg())
}

// Mul sets the Poly to the product of two Polys, returning the result.
func (p *Poly) Mul(x, y *Poly) *Poly {
	x.assertP(y.p)
	p.p = x.p
	p.coeff = make([]*Zp, x.degree+y.degree+1)
	p.degree = x.degree + y.degree
	for i := 0; i <= x.degree; i++ {
		for j := 0; j <= y.degree; j++ {
			zp := p.coeff[i+j]
			if zp == nil {
				zp = Z(p.p)
				p.coeff[i+j] = zp
			}
			zp.Add(zp, Z(p.p).Mul(x.coeff[i], y.coeff[j]))
		}
	}
	p.trim()
	return p
}

// IsConstant returns whether the Poly is just a constant value.
func (p *Poly) IsConstant(c *Zp) bool {
	return p.degree == 0 && p.coeff[0].Cmp(c) == 0
}

// Eval returns the output value of the Poly at the given sample point z.
func (p *Poly) Eval(z *Zp) *Zp {
	sum := Zi(p.p, 0)
	for d := 0; d <= p.degree; d++ {
		sum.Add(sum.Copy(), Z(p.p).Mul(p.coeff[d], Z(p.p).Exp(z, Zi(p.p, d))))
	}
	return sum
}

// PolyTerm creates a new Poly with a single non-zero coefficient.
func PolyTerm(degree int, c *Zp) *Poly {
	p := &Poly{p: c.P, degree: degree,
		coeff: make([]*Zp, degree+1)}
	for i := 0; i <= degree; i++ {
		if i == degree {
			p.coeff[i] = c.Copy()
		} else {
			p.coeff[i] = Z(p.p)
		}
	}
	return p
}

// PolyDivmod returns the quotient and remainder between two Polys.
func PolyDivmod(x, y *Poly) (q *Poly, r *Poly, err error) {
	x.assertP(y.p)
	if x.IsConstant(Zi(x.p, 0)) {
		return NewPoly(Z(x.p)), NewPoly(Z(y.p)), nil
	} else if y.degree > x.degree {
		return NewPoly(Z(x.p)), x, nil
	}
	degDiff := x.degree - y.degree
	if degDiff < 0 {
		return nil, nil, errgo.Newf("quotient degree %d < dividend %d", x.degree, y.degree)
	}
	c := Z(x.p).Div(x.coeff[x.degree], y.coeff[y.degree])
	m := PolyTerm(degDiff, c)
	my := NewPoly().Mul(m, y)
	newX := NewPoly().Sub(x, my)

	if newX.degree < x.degree || x.degree == 0 {
		// TODO: eliminate recursion
		q, r, err := PolyDivmod(newX, y)
		if err != nil {
			return nil, nil, errgo.Mask(err)
		}

		q = NewPoly().Add(q, m)
		return q, r, nil
	}
	return nil, nil, errgo.New("divmod error")
}

// PolyDiv returns the quotient between two Polys.
func PolyDiv(x, y *Poly) (*Poly, error) {
	q, _, err := PolyDivmod(x, y)
	return q, err
}

// PolyDiv returns the mod function between two Polys.
func PolyMod(x, y *Poly) (*Poly, error) {
	_, r, err := PolyDivmod(x, y)
	return r, err
}

func polyGcd(x, y *Poly) (*Poly, error) {
	if y.IsConstant(Zi(x.p, 0)) {
		return x, nil
	}
	_, r, err := PolyDivmod(x, y)
	if err != nil {
		return nil, errgo.Mask(err)
	}
	return polyGcd(y, r)
}

// PolyGcd returns the greatest common divisor between two Polys.
func PolyGcd(x, y *Poly) (*Poly, error) {
	result, err := polyGcd(x, y)
	if err != nil {
		return nil, errgo.Mask(err)
	}
	result = NewPoly().Mul(result,
		NewPoly(result.coeff[result.degree].Copy().Inv()))
	return result, nil
}

// RationalFn describes a function that is the ratio between two polynomials.
type RationalFn struct {
	Num   *Poly
	Denom *Poly
}

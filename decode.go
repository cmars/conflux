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
	"errors"
	"fmt"
	"math/big"
)

var InterpolationFailure = errors.New("Interpolation failed")

func abs(x int) int {
	if x < 0 {
		return 0 - x
	}
	return x
}

func Interpolate(values []*Zp, points []*Zp, degDiff int) (rfn *RationalFn, err error) {
	if abs(degDiff) > len(values) {
		err = InterpolationFailure
		return
	}
	p := values[0].P
	mbar := len(values)
	if (len(values)+degDiff)%2 != 0 {
		mbar = len(values) - 1
	} else {
		mbar = len(values)
	}
	ma := (mbar + degDiff) / 2
	mb := (mbar - degDiff) / 2
	matrix := NewMatrix(mbar+1, mbar, Zi(p, 0))
	for j := 0; j < mbar; j++ {
		accum := Zi(p, 1)
		kj := points[j]
		fj := values[j]
		for i := 0; i < ma; i++ {
			matrix.Set(i, j, accum)
			accum.Mul(accum, kj)
		}
		kjma := accum.Copy()
		accum = fj.Copy().Neg()
		for i := ma; i < mbar; i++ {
			matrix.Set(i, j, accum)
			accum.Mul(accum, kj)
		}
		fjkjmb := accum.Copy().Neg()
		matrix.Set(mbar, j, Z(p).Sub(fjkjmb, kjma))
	}
	err = matrix.Reduce()
	if err != nil {
		return
	}
	// Fill 'A' coefficients
	acoeffs := make([]*Zp, ma+1)
	acoeffs[ma] = Zi(p, 1)
	for j := 0; j < ma; j++ {
		acoeffs[j] = matrix.Get(mbar, j)
	}
	apoly := NewPoly(acoeffs...)
	// Fill 'B' coefficients
	bcoeffs := make([]*Zp, mb+1)
	bcoeffs[mb] = Zi(p, 1)
	for j := 0; j < mb; j++ {
		bcoeffs[j] = matrix.Get(mbar, j+ma)
	}
	bpoly := NewPoly(bcoeffs...)
	// Reduce
	g, err := PolyGcd(apoly, bpoly)
	if err != nil {
		return nil, err
	}
	rfn = &RationalFn{}
	rfn.Num, err = PolyDiv(apoly, g)
	if err != nil {
		return nil, err
	}
	rfn.Denom, err = PolyDiv(bpoly, g)
	return
}

var LowMBar error = errors.New("Low MBar")

var powModSmallN = errors.New("PowMod not implemented for small values of N")

// polyPowMod computes ``f**n`` in ``GF(p)[x]/(g)`` using repeated squaring.             
// Given polynomials ``f`` and ``g`` in ``GF(p)[x]`` and a non-negative      
// integer ``n``, efficiently computes ``f**n (mod g)`` i.e. the remainder   
// of ``f**n`` from division by ``g``, using the repeated squaring algorithm.
// This function was ported from sympy.polys.galoistools.
func polyPowMod(f *Poly, n *big.Int, g *Poly) (h *Poly, err error) {
	zero := big.NewInt(int64(0))
	one := big.NewInt(int64(1))
	n = big.NewInt(int64(0)).Set(n)
	if n.BitLen() < 3 {
		// Small values of n not useful for recon
		err = powModSmallN
		return
	}
	h = NewPoly(Zi(f.p, 1))
	for {
		if n.Bit(0) > 0 {
			h = NewPoly().Mul(h, f)
			h, err = PolyMod(h, g)
			if err != nil {
				return
			}
			n.Sub(n, one)
		}
		n.Rsh(n, 1)
		if n.Cmp(zero) == 0 {
			break
		}
		f = NewPoly().Mul(f, f)
		f, err = PolyMod(f, g)
		if err != nil {
			return
		}
	}
	return
}

// PolyRand generates a random polynomial of degree n.
// This is useful for probabilistic polynomial factoring.
func PolyRand(p *big.Int, degree int) *Poly {
	var terms []*Zp
	for i := 0; i <= degree; i++ {
		if i == degree {
			terms = append(terms, Zi(p, 1))
		} else {
			terms = append(terms, Zrand(p))
		}
	}
	return NewPoly(terms...)
}

// Factor reduces a polynomial to irreducible linear components.
// If the polynomial is not reducible to a product of linears,
// the polynomial is useless for reconciliation, resulting in an error.
// Returns a ZSet of all the constants in each linear factor.
func (p *Poly) Factor() (roots *ZSet, err error) {
	factors, err := p.factor()
	if err != nil {
		return
	}
	roots = NewZSet()
	for _, f := range factors {
		if f.degree != 1 {
			return nil, errors.New(fmt.Sprintf("Invalid factor: (%v)", f))
		}
		roots.Add(f.coeff[0])
	}
	return
}

// factor performs Cantor-Zassenhaus: Probabilistic Equal Degree Factorization
// on a complex polynomial into linear factors.
// Adapted from sympy.polys.galoistools.gf_edf_zassenhaus, specialized for
// the reconciliation cases of GF(p) and factor degree.
func (p *Poly) factor() (factors []*Poly, err error) {
	factors = append(factors, p)
	q := big.NewInt(int64(0)).Set(p.p)
	if p.degree <= 1 {
		return
	}
	for len(factors) < p.degree {
		//r := Zrand(p.p).Mod(Zi(p.p, (2*p.degree) - 1))
		r := PolyRand(p.p, 2*p.degree - 1)
		qh := big.NewInt(int64(0)).Sub(q, big.NewInt(int64(0)))
		qh.Div(qh, big.NewInt(int64(2)))
		if err != nil {
			return nil, err
		}
		h, err := polyPowMod(r, qh, p)
		if err != nil {
			return nil, err
		}
		g, err := PolyGcd(p, NewPoly().Sub(h, NewPoly(Zi(p.p, 1))))
		if err != nil {
			return nil, err
		}
		if !g.Equal(NewPoly(Zi(p.p, 1))) && !g.Equal(p) {
			qfg, err := PolyDiv(p, g)
			if err != nil {
				return nil, err
			}
			factors, err = g.factor()
			if err != nil {
				return nil, err
			}
			qfgFactors, err := qfg.factor()
			if err != nil {
				return nil, err
			}
			factors = append(factors, qfgFactors...)
		}
	}
	return
}

func factorCheck(p *Poly) bool {
	panic("TODO")
}

func Reconcile(values []*Zp, points []*Zp, degDiff int) (*ZSet, *ZSet, error) {
	rfn, err := Interpolate(
		values[:len(values)-1], points[:len(points)-1], degDiff)
	if err != nil {
		return nil, nil, err
	}
	lastPoint := points[len(points)-1]
	valFromPoly := Z(lastPoint.P).Div(
		rfn.Num.Eval(lastPoint), rfn.Denom.Eval(lastPoint))
	lastValue := values[len(values)-1]
	if valFromPoly.Cmp(lastValue) != 0 ||
		!factorCheck(rfn.Num) || !factorCheck(rfn.Denom) {
		return nil, nil, LowMBar
	}
	numF, err := rfn.Num.Factor()
	if err != nil {
		return nil, nil, err
	}
	denomF, err := rfn.Denom.Factor()
	if err != nil {
		return nil, nil, err
	}
	return numF, denomF, nil
}

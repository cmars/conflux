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
	"errors"
	"math/big"

	"gopkg.in/errgo.v1"
)

var ErrInterpolate = errors.New("interpolation failed")

var ErrLowMBar = errors.New("low MBar")

var ErrPowModSmallN = errors.New("PowMod not implemented for small values of N")

func IsInterpolateFailure(err error) bool {
	switch err {
	case ErrInterpolate:
		return true
	case ErrLowMBar:
		return true
	}
	return false
}

func abs(x int) int {
	if x < 0 {
		return 0 - x
	}
	return x
}

// Interpolate returns the ratio of two polynomials RationalFn, given a set of
// sample points and output values. The coefficients of the resulting numerator
// and denominator represent the disjoint members in two sets being reconciled.
func Interpolate(values []*Zp, points []*Zp, degDiff int) (*RationalFn, error) {
	if abs(degDiff) > len(values) {
		return nil, errgo.Mask(ErrInterpolate, IsInterpolateFailure)
	}
	p := values[0].P
	mbar := len(values)
	if (mbar+degDiff)%2 != 0 {
		mbar--
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
			accum = Z(p).Mul(accum, kj)
		}
		kjma := accum.Copy()
		accum = fj.Copy().Neg()
		for i := ma; i < mbar; i++ {
			matrix.Set(i, j, accum)
			accum = Z(p).Mul(accum, kj)
		}
		fjkjmb := accum.Copy().Neg()
		matrix.Set(mbar, j, Z(p).Sub(fjkjmb, kjma))
	}
	err := matrix.Reduce()
	if err != nil {
		return nil, errgo.Mask(err)
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
		return nil, errgo.Mask(err)
	}
	rfn := &RationalFn{}
	rfn.Num, err = PolyDiv(apoly, g)
	if err != nil {
		return nil, errgo.Mask(err)
	}
	rfn.Denom, err = PolyDiv(bpoly, g)
	if err != nil {
		return nil, errgo.Mask(err)
	}
	return rfn, nil
}

// polyPowMod computes ``f**n`` in ``GF(p)[x]/(g)`` using repeated squaring.
// Given polynomials ``f`` and ``g`` in ``GF(p)[x]`` and a non-negative
// integer ``n``, efficiently computes ``f**n (mod g)`` i.e. the remainder
// of ``f**n`` from division by ``g``, using the repeated squaring algorithm.
// This function was ported from sympy.polys.galoistools.
func polyPowMod(f *Poly, n *big.Int, g *Poly) (*Poly, error) {
	zero := big.NewInt(int64(0))
	one := big.NewInt(int64(1))
	n = big.NewInt(int64(0)).Set(n)
	if n.BitLen() < 3 {
		// Small values of n not useful for recon
		return nil, errgo.Mask(ErrPowModSmallN)
	}

	var err error
	h := NewPoly(Zi(f.p, 1))
	for {
		if n.Bit(0) > 0 {
			h = NewPoly().Mul(h, f)
			h, err = PolyMod(h, g)
			if err != nil {
				return nil, errgo.Mask(err)
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
			return nil, errgo.Mask(err)
		}
	}
	return h, nil
}

// PolyRand generates a random polynomial of degree n. This is useful for
// probabilistic polynomial factoring.
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

// Factor reduces a polynomial to irreducible linear components. If the
// polynomial is not reducible to a product of linears, the polynomial is
// useless for reconciliation, resulting in an error. Returns a ZSet of all the
// constants in each linear factor.
func (p *Poly) Factor() (*ZSet, error) {
	factors, err := p.factor()
	if err != nil {
		return nil, errgo.Mask(err)
	}
	roots := NewZSet()
	one := Zi(p.p, 1)
	for _, f := range factors {
		if f.degree == 0 && f.coeff[0].Cmp(one) == 0 {
			continue
		}
		if f.degree != 1 {
			return nil, errgo.Newf("invalid factor: (%v)", f)
		}
		roots.Add(f.coeff[0].Copy().Neg())
	}
	return roots, nil
}

// factor performs Cantor-Zassenhaus: Probabilistic Equal Degree Factorization
// on a complex polynomial into linear factors.
//
// Adapted from sympy.polys.galoistools.gf_edf_zassenhaus, specialized for
// the reconciliation cases of GF(p) and factor degree.
func (p *Poly) factor() ([]*Poly, error) {
	factors := []*Poly{p}
	q := big.NewInt(int64(0)).Set(p.p)
	if p.degree <= 1 {
		return factors, nil
	}
	for len(factors) < p.degree {
		r := PolyRand(p.p, 2*p.degree-1)
		qh := big.NewInt(int64(0))
		qh.Sub(q, qh)
		qh.Div(qh, big.NewInt(int64(2)))

		h, err := polyPowMod(r, qh, p)
		if err != nil {
			return nil, errgo.Mask(err)
		}
		g, err := PolyGcd(p, NewPoly().Sub(h, NewPoly(Zi(p.p, 1))))
		if err != nil {
			return nil, errgo.Mask(err)
		}
		if !g.Equal(NewPoly(Zi(p.p, 1))) && !g.Equal(p) {
			qfg, err := PolyDiv(p, g)
			if err != nil {
				return nil, errgo.Mask(err)
			}
			factors, err = g.factor()
			if err != nil {
				return nil, errgo.Mask(err)
			}
			qfgFactors, err := qfg.factor()
			if err != nil {
				return nil, errgo.Mask(err)
			}
			factors = append(factors, qfgFactors...)
		}
	}
	return factors, nil
}

func factorCheck(p *Poly) bool {
	if p.degree <= 1 {
		return true
	}
	z := NewPoly(Zi(p.p, 0), Zi(p.p, 1))
	zq, err := polyPowMod(z, P_SKS, p)
	if err != nil {
		return false
	}
	for i := 0; i <= z.degree; i++ {
		z.coeff[i] = Z(p.p).Mul(z.coeff[i], Zi(p.p, -1))
	}
	zqmz, err := PolyMod(NewPoly().Add(zq, z), p)
	if err != nil {
		return false
	}
	return zqmz.degree == 0 || (zqmz.degree == 1 && zqmz.coeff[0].IsZero())
}

// Generate points for rational function interpolation.
func Zpoints(p *big.Int, n int) []*Zp {
	points := make([]*Zp, n)
	for i := 0; i < n; i++ {
		var pi int
		if i%2 == 0 {
			pi = ((i + 1) / 2) * 1
		} else {
			pi = ((i + 1) / 2) * -1
		}
		points[i] = Zi(p, pi)
	}
	return points
}

// Reconcile performs rational function interpolation on the given output
// values at sample points, to return the disjoint values between two sets.
func Reconcile(values []*Zp, points []*Zp, degDiff int) (*ZSet, *ZSet, error) {
	rfn, err := Interpolate(
		values[:len(values)-1], points[:len(points)-1], degDiff)
	if err != nil {
		return nil, nil, errgo.Mask(err)
	}
	lastPoint := points[len(points)-1]
	valFromPoly := Z(lastPoint.P).Div(
		rfn.Num.Eval(lastPoint), rfn.Denom.Eval(lastPoint))
	lastValue := values[len(values)-1]
	if valFromPoly.Cmp(lastValue) != 0 ||
		!factorCheck(rfn.Num) || !factorCheck(rfn.Denom) {
		return nil, nil, errgo.Mask(ErrLowMBar, IsInterpolateFailure)
	}
	numF, err := rfn.Num.Factor()
	if err != nil {
		return nil, nil, errgo.Mask(err)
	}
	denomF, err := rfn.Denom.Factor()
	if err != nil {
		return nil, nil, errgo.Mask(err)
	}
	return numF, denomF, nil
}

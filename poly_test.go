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
	"math/big"

	gc "gopkg.in/check.v1"
)

type PolySuite struct{}

var _ = gc.Suite(&PolySuite{})

func (s *PolySuite) TestPolyDegree(c *gc.C) {
	p := big.NewInt(int64(65537))
	poly := NewPoly(Zi(p, 4), Zi(p, 3), Zi(p, 2))
	c.Assert(2, gc.Equals, poly.Degree())
	poly = NewPoly(nil, nil, nil, nil, nil, nil, nil, nil)
	c.Assert(0, gc.Equals, poly.Degree())
	poly = NewPoly(nil, nil, nil, nil, nil, nil, nil, Zi(p, 1))
	c.Assert(7, gc.Equals, poly.Degree())
	poly = NewPoly(nil, nil, nil, nil, nil, nil, nil, Zi(p, 1), nil, nil)
	c.Assert(7, gc.Equals, poly.Degree())
}

func (s *PolySuite) TestPolyFmt(c *gc.C) {
	p := big.NewInt(int64(65537))
	poly := NewPoly(Zi(p, 4), Zi(p, 3), Zi(p, 2))
	c.Assert("2z^2 + 3z^1 + 4", gc.Equals, poly.String())
}

func (s *PolySuite) TestPolyEval(c *gc.C) {
	var poly *Poly
	var z *Zp
	p := big.NewInt(int64(97))
	// Constant
	poly = NewPoly(Zi(p, 5))
	z = poly.Eval(Zi(p, 8))
	c.Assert(int64(5), gc.Equals, z.Int64())
	// Linear
	poly = NewPoly(Zi(p, 5), Zi(p, 3))
	z = poly.Eval(Zi(p, 8))
	c.Assert(int64(29), gc.Equals, z.Int64())
	// Quadratic
	poly = NewPoly(Zi(p, 5), Zi(p, 3), Zi(p, 2))
	z = poly.Eval(Zi(p, 8))
	c.Assert(Zi(p, 157).Int64(), gc.Equals, z.Int64())
}

func (s *PolySuite) TestPolyMul(c *gc.C) {
	p := big.NewInt(int64(97))
	x := NewPoly(Zi(p, -6), Zi(p, 11), Zi(p, -6), Zi(p, 1))
	y := NewPoly(Zi(p, 2), Zi(p, 1))
	z := NewPoly().Mul(x, y)
	c.Assert(5, gc.Equals, len(z.coeff))
	c.Logf("z=%v", z)
	for i, v := range []int{85, 16, 96, 93, 1} {
		c.Assert(Zi(p, v).String(), gc.Equals, z.coeff[i].String())
	}
}

func (s *PolySuite) TestPolyAdd(c *gc.C) {
	p := big.NewInt(int64(97))
	// (x+1) + (x+2) = (2x+3)
	x := NewPoly(Zi(p, 1), Zi(p, 1))
	y := NewPoly(Zi(p, 2), Zi(p, 1))
	z := NewPoly().Add(x, y)
	c.Assert(1, gc.Equals, z.degree)
	c.Assert(int64(3), gc.Equals, z.coeff[0].Int64())
	c.Assert(int64(2), gc.Equals, z.coeff[1].Int64())
	// (2x+3) - (x+2) = (x+1)
	x = NewPoly(Zi(p, 3), Zi(p, 2))
	y = NewPoly(Zi(p, 2), Zi(p, 1))
	z = NewPoly().Sub(x, y)
	c.Assert(1, gc.Equals, z.degree)
	c.Assert(int64(1), gc.Equals, z.coeff[0].Int64())
	c.Assert(int64(1), gc.Equals, z.coeff[1].Int64())
	// (x+1) - (x^2+2x+1) = (-x^2 - x)
	x = NewPoly(Zi(p, 1), Zi(p, 1))
	y = NewPoly(Zi(p, 1), Zi(p, 2), Zi(p, 1))
	z = NewPoly().Sub(x, y)
	c.Assert(2, gc.Equals, z.degree)
	c.Assert(int64(0), gc.Equals, z.coeff[0].Int64())
	c.Assert(Zi(p, -1).Int64(), gc.Equals, z.coeff[1].Int64())
	c.Assert(Zi(p, -1).Int64(), gc.Equals, z.coeff[2].Int64())
}

func (s *PolySuite) TestPolyDivmod(c *gc.C) {
	// (x^2 + 2x + 1) / (x + 1) = (x + 1)
	p := big.NewInt(int64(97))
	x := NewPoly(Zi(p, 1), Zi(p, 2), Zi(p, 1))
	y := NewPoly(Zi(p, 1), Zi(p, 1))
	q, r, err := PolyDivmod(x, y)
	c.Logf("q=(%v) r=(%v) err=(%v)", q, r, err)
	c.Assert(1, gc.Equals, q.degree)
	c.Assert(int64(1), gc.Equals, q.coeff[0].Int64())
	c.Assert(int64(1), gc.Equals, q.coeff[1].Int64())
	c.Assert(2, gc.Equals, len(q.coeff))
	c.Assert(err, gc.IsNil, gc.Commentf("%v", err))
	c.Assert(0, gc.Equals, r.degree)
	c.Assert(err, gc.IsNil)
}

func (s *PolySuite) TestGcd(c *gc.C) {
	p := big.NewInt(int64(97))
	x := NewPoly(Zi(p, 1), Zi(p, 2), Zi(p, 1))
	y := NewPoly(Zi(p, 1), Zi(p, 1))
	r, err := PolyGcd(x, y)
	c.Assert(err, gc.IsNil)
	c.Logf("r=(%v)", r)
	c.Assert(1, gc.Equals, r.degree)
	c.Assert(int64(1), gc.Equals, r.coeff[0].Int64())
	c.Assert(int64(1), gc.Equals, r.coeff[1].Int64())
	c.Assert(2, gc.Equals, len(r.coeff))
}

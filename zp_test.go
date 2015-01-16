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

type ZpSuite struct{}

var _ = gc.Suite(&ZpSuite{})

func p(n int) *big.Int {
	return big.NewInt(int64(n))
}

func zp5(n int) *Zp {
	return Zi(p(5), n)
}

func zp7(n int) *Zp {
	return Zi(p(7), n)
}

func (s *ZpSuite) TestAdd(c *gc.C) {
	a := zp5(1)
	b := zp5(3)
	c.Assert(0, gc.Equals, zp5(4).Cmp(a.Add(a, b)))
}

func (s *ZpSuite) TestAddWrap(c *gc.C) {
	a := zp5(1)
	b := zp5(9)
	c.Assert(0, gc.Equals, zp5(0).Cmp(a.Add(a, b)))
}

func (s *ZpSuite) TestMinusOne(c *gc.C) {
	a := Zi(p(65537), -1)
	c.Assert(int64(65536), gc.Equals, a.Int64())
}

func (s *ZpSuite) TestMul(c *gc.C) {
	// 4x3
	a := zp5(4)
	b := zp5(3)
	a.Mul(a, b)
	c.Assert(int64(2), gc.Equals, a.Int64())
	// 4x4x3
	a = zp5(4)
	b = zp5(3)
	a.Mul(a, a)
	a.Mul(a, b)
	c.Assert(int64(3), gc.Equals, a.Int64())
	// 16x16
	a = zp5(4)
	a.Mul(a, a) // 4x4
	a.Mul(a, a) // 16x16
	c.Assert(int64(1), gc.Equals, a.Int64())
}

func (s *ZpSuite) TestDiv(c *gc.C) {
	// in Z(5), 1 / 2 = 3 because 3 * 2 = 1.
	a := zp5(1)
	b := zp5(2)
	q := Z(p(5)).Div(a, b)
	c.Assert(int64(3), gc.Equals, q.Int64())
	// in Z(5), 1 / 3 = 2 because 3 * 2 = 1.
	a = zp5(1)
	b = zp5(3)
	q = Z(p(5)).Div(a, b)
	c.Assert(int64(2), gc.Equals, q.Int64())
}

func (s *ZpSuite) TestMismatchedP(c *gc.C) {
	defer func() {
		r := recover()
		c.Assert(r, gc.NotNil)
	}()
	a := zp5(1)
	b := Zi(p(65537), 9)
	a.Add(a, b)
	c.Fail()
}

func (s *ZpSuite) TestNeg(c *gc.C) {
	a := zp5(2)
	a.Neg()
	c.Assert(int64(3), gc.Equals, a.Int64())
	a = zp5(0)
	a.Neg()
	c.Assert(int64(0), gc.Equals, a.Int64())
}

func (s *ZpSuite) TestSub(c *gc.C) {
	az := zp5(4)
	bz := zp5(3)
	cz := az.Copy().Sub(az, bz)
	c.Assert(int64(4), gc.Equals, az.Int64())
	c.Assert(int64(3), gc.Equals, bz.Int64())
	c.Assert(int64(1), gc.Equals, cz.Int64())
}

func (s *ZpSuite) TestSubRoll(c *gc.C) {
	az := zp5(1)
	bz := zp5(3)
	cz := az.Copy().Sub(az, bz)
	c.Assert(int64(1), gc.Equals, az.Int64())
	c.Assert(int64(3), gc.Equals, bz.Int64())
	c.Assert(int64(3), gc.Equals, cz.Int64()) // -2 == 3
	az = zp5(1)
	bz = zp5(4)
	cz = az.Copy().Sub(az, bz)
	c.Assert(int64(1), gc.Equals, az.Int64())
	c.Assert(int64(4), gc.Equals, bz.Int64())
	c.Assert(int64(2), gc.Equals, cz.Int64()) // -3 == 2
}

func (s *ZpSuite) TestZSet(c *gc.C) {
	a := NewZSet()
	a.Add(zp5(1))
	a.Add(zp5(1))
	a.Add(zp5(2))
	a.Add(zp5(3))
	items := a.Items()
	c.Assert(items, gc.HasLen, 3)
	c.Assert(a.Has(zp5(1)), gc.Equals, true)
	c.Assert(a.Has(zp5(2)), gc.Equals, true)
	c.Assert(a.Has(zp5(3)), gc.Equals, true)
}

func (s *ZpSuite) TestZsetDisjoint(c *gc.C) {
	zs1 := NewZSet(Zi(P_SKS, 65537), Zi(P_SKS, 65539))
	zs2 := NewZSet(Zi(P_SKS, 65537), Zi(P_SKS, 65541))
	c.Assert(zs1.Has(Zi(P_SKS, 65537)), gc.Equals, true)
	c.Assert(zs2.Has(Zi(P_SKS, 65537)), gc.Equals, true)
	c.Assert(zs1.Has(Zi(P_SKS, 65539)), gc.Equals, true)
	c.Assert(zs2.Has(Zi(P_SKS, 65541)), gc.Equals, true)
	c.Assert(!zs2.Has(Zi(P_SKS, 65539)), gc.Equals, true)
	c.Assert(!zs1.Has(Zi(P_SKS, 65541)), gc.Equals, true)
}

func (s *ZpSuite) TestZSetDiff(c *gc.C) {
	zs1 := NewZSet(Zi(P_SKS, 65537), Zi(P_SKS, 65539))
	zs2 := NewZSet(Zi(P_SKS, 65537), Zi(P_SKS, 65541))
	zs3 := ZSetDiff(zs1, zs2)
	zs4 := ZSetDiff(zs2, zs1)
	c.Assert(zs3.Has(Zi(P_SKS, 65539)), gc.Equals, true)
	c.Assert(zs3.Items(), gc.HasLen, 1)
	c.Assert(zs4.Has(Zi(P_SKS, 65541)), gc.Equals, true)
	c.Assert(zs4.Items(), gc.HasLen, 1)
}

func (s *ZpSuite) TestZSetDiffEmpty(c *gc.C) {
	zs1 := NewZSet(Zi(P_SKS, 65537), Zi(P_SKS, 65539))
	zs2 := NewZSet()
	zs3 := ZSetDiff(zs1, zs2)
	zs4 := ZSetDiff(zs2, zs1)
	c.Assert(zs3.Has(Zi(P_SKS, 65537)), gc.Equals, true)
	c.Assert(zs3.Has(Zi(P_SKS, 65539)), gc.Equals, true)
	c.Assert(zs3.Items(), gc.HasLen, 2)
	c.Assert(zs4.Items(), gc.HasLen, 0)
}

func (s *ZpSuite) TestByteOrder(c *gc.C) {
	z := Zi(P_SKS, 65536)
	c.Logf("%x", z.Bytes())
	c.Assert(byte(0), gc.Equals, z.Bytes()[0])
	c.Assert(byte(0), gc.Equals, z.Bytes()[1])
	c.Assert(byte(1), gc.Equals, z.Bytes()[2])
}

func (s *ZpSuite) TestByteRtt(c *gc.C) {
	z := Zi(P_SKS, 65536)
	z2 := Zb(P_SKS, z.Bytes())
	c.Assert(z.Bytes(), gc.DeepEquals, z2.Bytes())
}

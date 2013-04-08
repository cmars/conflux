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

func p(n int) *big.Int {
	return big.NewInt(int64(n))
}

func zp5(n int) *Zp {
	return Zi(p(5), n)
}

func zp7(n int) *Zp {
	return Zi(p(7), n)
}

func TestAdd(t *testing.T) {
	a := zp5(1)
	b := zp5(3)
	assert.Equal(t, 0, zp5(4).Cmp(a.Add(a, b)))
}

func TestAddWrap(t *testing.T) {
	a := zp5(1)
	b := zp5(9)
	assert.Equal(t, 0, zp5(0).Cmp(a.Add(a, b)))
}

func TestMinusOne(t *testing.T) {
	a := Zi(p(65537), -1)
	assert.Equal(t, int64(65536), a.Int64())
}

func TestMul(t *testing.T) {
	// 4x3
	a := zp5(4)
	b := zp5(3)
	a.Mul(a, b)
	assert.Equal(t, int64(2), a.Int64())
	// 4x4x3
	a = zp5(4)
	b = zp5(3)
	a.Mul(a, a)
	a.Mul(a, b)
	assert.Equal(t, int64(3), a.Int64())
	// 16x16
	a = zp5(4)
	a.Mul(a, a) // 4x4
	a.Mul(a, a) // 16x16
	assert.Equal(t, int64(1), a.Int64())
}

func TestDiv(t *testing.T) {
	// in Z(5), 1 / 2 = 3 because 3 * 2 = 1.
	a := zp5(1)
	b := zp5(2)
	q := Z(p(5)).Div(a, b)
	assert.Equal(t, int64(3), q.Int64())
	// in Z(5), 1 / 3 = 2 because 3 * 2 = 1.
	a = zp5(1)
	b = zp5(3)
	q = Z(p(5)).Div(a, b)
	assert.Equal(t, int64(2), q.Int64())
}

func TestMismatchedP(t *testing.T) {
	defer func() {
		r := recover()
		assert.T(t, r != nil)
	}()
	a := zp5(1)
	b := Zi(p(65537), 9)
	a.Add(a, b)
	t.Fail()
}

func TestNeg(t *testing.T) {
	a := zp5(2)
	a.Neg()
	assert.Equal(t, int64(3), a.Int64())
	a = zp5(0)
	a.Neg()
	assert.Equal(t, int64(0), a.Int64())
}

func TestSub(t *testing.T) {
	a := zp5(4)
	b := zp5(3)
	c := a.Copy().Sub(a, b)
	assert.Equal(t, int64(4), a.Int64())
	assert.Equal(t, int64(3), b.Int64())
	assert.Equal(t, int64(1), c.Int64())
}

func TestSubRoll(t *testing.T) {
	a := zp5(1)
	b := zp5(3)
	c := a.Copy().Sub(a, b)
	assert.Equal(t, int64(1), a.Int64())
	assert.Equal(t, int64(3), b.Int64())
	assert.Equal(t, int64(3), c.Int64()) // -2 == 3
	a = zp5(1)
	b = zp5(4)
	c = a.Copy().Sub(a, b)
	assert.Equal(t, int64(1), a.Int64())
	assert.Equal(t, int64(4), b.Int64())
	assert.Equal(t, int64(2), c.Int64()) // -3 == 2
}

func TestZSet(t *testing.T) {
	a := NewZSet()
	a.Add(zp5(1))
	a.Add(zp5(1))
	a.Add(zp5(2))
	a.Add(zp5(3))
	items := a.Items()
	assert.Equal(t, 3, len(items))
	assert.T(t, a.Has(zp5(1)))
	assert.T(t, a.Has(zp5(2)))
	assert.T(t, a.Has(zp5(3)))
}

func TestZsetDisjoint(t *testing.T) {
	zs1 := NewZSet(Zi(P_SKS, 65537), Zi(P_SKS, 65539))
	zs2 := NewZSet(Zi(P_SKS, 65537), Zi(P_SKS, 65541))
	assert.T(t, zs1.Has(Zi(P_SKS, 65537)))
	assert.T(t, zs2.Has(Zi(P_SKS, 65537)))
	assert.T(t, zs1.Has(Zi(P_SKS, 65539)))
	assert.T(t, zs2.Has(Zi(P_SKS, 65541)))
	assert.T(t, !zs2.Has(Zi(P_SKS, 65539)))
	assert.T(t, !zs1.Has(Zi(P_SKS, 65541)))
}

func TestZSetDiff(t *testing.T) {
	zs1 := NewZSet(Zi(P_SKS, 65537), Zi(P_SKS, 65539))
	zs2 := NewZSet(Zi(P_SKS, 65537), Zi(P_SKS, 65541))
	zs3 := ZSetDiff(zs1, zs2)
	zs4 := ZSetDiff(zs2, zs1)
	assert.T(t, zs3.Has(Zi(P_SKS, 65539)))
	assert.Equal(t, 1, len(zs3.Items()))
	assert.T(t, zs4.Has(Zi(P_SKS, 65541)))
	assert.Equal(t, 1, len(zs4.Items()))
}

func TestZSetDiffEmpty(t *testing.T) {
	zs1 := NewZSet(Zi(P_SKS, 65537), Zi(P_SKS, 65539))
	zs2 := NewZSet()
	zs3 := ZSetDiff(zs1, zs2)
	zs4 := ZSetDiff(zs2, zs1)
	assert.T(t, zs3.Has(Zi(P_SKS, 65537)))
	assert.T(t, zs3.Has(Zi(P_SKS, 65539)))
	assert.Equal(t, 2, len(zs3.Items()))
	assert.Equal(t, 0, len(zs4.Items()))
}

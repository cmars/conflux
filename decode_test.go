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
	"crypto/rand"
	"github.com/bmizerany/assert"
	"math/big"
	"testing"
)

func randInt(max int) int {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		panic(err)
	}
	return int(n.Int64())
}

// randLinearProd randomly generates a product of
// linears (x + a0)(x + a1)...(x + an).
func randLinearProd(p *big.Int, n int) *Poly {
	result := NewPoly(Zi(p, 1))
	for i := 0; i < n; i++ {
		result = NewPoly().Mul(result, PolyRand(p, 1))
	}
	return result
}

func factorTest(t *testing.T) {
	deg := randInt(10) + 1
	p := big.NewInt(int64(97))
	// Create a factor-able, polynomial product of linears
	var poly *Poly = randLinearProd(p, deg)
	t.Logf("factor poly: (%v)", poly)
	roots, err := poly.Factor()
	assert.Equal(t, err, nil)
	t.Logf("roots=%v", roots)
	// TODO: test roots against z0 constants in the linears
}

func TestFactorization(t *testing.T) {
	for i := 0; i < 100; i++ {
		t.Logf("Factorization #%d", i)
		factorTest(t)
	}
}

func TestCannedInterpolation(t *testing.T) {
	/*
	interpolate
	values=[50209572917763804813893169477404135246 523915264287429384599917983489241637041 193879208340335596473327301694891073112 336257832174512052845041381684545224326 525220581565510310465258018589146771167 369646301408454767673033771110855434260 371821946850459872311187739000814476019 144426966457292640051271632674756114101 379207879747256731229136438792149285186 46108152160169587744128314614996604924 227801899428306415871207999262631174702 207497927707680176901864453717256663645 190227327194805171829784109272423912872 ]
	points=[0 -1 1 -2 2 -3 3 -4 4 -5 5 -6 6 ]
	d=-11
	num=1 z^1 + 201510631159794911579036209221877731351
	denom=1 z^12 + 129168611341530605578585909520009112853 z^11 + 49011742009925395272518613422388842885 z^10 + 209097283573511646123086148468849229449 z^9 + 91519704684961309708461769260348368047 z^8 + 451945789461805767613376502019011847396 z^7 + 278888159583692127965164290452048385915 z^6 + 323796663999875107447504850182776354321 z^5 + 276914137420158008254462690448206036905 z^4 + 496937274460702615962215989437926963535 z^3 + 213853624487129571321714452851928100712 z^2 + 295519390096665634601203803035473291375 z^1 + 471406228141421561633415986254867829648
	*/
	p := P_SKS
	values := []*Zp{Zs(p, "50209572917763804813893169477404135246"), Zs(p, "523915264287429384599917983489241637041"), Zs(p, "193879208340335596473327301694891073112"), Zs(p, "336257832174512052845041381684545224326"), Zs(p, "525220581565510310465258018589146771167"), Zs(p, "369646301408454767673033771110855434260"), Zs(p, "371821946850459872311187739000814476019"), Zs(p, "144426966457292640051271632674756114101"), Zs(p, "379207879747256731229136438792149285186"), Zs(p, "46108152160169587744128314614996604924"), Zs(p, "227801899428306415871207999262631174702"), Zs(p, "207497927707680176901864453717256663645"), Zs(p, "190227327194805171829784109272423912872")}
	points := []*Zp{Zi(p, 0), Zi(p, -1), Zi(p, 1), Zi(p, -2), Zi(p, 2), Zi(p, -3), Zi(p, 3), Zi(p, -4), Zi(p, 4), Zi(p, -5), Zi(p, 5), Zi(p, -6), Zi(p, 6)}
	d := -11
	rfn, err := Interpolate(values, points, d)
	assert.Equal(t, err, nil)
	t.Logf("num=%v denom=%v", rfn.Num, rfn.Denom)
	numExpect := []*Zp{Zs(p, "201510631159794911579036209221877731351"), Zi(p, 1)}
	denomExpect := []*Zp{
		Zs(p, "471406228141421561633415986254867829648"),
		Zs(p, "295519390096665634601203803035473291375"),
		Zs(p, "213853624487129571321714452851928100712"),
		Zs(p, "496937274460702615962215989437926963535"),
		Zs(p, "276914137420158008254462690448206036905"),
		Zs(p, "323796663999875107447504850182776354321"),
		Zs(p, "278888159583692127965164290452048385915"),
		Zs(p, "451945789461805767613376502019011847396"),
		Zs(p, "91519704684961309708461769260348368047"),
		Zs(p, "209097283573511646123086148468849229449"),
		Zs(p, "49011742009925395272518613422388842885"),
		Zs(p, "129168611341530605578585909520009112853"),
		Zi(p, 1)}
	for i, z := range numExpect {
		assert.Equal(t, z.String(), rfn.Num.coeff[i].String())
	}
	for i, z := range denomExpect {
		assert.Equal(t, z.String(), rfn.Denom.coeff[i].String())
	}
}

func TestInterpolation(t *testing.T) {
	for i := 0; i < 100; i++ {
		t.Logf("Interpolation #%d", i)
		interpTest(t)
	}
}

func interpTest(t *testing.T) {
	var err error
	p := P_SKS
	deg := randInt(10) + 1
	numDeg := randInt(deg)
	denomDeg := deg - numDeg
	num := randLinearProd(p, numDeg)
	denom := randLinearProd(p, denomDeg)
	assert.Equal(t, num.degree, numDeg)
	assert.Equal(t, denom.degree, denomDeg)
	t.Logf("num: (%v) denom: (%v)", num, denom)
	mbar := randInt(9) + 1
	n := mbar + 1
	toobig := deg+1 > mbar
	values := make([]*Zp, n)
	points := make([]*Zp, n)
	for i := 0; i < n; i++ {
		var pi int
		if i%2 == 0 {
			pi = ((i + 1) / 2) * 1
		} else {
			pi = ((i + 1) / 2) * -1
		}
		points[i] = Zi(p, pi)
		values[i] = Z(p).Div(num.Eval(points[i]), denom.Eval(points[i]))
	}
	t.Logf("values=(%v) points=(%v) degDiff=(%v)", values, points, abs(numDeg-denomDeg))
	rfn, err := Interpolate(values, points, numDeg-denomDeg)
	if toobig {
		return
	} else {
		assert.Equal(t, err, nil)
	}
	//t.Logf("mbar: %d, num_deg: %d, denom_deg: %d", mbar, numDeg, denomDeg)
	assert.Tf(t, num.Equal(rfn.Num), "num: (%v) != (%v)", num, rfn.Num)
	assert.Tf(t, denom.Equal(rfn.Denom), "denom: (%v) != (%v)", denom, rfn.Denom)
}

type zGenF func() *Zp

func setInit(n int, f zGenF) *ZSet {
	zs := NewZSet()
	for i := 0; i < n; i++ {
		zs.Add(f())
	}
	return zs
}

func TestReconcile(t *testing.T) {
	for i := 0; i < 100; i++ {
		t.Logf("Reconcile #%d", i)
		reconcileTest(t)
	}
}

func reconcileTest(t *testing.T) {
	p := P_SKS
	mbar := randInt(20) + 1
	n := mbar + 1
	svalues1 := Zarray(p, n, Zi(p, 1))
	svalues2 := Zarray(p, n, Zi(p, 1))
	points := Zpoints(p, n)
	m := randInt(mbar*2) + 1
	// m1 and m2 are a partitioning of m
	m1 := randInt(m)
	m2 := m - m1
	set1 := setInit(m1, func() *Zp { return Zrand(p) })
	set2 := setInit(m2, func() *Zp { return Zrand(p) })
	t.Logf("mbar: %d, m: %d, m1: %d, m2: %d", mbar, m, m1, m2)
	for _, s1i := range set1.Items() {
		for i := 0; i < n; i++ {
			svalues1[i].Mul(svalues1[i].Copy(), Z(p).Sub(points[i], s1i))
		}
	}
	for _, s2i := range set2.Items() {
		for i := 0; i < n; i++ {
			svalues2[i].Mul(svalues2[i].Copy(), Z(p).Sub(points[i], s2i))
		}
	}
	values := make([]*Zp, len(svalues1))
	for i := 0; i < len(values); i++ {
		values[i] = Z(p).Div(svalues1[i], svalues2[i])
	}
	diff1, diff2, err := Reconcile(values, points, m1-m2)
	if err != nil {
		t.Logf("Low MBar")
		assert.Tf(t, err != nil, "error: %v", err)
		assert.Tf(t, m > mbar, "m %d > mbar %d", m, mbar)
		return
	}
	assert.Equal(t, err, nil)
	t.Logf("recon compare: %v ==? %v", diff1, set1)
	t.Logf("recon compare: %v ==? %v", diff2, set2)
	assert.T(t, diff1.Equal(set1))
	assert.T(t, diff2.Equal(set2))
}

func TestFactorCheck(t *testing.T) {
	//factor_check x=1 z^2 + 117479252320778380699969369242473163812 z^1 + 23910866165498202015403350789738609658 zq=1 z^1 + 0 mz=530512889551602322505127520352579437338 z^1 + 0 zqmz=0
	p := P_SKS
	x := NewPoly(Zs(p, "23910866165498202015403350789738609658"),
			Zs(p, "117479252320778380699969369242473163812"),
			Zs(p, "1"))
	assert.Tf(t, factorCheck(x), "%v", x)
}

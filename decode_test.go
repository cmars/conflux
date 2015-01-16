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
	"crypto/rand"
	"math/big"

	gc "gopkg.in/check.v1"
	"gopkg.in/errgo.v1"
)

type DecodeSuite struct{}

var _ = gc.Suite(&DecodeSuite{})

func randInt(max int) int {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		panic(err)
	}
	return int(n.Int64())
}

// randLinearProd randomly generates a product of
// linears (x - a0)(x - a1)...(x - an).
func randLinearProd(p *big.Int, n int) (*Poly, *ZSet) {
	result := NewPoly(Zi(p, 1))
	roots := NewZSet()
	for i := 0; i < n; i++ {
		pr := PolyRand(p, 1)
		roots.Add(pr.coeff[0].Copy().Neg()) // The root is negated: a0 from (z - a0)
		result = NewPoly().Mul(result, pr)
	}
	return result, roots
}

func factorTest(c *gc.C) {
	deg := randInt(10) + 1
	p := big.NewInt(int64(97))
	// Create a factor-able, polynomial product of linears
	poly, roots := randLinearProd(p, deg)
	c.Logf("factor poly: (%v)", poly)
	factoredRoots, err := poly.Factor()
	c.Assert(err, gc.IsNil)
	c.Logf("factoredRoots=%v ?== roots=%v", factoredRoots, roots)
	c.Assert(roots.Equal(factoredRoots), gc.Equals, true,
		gc.Commentf("%v !== %v", roots, factoredRoots))
}

func (s *DecodeSuite) TestFactorization(c *gc.C) {
	for i := 0; i < 100; i++ {
		c.Logf("Factorization #%d", i)
		factorTest(c)
	}
}

func (s *DecodeSuite) TestCannedInterpolation(c *gc.C) {
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
	c.Assert(err, gc.IsNil)
	c.Logf("num=%v denom=%v", rfn.Num, rfn.Denom)
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
		c.Assert(z.String(), gc.Equals, rfn.Num.coeff[i].String())
	}
	for i, z := range denomExpect {
		c.Assert(z.String(), gc.Equals, rfn.Denom.coeff[i].String())
	}
}

func (s *DecodeSuite) TestInterpolation(c *gc.C) {
	for i := 0; i < 100; i++ {
		c.Logf("Interpolation #%d", i)
		interpTest(c)
	}
}

func interpTest(c *gc.C) {
	var err error
	p := P_SKS
	deg := randInt(10) + 1
	numDeg := randInt(deg)
	denomDeg := deg - numDeg
	num, _ := randLinearProd(p, numDeg)
	denom, _ := randLinearProd(p, denomDeg)
	c.Assert(num.degree, gc.Equals, numDeg)
	c.Assert(denom.degree, gc.Equals, denomDeg)
	c.Logf("num: (%v) denom: (%v)", num, denom)
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
	c.Logf("values=(%v) points=(%v) degDiff=(%v)", values, points, abs(numDeg-denomDeg))
	rfn, err := Interpolate(values, points, numDeg-denomDeg)
	if toobig {
		return
	} else {
		c.Assert(err, gc.IsNil)
	}
	c.Logf("mbar: %d, num_deg: %d, denom_deg: %d", mbar, numDeg, denomDeg)
	c.Assert(num.Equal(rfn.Num), gc.Equals, true, gc.Commentf("num: (%v) != (%v)", num, rfn.Num))
	c.Assert(denom.Equal(rfn.Denom), gc.Equals, true, gc.Commentf("denom: (%v) != (%v)", denom, rfn.Denom))
}

type zGenF func() *Zp

func setInit(n int, f zGenF) *ZSet {
	zs := NewZSet()
	for i := 0; i < n; i++ {
		zs.Add(f())
	}
	return zs
}

func (s *DecodeSuite) TestCannedReconcile(c *gc.C) {
	p := P_SKS
	set1 := NewZSet()
	s1items := []*Zp{Zs(p, "8952777669297728851091848378379377617"), Zs(p, "162085839528403560100929159811161460293"), Zs(p, "181484969924633124558171484324504401075"), Zs(p, "229305846979453177871691812413112208676"), Zs(p, "284001389401364703525738874626145923778"), Zs(p, "333026889954813771673937036618957938545"), Zs(p, "401537002901186069501925598757914356337"), Zs(p, "408597178507212301417184698839771487762"), Zs(p, "419504520512224794235831228788173561599"), Zs(p, "454233583376105592897174699470827876606")}
	set1.AddSlice(s1items)
	set2 := NewZSet()
	s2items := []*Zp{Zs(p, "110633522524732890588089295220994803977"), Zs(p, "194223389264051186134544082841809104115"), Zs(p, "332150253195118153886619367406744054566"), Zs(p, "431844203966462129313295191768688911950"), Zs(p, "505931393060085050712145173574130038354")}
	set2.AddSlice(s2items)
	values := []*Zp{Zs(p, "325567491442841181381134847399735305017"), Zs(p, "395037391445571452972721527936312200522"), Zs(p, "383458038386494547334014086327713094385"), Zs(p, "217174866600085692973450729194577792210"), Zs(p, "385011357579896657977528957507240613253"), Zs(p, "402781597512949507740967136267068344630"), Zs(p, "232703526201630690874279192086579665024"), Zs(p, "517714262168165665799778316804817689980"), Zs(p, "32661406820901877880191293945563287049"), Zs(p, "367894599536965704928081351211416869922"), Zs(p, "277789799296462035245112840153664041656"), Zs(p, "55517351568679792361000876949275186668"), Zs(p, "262234380059790006121506334185487551936"), Zs(p, "269358796384139303257285300138875449325"), Zs(p, "230494386168101481930613981157929389116"), Zs(p, "497730714764611287106884245786790787566"), Zs(p, "51691307971910305814631217339926265833"), Zs(p, "290446399753991600191456012845409641740"), Zs(p, "427530032313331291010476618229794543878"), Zs(p, "120344848642406229503266522177541779886"), Zs(p, "399989145164239204145711147975735514135")}
	points := []*Zp{Zi(p, 0), Zi(p, -1), Zi(p, 1), Zi(p, -2), Zi(p, 2), Zi(p, -3), Zi(p, 3), Zi(p, -4), Zi(p, 4), Zi(p, -5), Zi(p, 5), Zi(p, -6), Zi(p, 6), Zi(p, -7), Zi(p, 7), Zi(p, -8), Zi(p, 8), Zi(p, -9), Zi(p, 9), Zi(p, -10), Zi(p, 10)}
	m1 := len(s1items)
	m2 := len(s2items)
	diff1, diff2, err := Reconcile(values, points, m1-m2)
	c.Assert(err, gc.IsNil)
	c.Logf("recon compare: %v ==? %v", diff1, set1)
	c.Logf("recon compare: %v ==? %v", diff2, set2)
	c.Assert(diff1, gc.DeepEquals, set1)
	c.Assert(diff2, gc.DeepEquals, set2)
	c.Assert(diff1.Equal(set1), gc.Equals, true)
	c.Assert(diff2.Equal(set2), gc.Equals, true)
}

func (s *DecodeSuite) TestReconcile(c *gc.C) {
	for i := 0; i < 100; i++ {
		c.Logf("Reconcile #%d", i)
		reconcileTest(c)
	}
}

func reconcileTest(c *gc.C) {
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
	c.Logf("mbar: %d, n: %d, m: %d, m1: %d, m2: %d", mbar, n, m, m1, m2)
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
	c.Logf("values=%v\npoints=%v\nd=%v", values, points, m1-m2)
	diff1, diff2, err := Reconcile(values, points, m1-m2)
	if err != nil {
		c.Logf("Low MBar")
		c.Assert(m > mbar, gc.Equals, true, gc.Commentf("m %d > mbar %d", m, mbar))
		return
	}
	c.Assert(err, gc.IsNil)
	c.Logf("recon compare: %v ==? %v", diff1, set1)
	c.Logf("recon compare: %v ==? %v", diff2, set2)
	c.Assert(diff1, gc.DeepEquals, set1)
	c.Assert(diff2, gc.DeepEquals, set2)
	c.Assert(diff1.Equal(set1), gc.Equals, true)
	c.Assert(diff2.Equal(set2), gc.Equals, true)
}

func (s *DecodeSuite) TestLowMBar(c *gc.C) {
	p := P_SKS
	values := []*Zp{Zs(p, "260405721246918987273155339614020972656"), Zs(p, "243393001638573476362665007855413044937"), Zs(p, "505905314437392989818278468923779137359"), Zs(p, "105358332430258313066486664282953088018"), Zs(p, "2560440886574256298562818527295701964"), Zs(p, "118746265689993312951910051444187575775"), Zs(p, "529698088600031242289045200206930982765"), Zs(p, "441488592726201746187835041000728091281")}
	points := Zpoints(p, len(values))
	_, _, err := Reconcile(values, points, 3)
	c.Assert(errgo.Cause(err), gc.Equals, ErrLowMBar)
}

func (s *DecodeSuite) TestFactorCheck(c *gc.C) {
	//factor_check x=1 z^2 + 117479252320778380699969369242473163812 z^1 + 23910866165498202015403350789738609658 zq=1 z^1 + 0 mz=530512889551602322505127520352579437338 z^1 + 0 zqmz=0
	p := P_SKS
	x := NewPoly(Zs(p, "23910866165498202015403350789738609658"),
		Zs(p, "117479252320778380699969369242473163812"),
		Zs(p, "1"))
	c.Assert(factorCheck(x), gc.Equals, true, gc.Commentf("%v", x))
}

func (s *DecodeSuite) TestPolyNomNomNom(c *gc.C) {
	// Values obtained from dumping an SKS unit test run.
	p := P_SKS
	num := NewPoly(Zs(p, "201510631159794911579036209221877731351"), Zi(p, 1))
	denom := NewPoly(Zs(p, "471406228141421561633415986254867829648"),
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
		Zi(p, 1))
	point := Zi(p, -7)
	numAt := num.Eval(point)
	c.Assert(numAt.String(), gc.Equals, "201510631159794911579036209221877731344")
	denomAt := denom.Eval(point)
	c.Assert(denomAt.String(), gc.Equals, "77151748131754717019960190430023395826")
	rational := Z(p).Div(numAt, denomAt)
	c.Assert(rational.String(), gc.Equals, "372597725470208235965358485960825765733")
}

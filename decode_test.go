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
	"testing"
	"math/big"
	"crypto/rand"
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

func TestInterpolation(t *testing.T) {
	for i := 0; i < 100; i++ {
		t.Logf("Interpolation #%d", i)
		interpTest(t)
	}
}

func interpTest(t *testing.T) {
	var err error
	p := big.NewInt(int64(97))
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
	//toobig := deg + 1 > mbar
	values := make([]*Zp, n)
	points := make([]*Zp, n)
	for i := 0; i < n; i++ {
		var pi int
		if i % 2 == 0 {
			pi = ((i + 1) / 2) * 1
		} else {
			pi = ((i + 1) / 2) * -1
		}
		points[i] = Zi(p, pi)
		values[i] = Z(p).Div(num.Eval(points[i]), denom.Eval(points[i]))
		assert.Equal(t, err, nil)
	}
	rfn, err := Interpolate(values, points, numDeg - denomDeg)
	assert.Equal(t, err, nil)
    t.Logf("mbar: %d, num_deg: %d, denom_deg: %d", mbar, numDeg, denomDeg)
    t.Logf("num: (%v) === (%v)", num, rfn.Num)
    t.Logf("denom: (%v) === (%v)", denom, rfn.Denom)
/*
  let toobig = deg + 1 > mbar in
  let values  = ZZp.mut_array_to_array (ZZp.svalues n) in
  let points = ZZp.points n in
  for i = 0 to Array.length values - 1 do
    values.(i) <- Poly.eval num points.(i) /: Poly.eval denom points.(i)
  done;
  try
    let (found_num,found_denom) =
      Decode.interpolate ~values ~points ~d:(num_deg - denom_deg)
    in
(*    printf "mbar: %d, num_deg: %d, denom_deg: %d\n" mbar num_deg denom_deg;
    printf "num: %s\ndenom: %s\n%!" (Poly.to_string num) (Poly.to_string denom);
    printf "gcd: %s\n" (Poly.to_string (Poly.gcd num denom));
    printf "found num: %s\nfound denom: %s\n%!"
      (Poly.to_string found_num) (Poly.to_string found_denom); *)
    test "degree equality" (toobig
                            || (Poly.degree found_num = Poly.degree num
                                && Poly.degree found_denom = Poly.degree denom));
    test "num equality" (toobig || Poly.eq found_num num);
    test "denom equality" (toobig || Poly.eq found_denom denom);
 with
     Interpolation_failure ->
       test (sprintf "interpolation failed (deg:%d,mbar:%d)" deg mbar)
         (deg + 1 > mbar)
*/
}

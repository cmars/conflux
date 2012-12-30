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
	mrand "math/rand"
)

func randPoly(p *big.Int, dim int) *Poly {
	terms := make([]*Zp, dim+1)
	for i := 0; i <= dim; i++ {
		if i == dim {
			terms[i] = Zi(p, 1)
		} else {
			terms[i] = Zrand(p)
		}
	}
	return NewPoly(terms...)
}

func factorTest(t *testing.T) {
	deg := (mrand.Int() % 10) + 1
	terms := []*Poly{}
	p := big.NewInt(int64(97))
	for i := 0; i <= deg; i++ {
		terms = append(terms, randPoly(p, 1))
	}
	var poly *Poly
	for _, term := range terms {
		t.Logf("term (%v)", term)
		if poly == nil {
			poly = term
		} else {
			poly = NewPoly().Mul(poly, term)
		}
	}
	t.Logf("factor poly: (%v)", poly)
	roots, err := poly.Factor()
	assert.Equal(t, err, nil)
	t.Logf("roots=%v", roots)
/*
  let deg = rand_int 10 + 1 in
  let terms = Array.to_list (Array.init deg (fun _ -> rand_poly 1)) in
  let poly = List.fold_left ~init:Poly.one ~f:Poly.mult terms in
  let roots = Decode.factor poly in
  let orig_roots =
    ZZp.zset_of_list (List.map ~f:(fun p -> ZZp.neg (Poly.to_array p).(0)) terms)
  in
  test "factor equality" (ZSet.equal orig_roots roots)
*/
}

func TestFactorization(t *testing.T) {
	for i := 0; i < 100; i++ {
		t.Logf("Factorization #%d", i)
		factorTest(t)
	}
}

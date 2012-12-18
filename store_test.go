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

var p97 *big.Int = big.NewInt(int64(97))

func TestExample323(t *testing.T) {
	saVals := []int{ 1, 2, 9, 12, 33 }
	sa := NewSimpleStore()
	for _, v := range saVals {
		sa.Add(Zi(p97, v))
	}
	mVals := []int{ -1, -2, -3, -4, -5 }
	xzaVals := []int{ 58, 19, 89, 77, 4 }
	for i := 0; i < len(mVals); i++ {
		xsaz, _ := sa.Evaluate(Zi(p97, mVals[i]))
		assert.Equal(t, int64(xzaVals[i]), xsaz.Int64())
	}
	sbVals := []int{ 1, 2, 9, 10, 12, 28 }
	sb := NewSimpleStore()
	for _, v := range sbVals {
		sb.Add(Zi(p97, v))
	}
	xzbVals := []int{ 15, 54, 68, 77, 50 }
	for i := 0; i < len(mVals); i++ {
		xsbz, _ := sb.Evaluate(Zi(p97, mVals[i]))
		assert.Equal(t, int64(xzbVals[i]), xsbz.Int64())
	}
}

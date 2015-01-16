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

// primegen is a utility for generating large primes that bound
// a given bit length.
package main

import (
	"crypto/rand"
	"fmt"
)

func main() {
	for _, n := range []int{128, 160, 256, 512} {
		p, err := rand.Prime(rand.Reader, n+1)
		if err != nil {
			panic(err)
		}
		fmt.Printf("var p_%v = big.NewInt(0).SetBytes([]byte{", n)
		data := p.Bytes()
		for i, b := range data {
			if i > 0 {
				fmt.Printf(",")
			}
			if i < len(data)-1 && i%8 == 0 {
				fmt.Printf("\n\t")
			}
			fmt.Printf("0x%x", b)
		}
		fmt.Printf("})\n\n")
	}
}

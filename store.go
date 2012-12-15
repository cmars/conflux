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
	"math/big"
)

// SetStore defines an interface for element storage operations
// (add, remove, exists) and per-point (m) evaluation.
type SetStore interface {
	// Evaluate the key store polynomial for m.
	Evaluate(m *Zp) (result *Zp, err error)
	// Add an element into the store.
	Add(element *Zp) error
	// Remove a key from the store.
	Remove(element *Zp) error
	// Test if a key exists in the store.
	Exists(element *Zp) (bool, error)
}

// SimpleStore is a very naive, inefficient implementation
// of a set store. However it is useful for demonstrating
// the concept, and for testing.
type SimpleStore struct {
	elements map[string]*Zp
}

func NewSimpleStore() *SimpleStore {
	return &SimpleStore{ elements: make(map[string]*Zp) }
}

func (ss *SimpleStore) Evaluate(m *Zp) (result *Zp, err error) {
	result = &Zp{ Int: big.NewInt(1), P: m.P }
	for _, v := range ss.elements {
		mv := &Zp{ Int: big.NewInt(0).Set(m.Int), P: m.P }
		mv.Sub(mv, v)
		result.Mul(result, mv)
	}
	return result, nil
}

func (ss *SimpleStore) Add(element *Zp) error {
	ss.elements[element.String()] = element
	return nil
}

func (ss *SimpleStore) Remove(element *Zp) error {
	delete(ss.elements, element.String())
	return nil
}

func (ss *SimpleStore) Exists(element *Zp) (bool, error) {
	_, has := ss.elements[element.String()]
	return has, nil
}

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

// Store defines an interface for key storage operations (add, remove, exists)
// and per-point (m) evaluation.
type Store interface {
	// Evaluate the key store polynomial for m.
	Evaluate(m *Zp) (result *Zp, err error)
	// Add a key into the store.
	Add(key *Zp) error
	// Remove a key from the store.
	Remove(key *Zp) error
	// Test if a key exists in the store.
	Exists(key *Zp) (bool, error)
}

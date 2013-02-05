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

package recon

import (
	_ "github.com/bmizerany/assert"
	. "github.com/cmars/conflux"
	"math/rand"
	"testing"
	"github.com/petar/GoLLRB/llrb"
)

/*
open StdLabels
open MoreLabels
module Unix=UnixLabels

module Set = PSet.Set
open Printf
(*module ZZp = RMisc.ZZp *)
module PTree = PrefixTree

let debug = !Settings.debug
*/

/*
let base = 1000
let bitquantum = !Settings.bitquantum
let num_samples = !Settings.mbar + 1

let (tree: unit option PTree.tree ) = 
  PTree.create ~txn:None ~num_samples ~bitquantum ~thresh:!Settings.mbar ()
let timer = MTimer.create () 

let keymatch ~key string = 
  let bitlength = Bitstring.num_bits key in
  let bstring = Bitstring.of_bytes_all_nocopy string in
  let keystr = Bitstring.create bitlength in
  Bitstring.blit ~src:bstring ~dst:keystr ~len:bitlength;
  (Bitstring.to_bytes_nocopy keystr) = (Bitstring.to_bytes_nocopy key)

let one = ZZp.of_int 1

let compute_svalue point elements = 
  Set.fold
    ~f:(fun el prod -> ZZp.mult prod (ZZp.sub point el))
    ~init:ZZp.one
    elements

let compute_svalues points elements =
  let array = 
    Array.map ~f:(fun point -> compute_svalue point elements) points
  in 
  ZZp.mut_array_of_array array

let print_vec vec = 
  let list = Array.to_list (ZZp.mut_array_to_array vec) in
  MList.print2 ~f:ZZp.print list

(*******************************************************)
*/

func addOrDelete(set *llrb.Tree, tree PTree, p float64) {
	var zz *Zp
	// Call it, friend-o.
	if rand.Float64() < p {
		// Add node
		zz = Zrand(P_SKS)
		tree.Insert(zz)
		set.InsertNoReplace(zz)
	} else {
		// Remove node
		if set.Len() == 0 {
			// No nodes, roll again
			addOrDelete(set, tree, p)
		} else {
			zz = set.DeleteMin().(*Zp)
			tree.Delete(zz)
		}
	}
}

func zpSlicesEqual(s1 []*Zp, s2 []*Zp) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := 0; i < len(s1); i++ {
		if s1[i].Cmp(s2[i]) != 0 {
			return false
		}
	}
	return true
}

func cmpZp(a, b interface{}) bool {
	za, isa := a.(*Zp)
	zb, isb := b.(*Zp)
	if !isa || !isb {
		// get ready for hurt
		return false
	}
	return za.Cmp(zb) == 0
}

func createPTree() PTree {
	panic("implement meeeeee")
}

func TestPrefixTree(t *testing.T) {
	set := llrb.New(cmpZp)
	tree := createPTree()
	for i := 0; i < 100000; i++ {
		addOrDelete(set, tree, 0.52)
	}
	panic("you shall not pass (until that commented code below is ported).")
/*
  let pt_set = PTree.elements tree (PTree.root tree) in
  if Set.equal !set pt_set
  then 
    print_string "Set and PTree report identical elements\n"
  else (
    print_string "Failure: Set and PTree report different elements\n";
    printf "Set:  \t%d, %s\n" (Set.cardinal !set) (ZZp.to_string (Set.min_elt !set));
    printf "Tree: \t%d, %s\n" (Set.cardinal pt_set) (ZZp.to_string (Set.min_elt pt_set));
    if Set.subset !set pt_set then
      printf "set is subset of tree\n"
    else if Set.subset pt_set !set then
      printf "tree is susbet of set\n"
    else 
      printf "No subset relationship\n"
      
  );

  if PTree.is_leaf (PTree.root tree) 
  then print_string "Root is leaf\n";

  let points = PTree.points tree in

  let rec verify key = 
    let node = PTree.get_node_key tree key in
    let elements = PTree.elements tree node in
    let svalues_computed = compute_svalues points elements in
    let svalues = PTree.svalues node in
    if not (zza_equal svalues_computed svalues)
    then (
      print_vec svalues; print_newline ();
      print_vec svalues_computed; print_newline ();
      failwith "svalues do not match";
    );
    let len = Set.cardinal elements 
    and reported_len = PTree.size node in
    if not (len = reported_len)
    then ( failwith 
	     (sprintf "element size %d does not match reported size %d"
		len reported_len ));
    if debug 
    then printf "Key: %s,\t num elements: %d\n" 
      (Bitstring.to_string key) (Set.cardinal elements);
    Set.iter ~f:(fun el -> 
		   if not (keymatch ~key (ZZp.to_bytes el))
		   then failwith "Elements don't match key!") elements;
    let keys = PTree.child_keys tree key in
    if not (PTree.is_leaf node) then
      List.iter ~f:verify keys
  in
  try
    verify (Bitstring.create 0);
    print_string "Verification succesful\n";
  with 
      Failure s -> 
	print_string (sprintf "Verification failed: %s\n" s);
*/
}

/*
   conflux - Distributed database synchronization library
	Based on the algorithm described in
		"Set Reconciliation with Nearly Optimal	Communication Complexity",
			Yaron Minsky, Ari Trachtenberg, and Richard Zippel, 2004.

   Copyright (C) 2012  Casey Marshall <casey.marshall@gmail.com>

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published by
   the Free Software Foundation, version 3.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package pqptree

const CreateTable_PNode = `
CREATE TABLE IF NOT EXISTS {{.Namespace}}_pnode (
node_key TEXT NOT NULL,
svalues TEXT NOT NULL,
num_elements INTEGER NOT NULL DEFAULT 0,
child_keys INTEGER[],
--
PRIMARY KEY (node_key))`

const CreateTable_PElement = `
CREATE TABLE IF NOT EXISTS {{.Namespace}}_pelement (
node_key TEXT NOT NULL,
element bytea NOT NULL,
--
PRIMARY KEY (element),
FOREIGN KEY (node_key) REFERENCES {{.Namespace}}_pnode(node_key))`

const CreateIndex_PElement_NodeKey = `
CREATE INDEX {{.Namespace}}_pelement_node_key ON {{.Namespace}}_pelement (node_key)`

const DropIndex_PElement_NodeKey = `DROP INDEX {{.Namespace}}_pelement_node_key`

const DropTable_PElement = `DROP TABLE {{.Namespace}}_pelement`

const DropTable_PNode = `DROP TABLE {{.Namespace}}_pnode CASCADE`

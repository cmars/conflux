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

package mgo

import (
	. "github.com/cmars/conflux/recon"
	"labix.org/v2/mgo"
	"log"
)

type client struct {
	connect string
	session *mgo.Session
}

func NewPeer(connect string, db string) (p *Peer, err error) {
	client, err := newClient(connect)
	if err != nil {
		return nil, err
	}
	settings, err := newSettings(client, db)
	if err != nil {
		return nil, err
	}
	tree, err := newPrefixTree(settings, db)
	if err != nil {
		return nil, err
	}
	return &Peer{
		RecoverChan: make(RecoverChan),
		Settings:    settings,
		PrefixTree:  tree}, nil
}

func newClient(connect string) (c *client, err error) {
	c = &client{connect: connect}
	log.Println("Connecting to mongodb:", c.connect)
	c.session, err = mgo.Dial(c.connect)
	if err != nil {
		log.Println("Connection failed:", err)
		return
	}
	c.session.SetMode(mgo.Strong, true)
	// Conservative on writes
	c.session.EnsureSafe(&mgo.Safe{
		W:     1,
		FSync: true})
	return
}

type settings struct {
	*client
	store *mgo.Collection
}

func newSettings(c *client, db string) (s *settings, err error) {
	s = &settings{client: c}
	s.store = c.session.DB(db).C("settings")
	// TODO: ensure indexes
	return s, nil
}

type prefixTree struct {
	*settings
	store *mgo.Collection
}

func newPrefixTree(s *settings, db string) (tree *prefixTree, err error) {
	tree = &prefixTree{settings: s}
	tree.store = s.client.session.DB(db).C("ptree")
	// TODO: ensure indexes
	return tree, nil
}

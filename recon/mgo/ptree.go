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
	"net"
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
	*config
}

type config struct {
	version                     string
	logName                     string
	httpPort                    int
	reconPort                   int
	partners                    []string
	filters                     []string
	threshMult                  int
	bitQuantum                  int
	mBar                        int
	splitThreshold              int
	joinThreshold               int
	numSamples                  int
	gossipIntervalSecs          int
	maxOutstandingReconRequests int
}

func newSettings(c *client, db string) (s *settings, err error) {
	s = &settings{client: c}
	s.store = c.session.DB(db).C("settings")
	// TODO: ensure indexes
	return s, nil
}

func (s *settings) Init() {
	q := s.store.Find(nil)
	if n, err := q.Count(); n == 0 {
		// Set defaults
		s.config = &config{
			version:                     "experimental",
			httpPort:                    11371,
			reconPort:                   11370,
			threshMult:                  DefaultThreshMult,
			bitQuantum:                  DefaultBitQuantum,
			mBar:                        DefaultMBar,
			gossipIntervalSecs:          60,
			maxOutstandingReconRequests: 100}
		// Insert object
		s.update()
	} else {
		s.config = &config{}
		err := q.One(s.config)
		if err != nil {
			panic(err)
		}
	}
	s.config.splitThreshold = s.config.threshMult * s.config.mBar
	s.config.joinThreshold = s.config.splitThreshold / 2
	s.config.numSamples = s.config.mBar + 1
}

func (s *settings) update() {
	err := s.store.Insert(s.config)
	if err != nil {
		panic(err)
	}
}

func (s *settings) Version() string {
	return s.config.version
}

func (s *settings) LogName() string {
	return s.config.logName
}

func (s *settings) HttpPort() int {
	return s.config.httpPort
}

func (s *settings) ReconPort() int {
	return s.config.reconPort
}

func (s *settings) Partners() (addrs []net.Addr) {
	for _, partner := range s.config.partners {
		addr, err := net.ResolveTCPAddr("tcp", partner)
		if err != nil {
			panic(err)
		}
		addrs = append(addrs, addr)
	}
	return
}

func (s *settings) Filters() []string {
	return s.config.filters
}

func (s *settings) ThreshMult() int {
	return s.config.threshMult
}

func (s *settings) BitQuantum() int {
	return s.config.bitQuantum
}

func (s *settings) MBar() int {
	return s.config.mBar
}

func (s *settings) SplitThreshold() int {
	return s.config.splitThreshold
}

func (s *settings) JoinThreshold() int {
	return s.config.joinThreshold
}

func (s *settings) NumSamples() int {
	return s.config.numSamples
}

func (s *settings) GossipIntervalSecs() int {
	return s.config.gossipIntervalSecs
}

func (s *settings) MaxOutstandingReconRequests() int {
	return s.config.maxOutstandingReconRequests
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

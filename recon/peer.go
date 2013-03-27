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

package recon

import (
	"errors"
	"fmt"
	. "github.com/cmars/conflux"
	"io"
	"log"
	"net"
)

const SERVE = "serve:"

type Response interface {
	Error() error
	WriteTo(w io.Writer)
}

type Recover struct {
	RemoteAddr     net.Addr
	RemoteElements *ZSet
}

func (r *Recover) String() string {
	return fmt.Sprintf("%v: %v", r.RemoteAddr, r.RemoteElements)
}

type RecoverChan chan *Recover

var PNodeNotFound error = errors.New("Prefix-tree node not found")

type Settings interface {
	Init()
	Version() string
	HttpPort() int
	ReconPort() int
	Partners() []net.Addr
	Filters() []string
	ThreshMult() int
	GossipIntervalSecs() int
	MaxOutstandingReconRequests() int
}

type DefaultSettings struct {
	version                     string
	httpPort                    int
	reconPort                   int
	partners                    []net.Addr
	filters                     []string
	threshMult                  int
	gossipIntervalSecs          int
	maxOutstandingReconRequests int
}

func (s *DefaultSettings) Version() string                  { return s.version }
func (s *DefaultSettings) HttpPort() int                    { return s.httpPort }
func (s *DefaultSettings) ReconPort() int                   { return s.reconPort }
func (s *DefaultSettings) Partners() []net.Addr             { return s.partners }
func (s *DefaultSettings) Filters() []string                { return s.filters }
func (s *DefaultSettings) ThreshMult() int                  { return s.threshMult }
func (s *DefaultSettings) GossipIntervalSecs() int          { return s.gossipIntervalSecs }
func (s *DefaultSettings) MaxOutstandingReconRequests() int { return s.maxOutstandingReconRequests }

func (s *DefaultSettings) Init() {
	s.version = "experimental"
	s.reconPort = 11370
	s.httpPort = 11371
	s.threshMult = DefaultThreshMult
	s.gossipIntervalSecs = 60
	s.maxOutstandingReconRequests = 100
}

type serverStop chan interface{}

type gossipEnable chan bool

type Peer struct {
	Settings
	PrefixTree
	RecoverChan  RecoverChan
	stop         serverStop
	gossipEnable gossipEnable
}

func NewMemPeer() *Peer {
	settings := new(DefaultSettings)
	settings.Init()
	tree := new(MemPrefixTree)
	tree.Init()
	peer := &Peer{
		RecoverChan: make(RecoverChan),
		Settings:    settings,
		PrefixTree:  tree}
	return peer
}

func (p *Peer) Start() {
	p.stop = make(serverStop)
	p.gossipEnable = make(gossipEnable)
	go p.Serve()
	go p.Gossip()
}

func (p *Peer) Stop() {
	close(p.gossipEnable)
	close(p.stop)
}

func (p *Peer) Serve() {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", p.ReconPort()))
	if err != nil {
		log.Print(err)
		return
	}
	for {
		select {
		case _, isOpen := <-p.stop:
			if !isOpen {
				return
			}
		default:
		}
		conn, err := ln.Accept()
		if err != nil {
			log.Print(err)
			continue
		}
		log.Println(SERVE, "connection from:", conn.RemoteAddr())
		config := &Config{Contents: map[string]string{"foo": "bar"}}
		WriteMsg(conn, config)
		log.Println(SERVE, "sent config")
		err = p.interactWithClient(conn, NewBitstring(0))
		if err != nil {
			log.Println(err)
		}
	}
}

type requestEntry struct {
	node PrefixNode
	key  *Bitstring
}

func (r *requestEntry) String() string {
	return fmt.Sprintf("Request entry key=%v", r.key)
}

type bottomEntry struct {
	*requestEntry
	state reconState
}

func (r *bottomEntry) String() string {
	if r == nil {
		return "nil"
	}
	return fmt.Sprintf("Bottom entry key=%v state=%v", r.key, r.state)
}

type reconState uint8

const (
	reconStateFlushEnded = reconState(iota)
	reconStateBottom     = reconState(iota)
)

func (rs reconState) String() string {
	switch rs {
	case reconStateFlushEnded:
		return "Flush Ended"
	case reconStateBottom:
		return "Bottom"
	}
	return "Unknown"
}

type reconWithClient struct {
	requestQ []*requestEntry
	bottomQ  []*bottomEntry
	rcvrSet  *ZSet
	flushing bool
	conn     net.Conn
}

func (rwc *reconWithClient) pushBottom(bottom *bottomEntry) {
	rwc.bottomQ = append(rwc.bottomQ, bottom)
}

func (rwc *reconWithClient) pushRequest(req *requestEntry) {
	rwc.requestQ = append(rwc.requestQ, req)
}

func (rwc *reconWithClient) topBottom() *bottomEntry {
	if len(rwc.bottomQ) == 0 {
		return nil
	}
	return rwc.bottomQ[0]
}

func (rwc *reconWithClient) popBottom() *bottomEntry {
	if len(rwc.bottomQ) == 0 {
		return nil
	}
	result := rwc.bottomQ[0]
	rwc.bottomQ = rwc.bottomQ[1:]
	return result
}

func (rwc *reconWithClient) popRequest() *requestEntry {
	if len(rwc.requestQ) == 0 {
		return nil
	}
	result := rwc.requestQ[0]
	rwc.requestQ = rwc.requestQ[1:]
	return result
}

func (rwc *reconWithClient) isDone() bool {
	return len(rwc.requestQ) == 0 && len(rwc.bottomQ) == 0
}

// TODO: need to send error back on chan as well
func readAllMsgs(r io.Reader) chan ReconMsg {
	c := make(chan ReconMsg)
	go func() {
		for {
			msg, err := ReadMsg(r)
			if err != nil {
				close(c)
				return
			}
			c <- msg
		}
	}()
	return c
}

func (rwc *reconWithClient) sendRequest(p *Peer, req *requestEntry) {
	var msg ReconMsg
	if req.node.IsLeaf() || (req.node.Size() < p.MBar()) {
		msg = &ReconRqstFull{
			Prefix:   req.key,
			Elements: NewZSet(req.node.Elements()...)}
	} else {
		msg = &ReconRqstPoly{
			Prefix:  req.key,
			Size:    req.node.Size(),
			Samples: req.node.SValues()}
	}
	log.Println(SERVE, "sendRequest:", msg)
	WriteMsg(rwc.conn, msg)
	rwc.pushBottom(&bottomEntry{requestEntry: req})
}

func (rwc *reconWithClient) handleReply(p *Peer, msg ReconMsg, req *requestEntry) (err error) {
	log.Println(SERVE, "handleReply:", "got:", msg)
	switch m := msg.(type) {
	case *SyncFail:
		if req.node.IsLeaf() {
			return errors.New("Syncfail received at leaf node")
		}
		var node PrefixNode
		node, err = p.Node(req.key)
		if err != nil {
			return
		}
		for _, childNode := range node.Children() {
			if err != nil {
				return
			}
			rwc.pushRequest(&requestEntry{key: childNode.Key(), node: childNode})
		}
	case *Elements:
		rwc.rcvrSet.AddAll(m.ZSet)
	case *FullElements:
		local := NewZSet(req.node.Elements()...)
		localdiff := ZSetDiff(local, m.ZSet)
		remotediff := ZSetDiff(m.ZSet, local)
		elementsMsg := &Elements{ZSet: localdiff}
		log.Println(SERVE, "handleReply:", "sending:", elementsMsg)
		WriteMsg(rwc.conn, elementsMsg)
		rwc.rcvrSet.AddAll(remotediff)
	default:
		err = errors.New(fmt.Sprintf("unexpected message: %v", m))
	}
	return
}

func (rwc *reconWithClient) flushQueue() {
	rwc.pushBottom(&bottomEntry{state: reconStateFlushEnded})
	rwc.flushing = true
}

func (p *Peer) interactWithClient(conn net.Conn, bitstring *Bitstring) (err error) {
	log.Println(SERVE, "interacting with client")
	recon := reconWithClient{conn: conn, rcvrSet: NewZSet()}
	var root PrefixNode
	root, err = p.Root()
	if err != nil {
		return
	}
	recon.pushRequest(&requestEntry{node: root, key: bitstring})
	msgChan := readAllMsgs(conn)
	for !recon.isDone() {
		bottom := recon.topBottom()
		log.Println(SERVE, "interact: bottom:", bottom)
		switch {
		case bottom == nil:
			req := recon.popRequest()
			log.Println(SERVE, "interact: popRequest:", req)
			recon.sendRequest(p, req)
		case bottom.state == reconStateFlushEnded:
			recon.popBottom()
			recon.flushing = false
		case bottom.state == reconStateBottom:
			log.Println("Queue length:", len(recon.bottomQ))
			var msg ReconMsg
			hasMsg := false
			select {
			case msg = <-msgChan:
				hasMsg = true
			}
			if hasMsg {
				recon.popBottom()
				err = recon.handleReply(p, msg, bottom.requestEntry)
			} else if len(recon.bottomQ) > p.MaxOutstandingReconRequests() ||
				len(recon.requestQ) == 0 {
				if !recon.flushing {
					recon.flushQueue()
				} else {
					recon.popBottom()
					msg = <-msgChan
					err = recon.handleReply(p, msg, bottom.requestEntry)
				}
			} else {
				req := recon.popRequest()
				recon.sendRequest(p, req)
			}
		}
		if err != nil {
			return
		}
	}
	msg := &Done{}
	WriteMsg(conn, msg)
	if recon.rcvrSet.Len() > 0 {
		p.RecoverChan <- &Recover{
			RemoteAddr:     conn.RemoteAddr(),
			RemoteElements: recon.rcvrSet}
	}
	return
}

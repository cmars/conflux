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
	"errors"
	"fmt"
	. "github.com/cmars/conflux"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
)

type Response interface {
	Error() error
	WriteTo(w io.Writer)
}

type Recover struct {
	RemoteAddr     net.Addr
	RemoteElements *ZSet
}

type RecoverChan chan *Recover

type PTree interface {
	Root() PNode
	// Interpolation sample points
	Points() []*Zp
	// Get the node for specified key
	GetNode([]byte) (PNode, error)
	// Get the child keys of given key
	ChildKeys([]byte) [][]byte
	// Insert an integer
	Insert(*Zp)
	// Delete an integer
	Delete(*Zp)
}

type PNode interface {
	SValues() []*Zp
	Size() int
	IsLeaf() bool
	Elements() *ZSet
}

var PNodeNotFound error = errors.New("Prefix-tree node not found")

type ReconConfig interface {
	Version() string
	HttpPort() int
	BitQuantum() int
	MBar() int
	Filters() []string
	ReconThreshMult() int
	GossipIntervalSecs() int
	MaxOutstandingReconRequests() int
}

type serverStop chan interface{}

type gossipEnable chan bool

type Peer struct {
	Port         int
	RecoverChan  RecoverChan
	Tree         PTree
	Settings     ReconConfig
	l            *log.Logger
	stop         serverStop
	gossipEnable gossipEnable
}

func (p *Peer) Start() {
	if p.l == nil {
		p.l = log.New(os.Stderr, fmt.Sprintf("[%s]", filepath.Base(os.Args[0])),
			log.LstdFlags|log.Lshortfile)
	}
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
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", p.Port))
	if err != nil {
		p.l.Print(err)
		return
	}
	for {
		select {
		case _, isOpen := <-p.stop:
			if !isOpen {
				return
			}
		}
		conn, err := ln.Accept()
		if err != nil {
			p.l.Print(err)
			continue
		}
		err = p.interactWithClient(conn, make([]byte, 0))
		if err != nil {
			p.l.Print(err)
		}
	}
}

type requestEntry struct {
	node PNode
	key  []byte
}

type bottomEntry struct {
	*requestEntry
	state reconState
}

type reconState uint8

const (
	reconStateFlushEnded = reconState(iota)
	reconStateBottom     = reconState(iota)
)

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
	if req.node.IsLeaf() || (req.node.Size() < p.Settings.MBar()) {
		msg = &ReconRqstFull{
			Prefix:   req.key,
			Elements: req.node.Elements()}
	} else {
		msg = &ReconRqstPoly{
			Prefix:  req.key,
			Size:    req.node.Size(),
			Samples: req.node.SValues()}
	}
	msg.marshal(rwc.conn)
	rwc.pushBottom(&bottomEntry{requestEntry: req})
}

func (rwc *reconWithClient) handleReply(p *Peer, msg ReconMsg, req *requestEntry) (err error) {
	switch m := msg.(type) {
	case *SyncFail:
		if req.node.IsLeaf() {
			return errors.New("Syncfail received at leaf node")
		}
		children := p.Tree.ChildKeys(req.key)
		var node PNode
		for _, key := range children {
			node, err = p.Tree.GetNode(key)
			if err != nil {
				return
			}
			rwc.pushRequest(&requestEntry{key: key, node: node})
		}
	case *Elements:
		rwc.rcvrSet.AddAll(m.ZSet)
	case *FullElements:
		local := req.node.Elements()
		localdiff := ZSetDiff(local, m.ZSet)
		remotediff := ZSetDiff(m.ZSet, local)
		(&Elements{ZSet: localdiff}).marshal(rwc.conn)
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

func (p *Peer) interactWithClient(conn net.Conn, bitstring []byte) (err error) {
	recon := reconWithClient{conn: conn}
	recon.pushRequest(&requestEntry{node: p.Tree.Root(), key: bitstring})
	msgChan := readAllMsgs(conn)
	for !recon.isDone() {
		bottom := recon.topBottom()
		switch {
		case bottom == nil:
			req := recon.popRequest()
			recon.sendRequest(p, req)
		case bottom.state == reconStateFlushEnded:
			recon.popBottom()
			recon.flushing = false
		case bottom.state == reconStateBottom:
			p.l.Print("Queue length:", len(recon.bottomQ))
			var msg ReconMsg
			hasMsg := false
			select {
			case msg = <-msgChan:
				hasMsg = true
			}
			if hasMsg {
				recon.popBottom()
				err = recon.handleReply(p, msg, bottom.requestEntry)
			} else if len(recon.bottomQ) > p.Settings.MaxOutstandingReconRequests() ||
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
	(&Done{}).marshal(conn)
	p.RecoverChan <- &Recover{
		RemoteAddr:     conn.RemoteAddr(),
		RemoteElements: recon.rcvrSet}
	return
}

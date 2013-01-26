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
	"net"
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
	Points() []*Zp
	GetNodeKey([]byte) (PNode, error)
	NumElements(PNode) int
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
}

type serverStop chan interface{}

type gossipEnable chan bool

type Peer struct {
	Port         int
	RecoverChan  RecoverChan
	Tree         PTree
	Settings     ReconConfig
	stop         serverStop
	gossipEnable gossipEnable
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
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", p.Port))
	if err != nil {
		// TODO: log error
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
			// TODO: log error
			continue
		}
		err = p.interactWithClient(conn, make([]byte, 0))
		if err != nil {
			// TODO: log error
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
	panic("not impl")
}

func (rwc *reconWithClient) handleReply(p *Peer, msg ReconMsg, bottom *bottomEntry) {
	panic("not impl")
}

func (p *Peer) interactWithClient(conn net.Conn, bitstring []byte) error {
	//var flushing bool
	recon := reconWithClient{}
	msgChan := readAllMsgs(conn)
	for !recon.isDone() {
		bottom := recon.topBottom()
		switch {
		case bottom == nil:
			req := recon.popRequest()
			recon.sendRequest(p, req)
		case bottom.state == reconStateFlushEnded:
			recon.popBottom()
			//flushing = false
		case bottom.state == reconStateBottom:
			// TODO: log queue length
			msg, has := <-msgChan
			if has {
				recon.popBottom()
				recon.handleReply(p, msg, bottom)
			} else {
				panic("not impl")
			}
		}
	}
	(&Done{}).marshal(conn)
	p.RecoverChan <- &Recover{
		RemoteAddr:     conn.RemoteAddr(),
		RemoteElements: recon.rcvrSet}
	return nil
}

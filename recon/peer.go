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

type gossipControl struct {
	enable bool
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
		/*conn*/ _, err := ln.Accept()
		if err != nil {
			// TODO: log error
			continue
		}
		// TODO: handle incoming connections
		panic("not impl")
	}
}

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

type Recover struct {
	RemoteAddr     net.Addr
	RemoteConfig   *Config
	RemoteElements []*Zp
}

func (r *Recover) String() string {
	return fmt.Sprintf("%v: %v", r.RemoteAddr, r.RemoteElements)
}

type RecoverChan chan *Recover

var PNodeNotFound error = errors.New("Prefix-tree node not found")

var RemoteRejectConfigError error = errors.New("Remote rejected configuration")

type serverEnable chan bool
type gossipEnable chan bool
type stopped chan interface{}

type reconCmd func() error

type reconCmdReq chan reconCmd
type reconCmdResp chan error

type Peer struct {
	*Settings
	PrefixTree
	RecoverChan  RecoverChan
	reconCmdReq  reconCmdReq
	reconCmdResp reconCmdResp
	serverEnable serverEnable
	gossipEnable gossipEnable
	stopped      stopped
}

func NewPeer(settings *Settings, tree PrefixTree) *Peer {
	return &Peer{
		RecoverChan: make(RecoverChan),
		Settings:    settings,
		PrefixTree:  tree}
}

func NewMemPeer() *Peer {
	settings := NewSettings()
	tree := new(MemPrefixTree)
	tree.Init()
	return NewPeer(settings, tree)
}

func (p *Peer) Start() {
	p.serverEnable = make(serverEnable)
	p.gossipEnable = make(gossipEnable)
	p.stopped = make(stopped)
	p.reconCmdReq = make(reconCmdReq)
	p.reconCmdResp = make(reconCmdResp)
	go p.Serve()
	go p.Gossip()
	go p.handleCmds()
}

func (p *Peer) Stop() {
	if p.serverEnable == nil {
		log.Println(SERVE, "Stop: peer not running")
		return
	}
	log.Println(SERVE, "Stopping")
	go func() { p.serverEnable <- false }()
	go func() { p.gossipEnable <- false }()
	// Drain recovery channel
	go func() {
		for _ = range p.RecoverChan {
		}
	}()
	// Acknowledged stop of server & gossip client
	<-p.stopped
	<-p.stopped
	// Close channels
	close(p.stopped)
	close(p.reconCmdReq)
	close(p.reconCmdResp)
	close(p.RecoverChan)
	// Re-init
	p.serverEnable = nil
	p.gossipEnable = nil
	p.stopped = nil
	p.reconCmdReq = nil
	p.reconCmdResp = nil
	p.RecoverChan = nil
	log.Println(SERVE, "Stopped")
}

// handleCmds executes recon cmds in a single goroutine.
// This forces sequential reads and writes to the prefix
// tree.
func (p *Peer) handleCmds() {
	for {
		select {
		case cmd, ok := <-p.reconCmdReq:
			if !ok {
				return
			}
			p.reconCmdResp <- cmd()
		}
	}
}

func (p *Peer) ExecCmd(cmd reconCmd) (err error) {
	p.reconCmdReq <- cmd
	err = <-p.reconCmdResp
	if err != nil {
		log.Println("CMD", err)
	}
	return
}

func (p *Peer) Insert(z *Zp) (err error) {
	return p.ExecCmd(func() error {
		return p.PrefixTree.Insert(z)
	})
}

func (p *Peer) Remove(z *Zp) (err error) {
	return p.ExecCmd(func() error {
		return p.PrefixTree.Remove(z)
	})
}

func (p *Peer) Serve() {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", p.ReconPort))
	if err != nil {
		log.Print(err)
		return
	}
	defer ln.Close()
	for {
		select {
		case enabled, isOpen := <-p.serverEnable:
			if !enabled || !isOpen {
				close(p.serverEnable)
				p.stopped <- true
				return
			}
		default:
		}
		conn, err := ln.Accept()
		if err != nil {
			log.Println(SERVE, err)
			continue
		}
		err = p.accept(conn)
		if err != nil {
			log.Println(SERVE, err)
		}
	}
}

func (p *Peer) handleConfig(conn net.Conn, role string) (remoteConfig *Config, err error) {
	// Send config to server on connect
	log.Println(role, "writing config:", p.Config())
	err = WriteMsg(conn, p.Config())
	if err != nil {
		return
	}
	// Receive remote peer's config
	log.Println(role, "reading remote config:", conn.RemoteAddr())
	var msg ReconMsg
	msg, err = ReadMsg(conn)
	if err != nil {
		return
	}
	var is bool
	remoteConfig, is = msg.(*Config)
	if !is {
		err = errors.New(fmt.Sprintf(
			"Remote config: expected config message, got %v", msg))
		return
	}
	log.Println(role, "remote config:", remoteConfig)
	if remoteConfig.BitQuantum != p.Config().BitQuantum {
		WriteString(conn, RemoteConfigFailed)
		WriteString(conn, "mismatched bitquantum")
		log.Println(role, "Cannot peer: BitQuantum remote=", remoteConfig.BitQuantum,
			"!=", p.Config().BitQuantum)
		err = IncompatiblePeerError
		return
	}
	if remoteConfig.MBar != p.Config().MBar {
		WriteString(conn, RemoteConfigFailed)
		WriteString(conn, "mismatched mbar")
		log.Println(role, "Cannot peer: MBar remote=", remoteConfig.MBar,
			"!=", p.Config().MBar)
		err = IncompatiblePeerError
		return
	}
	err = WriteString(conn, RemoteConfigPassed)
	if err != nil {
		return
	}
	remoteConfigStatus, err := ReadString(conn)
	if remoteConfigStatus != RemoteConfigPassed {
		var reason string
		if reason, err = ReadString(conn); err == nil {
			log.Println(role, reason)
			err = RemoteRejectConfigError
		}
		return
	}
	return
}

func (p *Peer) accept(conn net.Conn) error {
	defer conn.Close()
	log.Println(SERVE, "connection from:", conn.RemoteAddr())
	remoteConfig, err := p.handleConfig(conn, SERVE)
	if err != nil {
		return err
	}
	return p.ExecCmd(func() error {
		return p.interactWithClient(conn, remoteConfig, NewBitstring(0))
	})
}

type requestEntry struct {
	node PrefixNode
	key  *Bitstring
}

func (r *requestEntry) String() string {
	if r == nil {
		return "nil"
	}
	return fmt.Sprintf("Request entry key=%v", r.key)
}

type bottomEntry struct {
	*requestEntry
	state reconState
}

func (r *bottomEntry) String() string {
	if r == nil {
		return "nil"
	} else if r.requestEntry == nil {
		return fmt.Sprintf("Bottom entry req=nil state=%v", r.state)
	}
	return fmt.Sprintf("Bottom entry key=%v state=%v", r.key, r.state)
}

type reconState uint8

const (
	reconStateBottom     = reconState(iota)
	reconStateFlushEnded = reconState(iota)
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
	*Peer
	requestQ []*requestEntry
	bottomQ  []*bottomEntry
	rcvrSet  *ZSet
	flushing bool
	conn     net.Conn
	messages []ReconMsg
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
			log.Println(SERVE, "ReadMsg:", msg)
			c <- msg
		}
	}()
	return c
}

func (rwc *reconWithClient) sendRequest(p *Peer, req *requestEntry) {
	var msg ReconMsg
	if req.node.IsLeaf() || (req.node.Size() < p.MBar) {
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
	rwc.messages = append(rwc.messages, msg)
	rwc.pushBottom(&bottomEntry{requestEntry: req})
}

func (rwc *reconWithClient) handleReply(p *Peer, msg ReconMsg, req *requestEntry) (err error) {
	log.Println(SERVE, "handleReply:", "got:", msg)
	switch m := msg.(type) {
	case *SyncFail:
		if req.node.IsLeaf() {
			return errors.New("Syncfail received at leaf node")
		}
		log.Println(SERVE, "SyncFail: pushing children")
		for _, childNode := range req.node.Children() {
			log.Println(SERVE, "push:", childNode.Key())
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
		rwc.messages = append(rwc.messages, elementsMsg)
		rwc.rcvrSet.AddAll(remotediff)
	default:
		err = errors.New(fmt.Sprintf("unexpected message: %v", m))
	}
	return
}

func (rwc *reconWithClient) flushQueue() {
	log.Println(SERVE, "flush queue")
	rwc.messages = append(rwc.messages, &Flush{})
	for _, msg := range rwc.messages {
		err := WriteMsg(rwc.conn, msg)
		if err != nil {
			log.Println(SERVE, "Error writing Flush message:", err)
		}
	}
	rwc.messages = nil
	rwc.pushBottom(&bottomEntry{state: reconStateFlushEnded})
	rwc.flushing = true
}

func (p *Peer) interactWithClient(conn net.Conn, remoteConfig *Config, bitstring *Bitstring) (err error) {
	log.Println(SERVE, "interacting with client")
	recon := reconWithClient{Peer: p, conn: conn, rcvrSet: NewZSet()}
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
			log.Println(SERVE, "interact: popRequest:", req, "sending...")
			recon.sendRequest(p, req)
		case bottom.state == reconStateFlushEnded:
			log.Println(SERVE, "interact: flush ended, popBottom")
			recon.popBottom()
			recon.flushing = false
		case bottom.state == reconStateBottom:
			log.Println(SERVE, "Queue length:", len(recon.bottomQ))
			var msg ReconMsg
			var ok bool
			hasMsg := false
			select {
			case msg, ok = <-msgChan:
				hasMsg = ok
			default:
			}
			if hasMsg {
				recon.popBottom()
				err = recon.handleReply(p, msg, bottom.requestEntry)
			} else if len(recon.bottomQ) > p.MaxOutstandingReconRequests ||
				len(recon.requestQ) == 0 {
				if !recon.flushing {
					recon.flushQueue()
				} else {
					recon.popBottom()
					msg, ok = <-msgChan
					if ok {
						err = recon.handleReply(p, msg, bottom.requestEntry)
					}
				}
			} else {
				req := recon.popRequest()
				recon.sendRequest(p, req)
			}
		default:
			log.Println("failed to match expected patterns")
		}
		if err != nil {
			return
		}
	}
	WriteMsg(conn, &Done{})
	items := recon.rcvrSet.Items()
	if len(items) > 0 {
		p.RecoverChan <- &Recover{
			RemoteAddr:     conn.RemoteAddr(),
			RemoteConfig:   remoteConfig,
			RemoteElements: items}
	}
	return
}

/*
   conflux - Distributed database synchronization library
	Based on the algorithm described in
		"Set Reconciliation with Nearly Optimal	Communication Complexity",
			Yaron Minsky, Ari Trachtenberg, and Richard Zippel, 2004.

   Copyright (c) 2012-2015  Casey Marshall <cmars@cmarstech.com>

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
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"gopkg.in/errgo.v1"
	log "gopkg.in/hockeypuck/logrus.v0"
	"gopkg.in/tomb.v2"

	. "github.com/cmars/conflux"
)

const SERVE = "serve"

type Recover struct {
	RemoteAddr     net.Addr
	RemoteConfig   *Config
	RemoteElements []*Zp
}

func (r *Recover) String() string {
	return fmt.Sprintf("%v: %d elements", r.RemoteAddr, len(r.RemoteElements))
}

func (r *Recover) HkpAddr() (string, error) {
	// Use remote HKP host:port as peer-unique identifier
	host, _, err := net.SplitHostPort(r.RemoteAddr.String())
	if err != nil {
		log.Error("cannot parse HKP remote address from", r.RemoteAddr, ":", err)
		return "", errgo.Mask(err)
	}
	return fmt.Sprintf("%s:%d ", host, r.RemoteConfig.HTTPPort), nil
}

type RecoverChan chan *Recover

var PNodeNotFound error = errors.New("Prefix-tree node not found")

var RemoteRejectConfigError error = errors.New("Remote rejected configuration")

type PeerMode string

var (
	PeerModeDefault    = PeerMode("")
	PeerModeGossipOnly = PeerMode("gossip only")
	PeerModeServeOnly  = PeerMode("serve only")
)

type Peer struct {
	settings *Settings
	ptree    PrefixTree

	RecoverChan RecoverChan

	t tomb.Tomb

	enableLock sync.Mutex
	enable     bool

	tracker Tracker
}

func NewPeer(settings *Settings, tree PrefixTree) *Peer {
	return &Peer{
		RecoverChan: make(RecoverChan),
		settings:    settings,
		ptree:       tree,
	}
}

func NewPeerState(settings *Settings, tree PrefixTree, state State) *Peer {
	peer := NewPeer(settings, tree)
	peer.tracker.Begin(state)
	return peer
}

func NewMemPeer() *Peer {
	settings := DefaultSettings()
	tree := new(MemPrefixTree)
	tree.Init()
	return NewPeer(settings, tree)
}

func (p *Peer) logName(label string) string {
	return fmt.Sprintf("%s %s ", label, p.settings.ReconAddr)
}

func (p *Peer) Start() {
	p.StartMode(PeerModeDefault)
}

func (p *Peer) StartMode(mode PeerMode) {
	switch mode {
	case PeerModeGossipOnly:
		p.t.Go(p.Gossip)
	case PeerModeServeOnly:
		p.t.Go(p.Serve)
	default:
		p.t.Go(p.Serve)
		p.t.Go(p.Gossip)
	}
	p.Enable()
}

type stopNotify chan interface{}

func (p *Peer) Stop() error {
	p.t.Kill(nil)
	return p.t.Wait()
}

func (p *Peer) Enabled() bool {
	p.enableLock.Lock()
	defer p.enableLock.Unlock()
	return p.enable
}

func (p *Peer) Enable() {
	p.enableLock.Lock()
	defer p.enableLock.Unlock()
	p.enable = true
}

func (p *Peer) Disable() {
	p.enableLock.Lock()
	defer p.enableLock.Unlock()
	p.enable = false
}

func (p *Peer) ExecCmd(f func() error) {
	p.tracker.ExecIdle(f)
}

func (p *Peer) Insert(zs ...*Zp) {
	p.tracker.ExecIdle(func() error {
		for _, z := range zs {
			err := p.ptree.Insert(z)
			if err != nil {
				return errgo.Mask(err)
			}
		}
		return nil
	})
}

func (p *Peer) Remove(zs ...*Zp) {
	p.tracker.ExecIdle(func() error {
		for _, z := range zs {
			err := p.ptree.Remove(z)
			if err != nil {
				return errgo.Mask(err)
			}
		}
		return nil
	})
}

func (p *Peer) Serve() error {
	addr, err := p.settings.ReconNet.Resolve(p.settings.ReconAddr)
	if err != nil {
		return errgo.Mask(err)
	}
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		return errgo.Mask(err)
	}
	p.t.Go(func() error {
		<-p.t.Dying()
		return ln.Close()
	})
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Error(p.logName(SERVE), err)
			return err
		}
		if p.settings.ReadTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(
				time.Second * time.Duration(p.settings.ReadTimeout)))
		}
		go func() {
			err = p.Accept(conn)
			if err != nil {
				log.Error(SERVE, errgo.Mask(err))
			}
		}()
	}
}

func (p *Peer) handleConfig(conn net.Conn, role string) (_ *Config, _err error) {
	config, err := p.settings.Config()
	if err != nil {
		return nil, errgo.Mask(err)
	}

	var handshake tomb.Tomb
	defer func() {
		handshake.Kill(nil)
		stopErr := handshake.Wait()
		if stopErr != nil {
			stopErr = errgo.Mask(stopErr)
			log.Error(p.logName(role), stopErr)
		}

		if _err == nil {
			_err = stopErr
		}
	}()

	handshake.Go(func() error {
		<-handshake.Dying()
		return nil
	})

	// Send config to server on connect
	handshake.Go(func() error {
		log.Debug(p.logName(role), "writing config:", config)
		err := WriteMsg(conn, config)
		if err != nil {
			return errgo.Mask(err)
		}
		return nil
	})

	// Receive remote peer's config
	log.Debug(p.logName(role), "reading remote config:", conn.RemoteAddr())
	var msg ReconMsg
	msg, err = ReadMsg(conn)
	if err != nil {
		return nil, errgo.Mask(err)
	}

	remoteConfig, ok := msg.(*Config)
	if !ok {
		return nil, errgo.Newf("remote config: expected config message, got %v", msg)
	}

	log.Debug(p.logName(role), "remote config:", remoteConfig)
	if remoteConfig.BitQuantum != config.BitQuantum {
		bufw := bufio.NewWriter(conn)
		err = WriteString(bufw, RemoteConfigFailed)
		if err != nil {
			log.Errorf(p.logName(role), errgo.Details(err))
		}
		err = WriteString(bufw, "mismatched bitquantum")
		if err != nil {
			log.Errorf(p.logName(role), errgo.Details(err))
		}

		bufw.Flush()
		log.Errorf(p.logName(role), "cannot peer: BitQuantum remote=%v != local=%v",
			remoteConfig.BitQuantum, config.BitQuantum)
		return nil, errgo.Mask(IncompatiblePeerError)
	}

	if remoteConfig.MBar != config.MBar {
		bufw := bufio.NewWriter(conn)
		err = WriteString(bufw, RemoteConfigFailed)
		if err != nil {
			log.Errorf(p.logName(role), errgo.Details(err))
		}
		err = WriteString(bufw, "mismatched mbar")
		if err != nil {
			log.Errorf(p.logName(role), errgo.Details(err))
		}

		bufw.Flush()
		log.Errorf(p.logName(role), "cannot peer: MBar remote=%v != local %v",
			remoteConfig.MBar, config.MBar)
		return nil, errgo.Mask(IncompatiblePeerError)
	}

	handshake.Go(func() error {
		bufw := bufio.NewWriter(conn)
		err = WriteString(bufw, RemoteConfigPassed)
		if err != nil {
			return errgo.Mask(err)
		}
		err = bufw.Flush()
		if err != nil {
			return errgo.Mask(err)
		}
		return nil
	})

	remoteConfigStatus, err := ReadString(conn)
	if err != nil {
		return nil, errgo.Mask(err)
	}

	if remoteConfigStatus != RemoteConfigPassed {
		reason, err := ReadString(conn)
		if err != nil {
			return nil, errgo.WithCausef(err, RemoteRejectConfigError, "remote rejected config")
		}
		return nil, errgo.NoteMask(RemoteRejectConfigError, reason)
	}

	return remoteConfig, nil
}

func (p *Peer) Accept(conn net.Conn) (_err error) {
	defer conn.Close()

	state, ok := p.tracker.Begin(StateServing)
	if !ok {
		return errgo.Notef(ErrPeerBusy, "service unavailable, currently %s", state)
	}
	defer p.tracker.Done()

	log.Debug(p.logName(SERVE), "connection from:", conn.RemoteAddr())
	defer func() {
		if _err != nil {
			log.Error(p.logName(SERVE), errgo.Details(_err))
		}
	}()

	remoteConfig, err := p.handleConfig(conn, SERVE)
	if err != nil {
		return errgo.Mask(err)
	}

	if p.Enabled() {
		return p.interactWithClient(conn, remoteConfig, NewBitstring(0))
	}
	return errgo.Newf("peer is currently disabled, ignoring connection.")
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

func (rwc *reconWithClient) sendRequest(p *Peer, req *requestEntry) error {
	var msg ReconMsg
	if req.node.IsLeaf() || (req.node.Size() < p.settings.MBar) {
		elements, err := req.node.Elements()
		if err != nil {
			return err
		}
		msg = &ReconRqstFull{
			Prefix:   req.key,
			Elements: NewZSet(elements...)}
	} else {
		msg = &ReconRqstPoly{
			Prefix:  req.key,
			Size:    req.node.Size(),
			Samples: req.node.SValues()}
	}
	log.Debug(p.logName(SERVE), "sendRequest:", msg)
	rwc.messages = append(rwc.messages, msg)
	rwc.pushBottom(&bottomEntry{requestEntry: req})
	return nil
}

func (rwc *reconWithClient) handleReply(p *Peer, msg ReconMsg, req *requestEntry) error {
	log.Debug(p.logName(SERVE), "handleReply:", "got:", msg)
	switch m := msg.(type) {
	case *SyncFail:
		if req.node.IsLeaf() {
			return errgo.New("Syncfail received at leaf node")
		}
		log.Debug(rwc.Peer.logName(SERVE), "SyncFail: pushing children")
		children, err := req.node.Children()
		if err != nil {
			return err
		}
		for _, childNode := range children {
			log.Debug(rwc.Peer.logName(SERVE), "push:", childNode.Key())
			rwc.pushRequest(&requestEntry{key: childNode.Key(), node: childNode})
		}
	case *Elements:
		rwc.rcvrSet.AddAll(m.ZSet)
	case *FullElements:
		elements, err := req.node.Elements()
		if err != nil {
			return err
		}
		local := NewZSet(elements...)
		localNeeds := ZSetDiff(m.ZSet, local)
		remoteNeeds := ZSetDiff(local, m.ZSet)
		elementsMsg := &Elements{ZSet: remoteNeeds}
		log.Debug(rwc.Peer.logName(SERVE), "handleReply:", "sending:", elementsMsg)
		rwc.messages = append(rwc.messages, elementsMsg)
		rwc.rcvrSet.AddAll(localNeeds)
	default:
		return errgo.Newf("unexpected message: %v", m)
	}
	return nil
}

func (rwc *reconWithClient) flushQueue() error {
	log.Println(SERVE, "flush queue")
	rwc.messages = append(rwc.messages, &Flush{})
	err := WriteMsg(rwc.conn, rwc.messages...)
	if err != nil {
		return errgo.NoteMask(err, "error writing messages")
	}
	rwc.messages = nil
	rwc.pushBottom(&bottomEntry{state: reconStateFlushEnded})
	rwc.flushing = true
	return nil
}

var zeroTime time.Time

func (p *Peer) interactWithClient(conn net.Conn, remoteConfig *Config, bitstring *Bitstring) error {
	log.Debug(p.logName(SERVE), "interacting with client")
	recon := reconWithClient{Peer: p, conn: conn, rcvrSet: NewZSet()}
	root, err := p.ptree.Root()
	if err != nil {
		return err
	}
	recon.pushRequest(&requestEntry{node: root, key: bitstring})
	for !recon.isDone() {
		bottom := recon.topBottom()
		log.Debug(p.logName(SERVE), "interact: bottom:", bottom)
		switch {
		case bottom == nil:
			req := recon.popRequest()
			log.Debug(p.logName(SERVE), "interact: popRequest:", req, "sending...")
			err = recon.sendRequest(p, req)
			if err != nil {
				return err
			}
		case bottom.state == reconStateFlushEnded:
			log.Debug(p.logName(SERVE), "interact: flush ended, popBottom")
			recon.popBottom()
			recon.flushing = false
		case bottom.state == reconStateBottom:
			log.Debug(p.logName(SERVE), "queue length:", len(recon.bottomQ))
			var msg ReconMsg
			var hasMsg bool

			// Set a small read timeout to simulate non-blocking I/O
			err = conn.SetReadDeadline(time.Now().Add(time.Millisecond))
			if err != nil {
				return errgo.Mask(err)
			}
			msg, nbErr := ReadMsg(conn)
			hasMsg = (nbErr == nil)

			// Restore blocking I/O
			err = conn.SetReadDeadline(zeroTime)
			if err != nil {
				return errgo.Mask(err)
			}

			if hasMsg {
				recon.popBottom()
				err = recon.handleReply(p, msg, bottom.requestEntry)
				if err != nil {
					return errgo.Mask(err)
				}
			} else if len(recon.bottomQ) > p.settings.MaxOutstandingReconRequests ||
				len(recon.requestQ) == 0 {
				if !recon.flushing {
					err = recon.flushQueue()
					if err != nil {
						return errgo.Mask(err)
					}
				} else {
					recon.popBottom()
					msg, err = ReadMsg(conn)
					if err != nil {
						return errgo.Mask(err)
					}
					log.Debug("reply:", msg)
					err = recon.handleReply(p, msg, bottom.requestEntry)
					if err != nil {
						return errgo.Mask(err)
					}
				}
			} else {
				req := recon.popRequest()
				err = recon.sendRequest(p, req)
				if err != nil {
					return err
				}
			}
		default:
			return errgo.New("failed to match expected patterns")
		}
	}
	err = WriteMsg(conn, &Done{})
	if err != nil {
		return errgo.Mask(err)
	}

	items := recon.rcvrSet.Items()
	if len(items) > 0 && p.t.Alive() {
		p.RecoverChan <- &Recover{
			RemoteAddr:     conn.RemoteAddr(),
			RemoteConfig:   remoteConfig,
			RemoteElements: items}
	}
	return nil
}

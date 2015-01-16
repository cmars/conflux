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
	"math/rand"
	"net"
	"time"

	"gopkg.in/errgo.v1"
	log "gopkg.in/hockeypuck/logrus.v0"

	. "github.com/cmars/conflux"
)

const GOSSIP = "gossip"

// Gossip with remote servers, acting as a client.
func (p *Peer) Gossip() error {
	rand.Seed(time.Now().UnixNano())
	delay := time.Second * time.Duration(rand.Intn(p.settings.GossipIntervalSecs))
	for {
		select {
		case <-p.t.Dying():
			return nil
		case <-time.After(delay):
			var (
				err  error
				peer net.Addr
			)

			if !p.Enabled() {
				log.Println("Peer currently disabled.")
				goto DELAY
			}

			peer, err = p.choosePartner()
			if err != nil {
				if errgo.Cause(err) == NoPartnersError {
					log.Debug(p.logName(GOSSIP), "no partners to gossip with")
				} else {
					log.Error(p.logName(GOSSIP), "choosePartner:", errgo.Details(err))
				}
				goto DELAY
			}

			err = p.InitiateRecon(peer)
			if err != nil && errgo.Cause(err) != ErrPeerBusy {
				log.Error(p.logName(GOSSIP), "recon failed:", errgo.Details(err))
			}
		DELAY:
			delay = time.Second * time.Duration(rand.Intn(p.settings.GossipIntervalSecs))
			log.Debugf("waiting %s for next gossip attempt", delay)
		}
	}
}

var NoPartnersError error = errors.New("no recon partners configured")
var IncompatiblePeerError error = errors.New("remote peer configuration is not compatible")
var ErrPeerBusy error = errors.New("peer is busy handling another request")

func (p *Peer) choosePartner() (net.Addr, error) {
	partners, err := p.settings.PartnerAddrs()
	if err != nil {
		return nil, errgo.Mask(err)
	}
	if len(partners) == 0 {
		return nil, NoPartnersError
	}
	return partners[rand.Intn(len(partners))], nil
}

func (p *Peer) InitiateRecon(addr net.Addr) error {
	state, ok := p.tracker.Begin(StateGossipping)
	if !ok {
		return errgo.Notef(ErrPeerBusy, "cannot gossip, currently %s", state)
	}
	defer p.tracker.Done()

	log.Debug(p.logName(GOSSIP), "initiating recon with peer", addr)
	conn, err := net.DialTimeout(addr.Network(), addr.String(), 3*time.Second)
	if err != nil {
		log.Debug(p.logName(GOSSIP), "error connecting to", addr, ":", err)
		return err
	}
	defer conn.Close()

	if p.settings.ReadTimeout > 0 {
		err = conn.SetReadDeadline(
			time.Now().Add(time.Second * time.Duration(p.settings.ReadTimeout)))
		if err != nil {
			log.Warn(p.logName(GOSSIP), "cannot set read timeout: %v", errgo.Details(err))
		}
	}
	remoteConfig, err := p.handleConfig(conn, GOSSIP)
	if err != nil {
		return errgo.Mask(err)
	}

	// Interact with peer
	return p.clientRecon(conn, remoteConfig)
}

type msgProgress struct {
	elements *ZSet
	err      error
	flush    bool
	messages []ReconMsg
}

func (mp *msgProgress) String() string {
	if mp.err != nil {
		return fmt.Sprintf("err=%v", mp.err)
	}
	return fmt.Sprintf("nelements=%d flush=%v messages=%+v",
		mp.elements.Len(), mp.flush, msgTypes(mp.messages))
}

func msgTypes(messages []ReconMsg) []string {
	var result []string
	for _, msg := range messages {
		result = append(result, msg.MsgType().String())
	}
	return result
}

type msgProgressChan chan *msgProgress

var ReconDone = errors.New("reconciliation done")

func (p *Peer) clientRecon(conn net.Conn, remoteConfig *Config) error {
	respSet := NewZSet()
	var pendingMessages []ReconMsg
	for step := range p.interactWithServer(conn) {
		if step.err != nil {
			if step.err == ReconDone {
				log.Info(p.logName(GOSSIP), "reconcilation done")
				break
			} else {
				err := WriteMsg(conn, &Error{&textMsg{Text: step.err.Error()}})
				if err != nil {
					log.Error(p.logName(GOSSIP), errgo.Details(err))
				}
				log.Error(p.logName(GOSSIP), errgo.Details(step.err))
				break
			}
		} else {
			pendingMessages = append(pendingMessages, step.messages...)
			if step.flush {
				for _, msg := range pendingMessages {
					err := WriteMsg(conn, msg)
					if err != nil {
						return errgo.Mask(err)
					}
				}
				pendingMessages = nil
			}
		}
		log.Debug(GOSSIP, "add step:", step)
		respSet.AddAll(step.elements)
		log.Info(GOSSIP, "recover set now:", respSet.Len(), "elements")
	}
	items := respSet.Items()
	if len(items) > 0 {
		log.Info(GOSSIP, "sending recover:", len(items), "items")
		p.RecoverChan <- &Recover{
			RemoteAddr:     conn.RemoteAddr(),
			RemoteConfig:   remoteConfig,
			RemoteElements: items}
	}
	return nil
}

func (p *Peer) interactWithServer(conn net.Conn) msgProgressChan {
	out := make(msgProgressChan)
	go func() {
		var resp *msgProgress
		for resp == nil || resp.err == nil {
			msg, err := ReadMsg(conn)
			if err != nil {
				log.Error(p.logName(GOSSIP), "interact: msg err:", err)
				out <- &msgProgress{err: err}
				return
			}
			log.Debug(p.logName(GOSSIP), "interact: got msg:", msg)
			switch m := msg.(type) {
			case *ReconRqstPoly:
				resp = p.handleReconRqstPoly(m)
			case *ReconRqstFull:
				resp = p.handleReconRqstFull(m)
			case *Elements:
				log.Debug(p.logName(GOSSIP), "elements:", m.ZSet.Len())
				resp = &msgProgress{elements: m.ZSet}
			case *Done:
				resp = &msgProgress{err: ReconDone}
			case *Flush:
				resp = &msgProgress{elements: NewZSet(), flush: true}
			default:
				resp = &msgProgress{err: errgo.Newf("unexpected message: %v", m)}
			}
			out <- resp
		}
	}()
	return out
}

var ReconRqstPolyNotFound = errgo.New("peer should not receive a request for a non-existant node in ReconRqstPoly")

func (p *Peer) handleReconRqstPoly(rp *ReconRqstPoly) *msgProgress {
	remoteSize := rp.Size
	points := p.ptree.Points()
	remoteSamples := rp.Samples
	node, err := p.ptree.Node(rp.Prefix)
	if err == PNodeNotFound {
		return &msgProgress{err: ReconRqstPolyNotFound}
	}
	localSamples := node.SValues()
	localSize := node.Size()
	remoteSet, localSet, err := p.solve(
		remoteSamples, localSamples, remoteSize, localSize, points)
	if err == ErrLowMBar {
		log.Info(p.logName(GOSSIP), "low MBar")
		if node.IsLeaf() || node.Size() < (p.settings.ThreshMult*p.settings.MBar) {
			log.Info(p.logName(GOSSIP), "sending full elements for node:", node.Key())
			elements, err := node.Elements()
			if err != nil {
				return &msgProgress{err: errgo.Mask(err)}
			}
			return &msgProgress{elements: NewZSet(), messages: []ReconMsg{
				&FullElements{ZSet: NewZSet(elements...)}}}
		} else {
			err = errgo.Notef(err, "bs=%v leaf=%v size=%d", node.Key(), node.IsLeaf(), node.Size())
		}
	}
	if err != nil {
		log.Info(p.logName(GOSSIP), "sending SyncFail because", errgo.Details(err))
		return &msgProgress{elements: NewZSet(), messages: []ReconMsg{&SyncFail{}}}
	}
	log.Info(p.logName(GOSSIP), "solved: localSet=", localSet, "remoteSet=", remoteSet)
	return &msgProgress{elements: remoteSet, messages: []ReconMsg{&Elements{ZSet: localSet}}}
}

func (p *Peer) solve(remoteSamples, localSamples []*Zp, remoteSize, localSize int, points []*Zp) (*ZSet, *ZSet, error) {
	var values []*Zp
	for i, x := range remoteSamples {
		values = append(values, Z(x.P).Div(x, localSamples[i]))
	}
	log.Debug(p.logName(GOSSIP), "reconcile", values, points, remoteSize-localSize)
	return Reconcile(values, points, remoteSize-localSize)
}

func (p *Peer) handleReconRqstFull(rf *ReconRqstFull) *msgProgress {
	var localset *ZSet
	node, err := p.ptree.Node(rf.Prefix)
	if err == PNodeNotFound {
		localset = NewZSet()
	} else if err != nil {
		return &msgProgress{err: err}
	} else {
		elements, err := node.Elements()
		if err != nil {
			return &msgProgress{err: err}
		}
		localset = NewZSet(elements...)
	}
	localNeeds := ZSetDiff(rf.Elements, localset)
	remoteNeeds := ZSetDiff(localset, rf.Elements)
	log.Info(p.logName(GOSSIP), "localNeeds=(", localNeeds.Len(), ") remoteNeeds=(", remoteNeeds.Len(), ")")
	return &msgProgress{elements: localNeeds, messages: []ReconMsg{&Elements{ZSet: remoteNeeds}}}
}

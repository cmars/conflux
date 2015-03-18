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
	"math/rand"
	"net"
	"time"

	"gopkg.in/errgo.v1"
	log "gopkg.in/hockeypuck/logrus.v0"

	cf "gopkg.in/hockeypuck/conflux.v2"
)

const GOSSIP = "gossip"

// Gossip with remote servers, acting as a client.
func (p *Peer) Gossip() error {
	rand.Seed(time.Now().UnixNano())
	timer := time.NewTimer(time.Second * time.Duration(rand.Intn(p.settings.GossipIntervalSecs)))
	for {
		select {
		case <-p.t.Dying():
			return nil
		case <-timer.C:

			if p.readAcquire() {
				peer, err := p.choosePartner()
				if err != nil {
					if errgo.Cause(err) == ErrNoPartners {
						p.log(GOSSIP).Debug("no partners to gossip with")
					} else {
						p.logErr(GOSSIP, err).Error("choosePartner")
					}
				} else {
					err = p.InitiateRecon(peer)
					if errgo.Cause(err) == ErrPeerBusy {
						p.logErr(GOSSIP, err).Debug()
					} else if err != nil {
						p.logErr(GOSSIP, err).Errorf("recon with %v failed", peer)
					}
				}

				p.wg.Done()
			}

			delay := time.Second * time.Duration(rand.Intn(p.settings.GossipIntervalSecs))
			p.log(GOSSIP).Infof("waiting %s for next gossip attempt", delay)
			timer.Reset(delay)
		}
	}
}

var ErrNoPartners error = errors.New("no recon partners configured")
var ErrIncompatiblePeer error = errors.New("remote peer configuration is not compatible")
var ErrPeerBusy error = errors.New("peer is busy handling another request")
var ErrReconDone = errors.New("reconciliation done")

func IsGossipBlocked(err error) bool {
	switch err {
	case ErrNoPartners:
		return true
	case ErrIncompatiblePeer:
		return true
	case ErrPeerBusy:
		return true
	}
	return false
}

func (p *Peer) choosePartner() (net.Addr, error) {
	partners, err := p.settings.PartnerAddrs()
	if err != nil {
		return nil, errgo.Mask(err)
	}
	if len(partners) == 0 {
		return nil, errgo.Mask(ErrNoPartners, IsGossipBlocked)
	}
	return partners[rand.Intn(len(partners))], nil
}

func (p *Peer) InitiateRecon(addr net.Addr) error {
	p.log(GOSSIP).Debugf("initiating recon with peer %v", addr)
	conn, err := net.DialTimeout(addr.Network(), addr.String(), 30*time.Second)
	if err != nil {
		return errgo.Mask(err)
	}
	defer conn.Close()

	remoteConfig, err := p.handleConfig(conn, GOSSIP, "")
	if err != nil {
		return errgo.Mask(err)
	}

	// Interact with peer
	return p.clientRecon(conn, remoteConfig)
}

type msgProgress struct {
	elements *cf.ZSet
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

func (p *Peer) clientRecon(conn net.Conn, remoteConfig *Config) error {
	w := bufio.NewWriter(conn)
	respSet := cf.NewZSet()
	defer func() {
		p.sendItems(respSet.Items(), conn, remoteConfig)
	}()

	var pendingMessages []ReconMsg
	for step := range p.interactWithServer(conn) {
		if step.err != nil {
			if step.err == ErrReconDone {
				p.log(GOSSIP).Info("reconcilation done")
				break
			} else {
				err := WriteMsg(w, &Error{&textMsg{Text: step.err.Error()}})
				if err != nil {
					p.logErr(GOSSIP, err).Error()
				}
				p.logErr(GOSSIP, step.err).Error("step error")
				break
			}
		} else {
			pendingMessages = append(pendingMessages, step.messages...)
			if step.flush {
				for _, msg := range pendingMessages {
					err := WriteMsg(w, msg)
					if err != nil {
						return errgo.Mask(err)
					}
				}
				pendingMessages = nil

				err := w.Flush()
				if err != nil {
					return errgo.Mask(err)
				}
			}
		}
		p.log(GOSSIP).Debugf("add step: %v", step)
		respSet.AddAll(step.elements)
		p.log(GOSSIP).Infof("recover set now %d elements", respSet.Len())
	}
	return nil
}

func (p *Peer) interactWithServer(conn net.Conn) msgProgressChan {
	out := make(msgProgressChan)
	go func() {
		defer close(out)

		var resp *msgProgress
		var n int
		for (resp == nil || resp.err == nil) && n < maxRecoverSize {
			p.setReadDeadline(conn, defaultTimeout)
			msg, err := ReadMsg(conn)
			if err != nil {
				p.logErr(GOSSIP, err).Error("interact: read msg")
				out <- &msgProgress{err: err}
				return
			}
			p.logFields(GOSSIP, log.Fields{"msg": msg}).Debug("interact")
			switch m := msg.(type) {
			case *ReconRqstPoly:
				resp = p.handleReconRqstPoly(m)
			case *ReconRqstFull:
				resp = p.handleReconRqstFull(m)
			case *Elements:
				p.logFields(GOSSIP, log.Fields{"nelements": m.ZSet.Len()}).Debug()
				resp = &msgProgress{elements: m.ZSet}
			case *Done:
				resp = &msgProgress{err: ErrReconDone}
			case *Flush:
				resp = &msgProgress{elements: cf.NewZSet(), flush: true}
			default:
				resp = &msgProgress{err: errgo.Newf("unexpected message: %v", m)}
			}
			n += resp.elements.Len()
			out <- resp
		}
	}()
	return out
}

var ErrReconRqstPolyNotFound = errors.New(
	"peer should not receive a request for a non-existant node in ReconRqstPoly")

func (p *Peer) handleReconRqstPoly(rp *ReconRqstPoly) *msgProgress {
	remoteSize := rp.Size
	points := p.ptree.Points()
	remoteSamples := rp.Samples
	node, err := p.ptree.Node(rp.Prefix)
	if err == ErrNodeNotFound {
		return &msgProgress{err: ErrReconRqstPolyNotFound}
	}
	localSamples := node.SValues()
	localSize := node.Size()
	remoteSet, localSet, err := p.solve(
		remoteSamples, localSamples, remoteSize, localSize, points)
	if errgo.Cause(err) == cf.ErrLowMBar {
		p.log(GOSSIP).Info("ReconRqstPoly: low MBar")
		if node.IsLeaf() || node.Size() < (p.settings.ThreshMult*p.settings.MBar) {
			p.logFields(GOSSIP, log.Fields{
				"node": node.Key(),
			}).Info("sending full elements")
			elements, err := node.Elements()
			if err != nil {
				return &msgProgress{err: errgo.Mask(err)}
			}
			return &msgProgress{elements: cf.NewZSet(), messages: []ReconMsg{
				&FullElements{ZSet: cf.NewZSet(elements...)}}}
		} else {
			err = errgo.Notef(err, "bs=%v leaf=%v size=%d", node.Key(), node.IsLeaf(), node.Size())
		}
	}
	if err != nil {
		p.logErr(GOSSIP, err).Info("ReconRqstPoly: sending SyncFail")
		return &msgProgress{elements: cf.NewZSet(), messages: []ReconMsg{&SyncFail{}}}
	}
	p.logFields(GOSSIP, log.Fields{"localSet": localSet, "remoteSet": remoteSet}).Info("ReconRqstPoly: solved")
	return &msgProgress{elements: remoteSet, messages: []ReconMsg{&Elements{ZSet: localSet}}}
}

func (p *Peer) solve(remoteSamples, localSamples []*cf.Zp, remoteSize, localSize int, points []*cf.Zp) (*cf.ZSet, *cf.ZSet, error) {
	var values []*cf.Zp
	for i, x := range remoteSamples {
		values = append(values, cf.Z(x.P).Div(x, localSamples[i]))
	}
	p.logFields(GOSSIP, log.Fields{
		"values":  values,
		"points":  points,
		"degDiff": remoteSize - localSize,
	}).Debug("reconcile")
	return cf.Reconcile(values, points, remoteSize-localSize)
}

func (p *Peer) handleReconRqstFull(rf *ReconRqstFull) *msgProgress {
	var localset *cf.ZSet
	node, err := p.ptree.Node(rf.Prefix)
	if err == ErrNodeNotFound {
		localset = cf.NewZSet()
	} else if err != nil {
		return &msgProgress{err: err}
	} else {
		elements, err := node.Elements()
		if err != nil {
			return &msgProgress{err: err}
		}
		localset = cf.NewZSet(elements...)
	}
	localNeeds := cf.ZSetDiff(rf.Elements, localset)
	remoteNeeds := cf.ZSetDiff(localset, rf.Elements)
	p.logFields(GOSSIP, log.Fields{
		"localNeeds":  localNeeds.Len(),
		"remoteNeeds": remoteNeeds.Len(),
	}).Info("ReconRqstFull")
	return &msgProgress{elements: localNeeds, messages: []ReconMsg{&Elements{ZSet: remoteNeeds}}}
}

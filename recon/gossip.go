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
	"log"
	"math/rand"
	"net"
	"time"
)

const GOSSIP = "gossip:"

// Gossip with remote servers, acting as a client.
func (p *Peer) Gossip() {
	enabled := true
	var isOpen bool
	for {
		select {
		case enabled, isOpen = <-p.gossipEnable:
			log.Println(GOSSIP, "enabled:", enabled && isOpen)
			if !enabled || !isOpen {
				close(p.gossipEnable)
				p.stopped <- true
				return
			}
		default:
		}
		peer, err := p.choosePartner()
		if err != nil {
			log.Println(GOSSIP, "choosePartner:", err)
			goto DELAY
		}
		log.Println(GOSSIP, "Initiating recon with peer", peer)
		err = p.initiateRecon(peer)
		if err != nil {
			log.Println(GOSSIP, "Recon error:", err)
		}
	DELAY:
		delay := time.Duration(p.GossipIntervalSecs()) * time.Second
		// jitter the delay
		time.Sleep(delay)
	}
}

var NoPartnersError error = errors.New("That feel when no recon partner")
var IncompatiblePeerError error = errors.New("Remote peer configuration is not compatible")

func (p *Peer) choosePartner() (net.Addr, error) {
	partners, err := p.PartnerAddrs()
	if err != nil {
		return nil, err
	}
	if len(partners) == 0 {
		return nil, NoPartnersError
	}
	return partners[rand.Intn(len(partners))], nil
}

func (p *Peer) initiateRecon(peer net.Addr) error {
	// Connect to peer
	conn, err := net.DialTimeout(peer.Network(), peer.String(), time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if p.ReadTimeout() > 0 {
		conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(p.ReadTimeout())))
	}
	remoteConfig, err := p.handleConfig(conn, GOSSIP)
	// Interact with peer
	return p.ExecCmd(func() error {
		return p.clientRecon(conn, remoteConfig)
	})
}

type msgProgress struct {
	elements *ZSet
	err      error
	flush    bool
	messages []ReconMsg
}

type msgProgressChan chan *msgProgress

var ReconDone = errors.New("Reconciliation Done")

func (p *Peer) clientRecon(conn net.Conn, remoteConfig *Config) error {
	respSet := NewZSet()
	var pendingMessages []ReconMsg
	for step := range p.interactWithServer(conn) {
		if step.err != nil {
			if step.err == ReconDone {
				log.Println(GOSSIP, "Reconcilation done.")
				break
			} else {
				WriteMsg(conn, &Error{&textMsg{Text: step.err.Error()}})
				log.Println(GOSSIP, step.err)
				break
			}
		} else {
			pendingMessages = append(pendingMessages, step.messages...)
			if step.flush {
				for _, msg := range pendingMessages {
					WriteMsg(conn, msg)
				}
				pendingMessages = nil
			}
		}
		log.Println(GOSSIP, "Add step:", step)
		respSet.AddAll(step.elements)
		log.Println(GOSSIP, "Recover set now:", respSet)
	}
	items := respSet.Items()
	if len(items) > 0 {
		log.Println(GOSSIP, "Sending recover:", items)
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
				log.Println(GOSSIP, "interact: msg err:", err)
				out <- &msgProgress{err: err}
				return
			}
			log.Println(GOSSIP, "interact: got msg:", msg)
			switch m := msg.(type) {
			case *ReconRqstPoly:
				resp = p.handleReconRqstPoly(m)
			case *ReconRqstFull:
				resp = p.handleReconRqstFull(m)
			case *Elements:
				log.Println(GOSSIP, "Elements:", m.ZSet)
				resp = &msgProgress{elements: m.ZSet}
			case *Done:
				resp = &msgProgress{err: ReconDone}
			case *Flush:
				resp = &msgProgress{elements: NewZSet(), flush: true}
			default:
				resp = &msgProgress{err: errors.New(fmt.Sprintf("Unexpected message: %v", m))}
			}
			out <- resp
		}
	}()
	return out
}

var ReconRqstPolyNotFound = errors.New("Peer should not receive a request for a non-existant node in ReconRqstPoly")

func (p *Peer) handleReconRqstPoly(rp *ReconRqstPoly) *msgProgress {
	remoteSize := rp.Size
	points := p.Points()
	remoteSamples := rp.Samples
	node, err := p.Node(rp.Prefix)
	if err == PNodeNotFound {
		return &msgProgress{err: ReconRqstPolyNotFound}
	}
	localSamples := node.SValues()
	localSize := node.Size()
	remoteSet, localSet, err := p.solve(
		remoteSamples, localSamples, remoteSize, localSize, points)
	if err == LowMBar {
		log.Println(GOSSIP, "Low MBar")
		if node.IsLeaf() || node.Size() < (p.ThreshMult()*p.MBar()) {
			log.Println(GOSSIP, "Sending full elements for node:", node.Key())
			return &msgProgress{elements: NewZSet(), messages: []ReconMsg{&FullElements{ZSet: NewZSet(node.Elements()...)}}}
		}
	}
	if err != nil {
		log.Println(GOSSIP, "sending SyncFail because", err)
		return &msgProgress{elements: NewZSet(), messages: []ReconMsg{&SyncFail{}}}
	}
	log.Println(GOSSIP, "solved: localSet=", localSet, "remoteSet=", remoteSet)
	return &msgProgress{elements: remoteSet, messages: []ReconMsg{&Elements{ZSet: localSet}}}
}

func (p *Peer) solve(remoteSamples, localSamples []*Zp, remoteSize, localSize int, points []*Zp) (*ZSet, *ZSet, error) {
	var values []*Zp
	for i, x := range remoteSamples {
		values = append(values, Z(x.P).Div(x, localSamples[i]))
	}
	log.Println(GOSSIP, "Reconcile", values, points, remoteSize-localSize)
	return Reconcile(values, points, remoteSize-localSize)
}

func (p *Peer) handleReconRqstFull(rf *ReconRqstFull) *msgProgress {
	node, err := p.Node(rf.Prefix)
	if err == PNodeNotFound {
		return &msgProgress{err: ReconRqstPolyNotFound}
	}
	localset := NewZSet(node.Elements()...)
	localdiff := ZSetDiff(localset, rf.Elements)
	remotediff := ZSetDiff(rf.Elements, localset)
	log.Println(GOSSIP, "localdiff=", localdiff, "remotediff=", remotediff)
	return &msgProgress{elements: remotediff, messages: []ReconMsg{&Elements{ZSet: localdiff}}}
}

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
	//"log"
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
			p.log(GOSSIP, "enabled:", enabled && isOpen)
			if !enabled || !isOpen {
				close(p.gossipEnable)
				p.stopped <- true
				return
			}
		default:
		}
		peer, err := p.choosePartner()
		if err != nil {
			if err != NoPartnersError {
				p.log(GOSSIP, "choosePartner:", err)
			}
			goto DELAY
		}
		p.log(GOSSIP, "Initiating recon with peer", peer)
		p.ExecCmd(func() error {
			return p.initiateRecon(peer)
		})
		//if err != nil {
		//log.Print(err)
		//}
	DELAY:
		delay := time.Duration(p.GossipIntervalSecs) * time.Second
		// jitter the delay
		time.Sleep(delay)
	}
}

var NoPartnersError error = errors.New("That feel when no recon partner")

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
	conn.SetDeadline(time.Now().Add(time.Second))
	p.log(GOSSIP, "Connected with", peer)
	// Interact with peer
	return p.clientRecon(conn)
}

type msgProgress struct {
	elements *ZSet
	err      error
}

type msgProgressChan chan *msgProgress

var ReconDone = errors.New("Reconciliation Done")

func (p *Peer) clientRecon(conn net.Conn) error {
	// Send config to server on connect
	err := WriteMsg(conn, &Config{Contents: p.Config()})
	if err != nil {
		return err
	}
	// Receive remote peer's config
	msg, err := ReadMsg(conn)
	if err != nil {
		return err
	}
	remoteConfig, is := msg.(*Config)
	if !is {
		return errors.New(fmt.Sprintf(
			"Remote config: expected config message, got %v", msg))
	}
	p.log(GOSSIP, "Got RemoteConfig:", remoteConfig)
	respSet := NewZSet()
	for step := range p.interactWithServer(conn) {
		if step.err != nil {
			if step.err == ReconDone {
				p.log(GOSSIP, "Reconcilation done.")
				break
			}
			p.log(GOSSIP, "Reconciliation failed:", step.err)
			return step.err
		}
		p.log(GOSSIP, "Add step:", step.elements)
		respSet.AddAll(step.elements)
		p.log(GOSSIP, "Recover set now:", respSet)
	}
	items := respSet.Items()
	if len(items) > 0 {
		p.log(GOSSIP, "Sending recover:", items)
		p.RecoverChan <- &Recover{
			RemoteAddr:     conn.RemoteAddr(),
			RemoteConfig:   remoteConfig.Contents,
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
				p.log(GOSSIP, "interact: msg err:", err)
				out <- &msgProgress{err: err}
				return
			}
			p.log(GOSSIP, "interact: got msg:", msg)
			switch m := msg.(type) {
			case *ReconRqstPoly:
				resp = p.handleReconRqstPoly(m, conn)
			case *ReconRqstFull:
				resp = p.handleReconRqstFull(m, conn)
			case *Elements:
				p.log(GOSSIP, "Elements:", m.ZSet)
				resp = &msgProgress{elements: m.ZSet}
			case *Done:
				resp = &msgProgress{err: ReconDone}
			case *Flush:
				resp = &msgProgress{elements: NewZSet()}
			default:
				resp = &msgProgress{err: errors.New(fmt.Sprintf("Unexpected message: %v", m))}
			}
			out <- resp
		}
	}()
	return out
}

var ReconRqstPolyNotFound = errors.New("Peer should not receive a request for a non-existant node in ReconRqstPoly")

func (p *Peer) handleReconRqstPoly(rp *ReconRqstPoly, conn net.Conn) *msgProgress {
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
		p.log(GOSSIP, "Low MBar")
		if node.IsLeaf() || node.Size() < (p.ThreshMult*p.MBar) {
			p.log(GOSSIP, "Sending full elements for node:", node.Key())
			WriteMsg(conn, &FullElements{ZSet: NewZSet(node.Elements()...)})
			return &msgProgress{elements: NewZSet()}
		}
	}
	if err != nil {
		p.log(GOSSIP, "sending SyncFail because", err)
		WriteMsg(conn, &SyncFail{})
		return &msgProgress{elements: NewZSet()}
	}
	p.log(GOSSIP, "solved: localSet=", localSet, "remoteSet=", remoteSet)
	WriteMsg(conn, &Elements{ZSet: localSet})
	return &msgProgress{elements: remoteSet}
}

func (p *Peer) solve(remoteSamples, localSamples []*Zp, remoteSize, localSize int, points []*Zp) (*ZSet, *ZSet, error) {
	var values []*Zp
	for i, x := range remoteSamples {
		values = append(values, Z(x.P).Div(x, localSamples[i]))
	}
	p.log(GOSSIP, "Reconcile", values, points, remoteSize-localSize)
	return Reconcile(values, points, remoteSize-localSize)
}

func (p *Peer) handleReconRqstFull(rf *ReconRqstFull, conn net.Conn) *msgProgress {
	node, err := p.Node(rf.Prefix)
	if err == PNodeNotFound {
		return &msgProgress{err: ReconRqstPolyNotFound}
	}
	localset := NewZSet(node.Elements()...)
	localdiff := ZSetDiff(localset, rf.Elements)
	remotediff := ZSetDiff(rf.Elements, localset)
	p.log(GOSSIP, "localdiff=", localdiff, "remotediff=", remotediff)
	WriteMsg(conn, &Elements{ZSet: localdiff})
	return &msgProgress{elements: remotediff}
}

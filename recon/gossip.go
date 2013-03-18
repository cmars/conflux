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
	"net"
	"time"
)

// Gossip with remote servers, acting as a client.
func (p *Peer) Gossip() {
	enabled := true
	var isOpen bool
	for {
		select {
		case enabled, isOpen = <-p.gossipEnable:
			if !enabled {
				continue
			}
			if !isOpen {
				return
			}
		default:
			;
		}
		peer, err := p.choosePartner()
		if err != nil {
			log.Print(err)
			goto DELAY
		}
		err = p.initiateRecon(peer)
		if err != nil {
			log.Print(err)
		}
	DELAY:
		delay := time.Duration(p.GossipIntervalSecs()) * time.Second
		// jitter the delay
		time.Sleep(delay)
	}
}

func (p *Peer) choosePartner() (net.Addr, error) {
	// TODO: choose a remote peer at random
	panic("not impl")
}

func (p *Peer) initiateRecon(peer net.Addr) error {
	// Connect to peer
	conn, err := net.Dial(peer.Network(), peer.String())
	if err != nil {
		return err
	}
	// Interact with peer
	return p.clientRecon(conn)
}

type msgProgress struct {
	elements *ZSet
	err      error
}

type msgProgressChan chan *msgProgress

var ReconDone = errors.New("Reconciliation Done")

func getRemoteConfig(conn net.Conn) (Settings, error) {
	panic("no impl")
}

func (p *Peer) clientRecon(conn net.Conn) error {
	// Get remote config
	/*remoteConfig*/ _, err := getRemoteConfig(conn)
	if err != nil {
		return err
	}
	var respSet *ZSet = NewZSet()
	for step := range p.interactWithServer(conn) {
		if step.err != nil {
			if step.err == ReconDone {
				break
			}
			return step.err
		} else {
			respSet.AddAll(step.elements)
		}
	}
	p.RecoverChan <- &Recover{
		RemoteAddr:     conn.RemoteAddr(),
		RemoteElements: respSet}
	return nil
}

func (p *Peer) interactWithServer(conn net.Conn) msgProgressChan {
	out := make(msgProgressChan)
	go func() {
		var resp *msgProgress
		for resp == nil || resp.err == nil {
			msg, err := ReadMsg(conn)
			if err != nil {
				out <- &msgProgress{err: err}
				return
			}
			switch m := msg.(type) {
			case *ReconRqstPoly:
				resp = p.handleReconRqstPoly(m, conn)
			case *ReconRqstFull:
				resp = p.handleReconRqstFull(m, conn)
			case *Elements:
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
	remoteSet, localSet, err := solve(
		remoteSamples, localSamples, remoteSize, localSize, points)
	if err == LowMBar {
		if node.IsLeaf() || node.Size() < (p.ThreshMult()*p.MBar()) {
			(&FullElements{ZSet: NewZSet(node.Elements()...)}).marshal(conn)
			return &msgProgress{elements: NewZSet()}
		} else {
			(&SyncFail{}).marshal(conn)
			return &msgProgress{elements: NewZSet()}
		}
	}
	(&Elements{ZSet: localSet}).marshal(conn)
	return &msgProgress{elements: remoteSet}
}

func solve(remoteSamples, localSamples []*Zp, remoteSize, localSize int, points []*Zp) (*ZSet, *ZSet, error) {
	var values []*Zp
	for i, x := range remoteSamples {
		values = append(values, Z(x.P).Div(x, localSamples[i]))
	}
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
	(&Elements{ZSet: localdiff}).marshal(conn)
	return &msgProgress{elements: remotediff}
}

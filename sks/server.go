package sks

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
}

type ReconServer struct {
	Port        int
	RecoverChan RecoverChan
	Tree        PTree
	Settings    ReconConfig
}

func (rs *ReconServer) Serve() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", rs.Port))
	if err != nil {
		return err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			// TODO: log error
			continue
		}
		go rs.handleConnection(conn)
	}
	return nil
}

type msgProgress struct {
	elements *ZSet
	err      error
}

type msgProgressChan chan *msgProgress

var ReconDone = errors.New("Reconciliation Done")

func (rs *ReconServer) handleConnection(conn net.Conn) {
	var respSet *ZSet = NewZSet()
	for step := range rs.interact(conn) {
		if step.err != nil {
			if step.err == ReconDone {
				break
			}
			// log error
			return
		} else {
			respSet.AddAll(step.elements)
		}
	}
	rs.RecoverChan <- &Recover{
		RemoteAddr:     conn.RemoteAddr(),
		RemoteElements: respSet}
}

func (rs *ReconServer) interact(conn net.Conn) msgProgressChan {
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
				resp = rs.handleReconRqstPoly(m, conn)
			case *ReconRqstFull:
				resp = rs.handleReconRqstFull(m, conn)
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

var ReconRqstPolyNotFound = errors.New("Server should not receive a request for a non-existant node in ReconRqstPoly")

func (rs *ReconServer) handleReconRqstPoly(rp *ReconRqstPoly, conn net.Conn) *msgProgress {
	remoteSize := rp.Size
	points := rs.Tree.Points()
	remoteSamples := rp.Samples
	node, err := rs.Tree.GetNodeKey(rp.Prefix)
	if err == PNodeNotFound {
		return &msgProgress{err: ReconRqstPolyNotFound}
	}
	localSamples := node.SValues()
	localSize := node.Size()
	remoteSet, localSet, err := solve(
		remoteSamples, localSamples, remoteSize, localSize, points)
	if err == LowMBar {
		if node.IsLeaf() || rs.Tree.NumElements(node) < (rs.Settings.ReconThreshMult()*rs.Settings.MBar()) {
			(&FullElements{ZSet: node.Elements()}).marshal(conn)
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

func (rs *ReconServer) handleReconRqstFull(rf *ReconRqstFull, conn net.Conn) *msgProgress {
	node, err := rs.Tree.GetNodeKey(rf.Prefix)
	if err == PNodeNotFound {
		return &msgProgress{err: ReconRqstPolyNotFound}
	}
	localset := node.Elements()
	localdiff := ZSetDiff(localset, rf.Elements)
	remotediff := ZSetDiff(rf.Elements, localset)
	(&Elements{ZSet: localdiff}).marshal(conn)
	return &msgProgress{elements:remotediff}
}

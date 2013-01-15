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

type PTree interface { /* TODO */
}

type ReconConfig interface {
	Version() string
	HttpPort() int
	BitQuantum() int
	MBar() int
	Filters() []string
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
				panic("todo")
			case *ReconRqstFull:
				panic("todo")
			case *Elements:
				resp = &msgProgress{elements: m.ZSet}
			case *FullElements:
				panic("todo")
			case *SyncFail:
				panic("todo")
			case *Done:
				resp = &msgProgress{err: ReconDone}
			case *Flush:
				panic("todo")
			case *Error:
				panic("todo")
			case *DbRqst:
				panic("todo")
			case *DbRepl:
				panic("todo")
			case *Config:
				panic("todo")
			default:
				resp = &msgProgress{err: errors.New(fmt.Sprintf("Unsupported message type: %v", m))}
			}
			out <- resp
		}
	}()
	return out
}

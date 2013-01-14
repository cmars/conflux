package sks

import (
	"fmt"
	"io"
	"net"
)

type Request struct {
	// Recon protocol message received
	Msg ReconMsg
	// Response channel
	Response ResponseChan
}

type Response interface {
	Error() error
	WriteTo(w io.Writer)
}

type RequestChan chan *Request

type ResponseChan chan Response

type ReconServer struct {
	Port int
	Requests RequestChan
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

func (rs *ReconServer) handleConnection(conn net.Conn) {
	msg, err := ReadMsg(conn)
	if err != nil {
		// TODO: log error
		return
	}
	respChan := make(ResponseChan)
	req := &Request{
		Msg: msg,
		Response: respChan }
	rs.Requests <-req
	resp := <-respChan
	resp.WriteTo(conn)
}

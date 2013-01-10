package conflux

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"github.com/cmars/conflux"
)

type MsgType uint8

const (
	MsgTypeReconRqstPoly = MsgType(0)
	MsgTypeReconRqstFull = MsgType(1)
	MsgTypeElements = MsgType(2)
	MsgTypeFullElements = MsgType(3)
	MsgTypeSyncFail = MsgType(4)
	MsgTypeDone = MsgType(5)
	MsgTypeFlush = MsgType(6)
	MsgTypeError = MsgType(7)
	MsgTypeDbRqst = MsgType(8)
	MsgTypeDbRepl = MsgType(9)
	MsgTypeConfig = MsgType(10)
)

type ReconMsg interface {
	MsgType() MsgType
	unmarshal(r io.Reader) error
	marshal(w io.Writer) error
}

type emptyMsg struct {}

func (msg *emptyMsg) unmarshal(r io.Reader) error { return nil }

func (msg *emptyMsg) marshal(w io.Writer) error { return nil }

type textMsg struct { Text string }

func (msg *textMsg) unmarshal(r io.Reader) (err error) {
	msg.Text, err = readString(r)
	return
}

func (msg *textMsg) marshal(w io.Writer) error {
	return writeString(w, msg.Text)
}

type notImplMsg struct {}

func (msg *notImplMsg) unmarshal(r io.Reader) error {
	panic("not implemented")
}

func (msg *notImplMsg) marshal(w io.Writer) error {
	panic("not implemented")
}

func readInt(r io.Reader) (n int, err error) {
	buf := make([]byte, 4)
	_, err = r.Read(buf)
	n = int(binary.BigEndian.Uint32(buf))
	return
}

func writeInt(w io.Writer, n int) (err error) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(n))
	_, err = w.Write(buf)
	return
}

func readString(r io.Reader) (string, error) {
	var n int
	n, err := readInt(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, n)
	_, err = r.Read(buf)
	return string(buf), err
}

func writeString(w io.Writer, text string) error {
	panic("not impl")
}

func readBitstring(r io.Reader) ([]byte, error) {
	var n int
	n, err := readInt(r)
	if err != nil {
		return nil, err
	}
	nbytes := n / 8
	if n % 8 > 0 {
		nbytes++
	}
	buf := make([]byte, nbytes)
	_, err = r.Read(buf)
	return buf, err
}

func readZZarray(r io.Reader) ([]*conflux.Zp, error) {
	panic("not impl")
}

func readZSet(r io.Reader) (*conflux.ZSet, error) {
	panic("not impl")
}

type ReconRqstPoly struct {
	*notImplMsg
	Prefix []byte
	Size int
	Samples []*conflux.Zp
}

func (msg *ReconRqstPoly) MsgType() MsgType {
	return MsgTypeReconRqstPoly
}

func (msg *ReconRqstPoly) unmarshal(r io.Reader) (err error) {
	msg.Prefix, err = readBitstring(r)
	if err != nil {
		return
	}
	msg.Size, err = readInt(r)
	if err != nil {
		return
	}
	msg.Samples, err = readZZarray(r)
	return
}

type ReconRqstFull struct {
	*notImplMsg
	Prefix []byte
	Elements *conflux.ZSet
}

func (msg *ReconRqstFull) MsgType() MsgType {
	return MsgTypeReconRqstFull
}

func (msg *ReconRqstFull) unmarshal(r io.Reader) (err error) {
	msg.Prefix, err = readBitstring(r)
	if err != nil {
		return
	}
	msg.Elements, err = readZSet(r)
	return
}

type Elements struct {
	*notImplMsg
}

func (msg *Elements) MsgType() MsgType {
	return MsgTypeElements
}

type FullElements struct {
	*notImplMsg
}

func (msg *FullElements) MsgType() MsgType {
	return MsgTypeFullElements
}

type SyncFail struct {
	*emptyMsg
}

func (msg *SyncFail) MsgType() MsgType {
	return MsgTypeSyncFail
}

type Done struct {
	*emptyMsg
}

func (msg *Done) MsgType() MsgType {
	return MsgTypeDone
}

type Flush struct {
	*emptyMsg
}

func (msg *Flush) MsgType() MsgType {
	return MsgTypeFlush
}

type Error struct {
	*notImplMsg
}

func (msg *Error) MsgType() MsgType {
	return MsgTypeError
}

type DbRqst struct {
	*notImplMsg
}

func (msg *DbRqst) MsgType() MsgType {
	return MsgTypeDbRqst
}

type DbRepl struct {
	*notImplMsg
}

func (msg *DbRepl) MsgType() MsgType {
	return MsgTypeDbRepl
}

type Config struct {
	*notImplMsg
	Contents map[string]string
}

func (msg *Config) MsgType() MsgType {
	return MsgTypeConfig
}

func (msg *Config) unmarshal(r io.Reader) (err error) {
	panic("not impl")
}

func ReadMsg(r io.Reader) (msg ReconMsg, err error) {
	buf := make([]byte, 1)
	_, err = r.Read(buf[:1])
	if err != nil {
		return nil, err
	}
	msgType := MsgType(buf[0])
	switch (msgType) {
	case MsgTypeReconRqstPoly:
		msg = &ReconRqstPoly{}
	case MsgTypeReconRqstFull:
		msg = &ReconRqstFull{}
	case MsgTypeElements:
		msg = &Elements{}
	case MsgTypeFullElements:
		msg = &FullElements{}
	case MsgTypeSyncFail:
		msg = &SyncFail{}
	case MsgTypeDone:
		msg = &Done{}
	case MsgTypeFlush:
		msg = &Flush{}
	case MsgTypeError:
		msg = &Error{}
	case MsgTypeDbRqst:
		msg = &DbRqst{}
	case MsgTypeDbRepl:
		msg = &DbRepl{}
	case MsgTypeConfig:
		msg = &Config{}
	default:
		return nil, errors.New(fmt.Sprintf("Unexpected message code: %d", msgType))
	}
	err = msg.unmarshal(r)
	return
}

package sks

import (
	"encoding/binary"
	"errors"
	"fmt"
	. "github.com/cmars/conflux"
	"io"
	"math/big"
)

var sksZpNbytes int

func init() {
	sksZpNbytes = P_SKS.BitLen() / 8
	if P_SKS.BitLen()%8 != 0 {
		sksZpNbytes++
	}
}

type MsgType uint8

const (
	MsgTypeReconRqstPoly = MsgType(0)
	MsgTypeReconRqstFull = MsgType(1)
	MsgTypeElements      = MsgType(2)
	MsgTypeFullElements  = MsgType(3)
	MsgTypeSyncFail      = MsgType(4)
	MsgTypeDone          = MsgType(5)
	MsgTypeFlush         = MsgType(6)
	MsgTypeError         = MsgType(7)
	MsgTypeDbRqst        = MsgType(8)
	MsgTypeDbRepl        = MsgType(9)
	MsgTypeConfig        = MsgType(10)
)

type ReconMsg interface {
	MsgType() MsgType
	unmarshal(r io.Reader) error
	marshal(w io.Writer) error
}

type emptyMsg struct{}

func (msg *emptyMsg) unmarshal(r io.Reader) error { return nil }

func (msg *emptyMsg) marshal(w io.Writer) error { return nil }

type textMsg struct{ Text string }

func (msg *textMsg) unmarshal(r io.Reader) (err error) {
	msg.Text, err = readString(r)
	return
}

func (msg *textMsg) marshal(w io.Writer) error {
	return writeString(w, msg.Text)
}

type notImplMsg struct{}

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

func writeString(w io.Writer, text string) (err error) {
	err = writeInt(w, len(text))
	if err != nil {
		return
	}
	_, err = w.Write([]byte(text))
	return
}

func readBitstring(r io.Reader) ([]byte, error) {
	var n int
	n, err := readInt(r)
	if err != nil {
		return nil, err
	}
	nbytes := n / 8
	if n%8 > 0 {
		nbytes++
	}
	buf := make([]byte, nbytes)
	_, err = r.Read(buf)
	return buf, err
}

func writeBitstring(w io.Writer, buf []byte) (err error) {
	err = writeInt(w, len(buf)*8)
	if err != nil {
		return
	}
	_, err = w.Write(buf)
	return
}

func readZZarray(r io.Reader) ([]*Zp, error) {
	n, err := readInt(r)
	if err != nil {
		return nil, err
	}
	arr := make([]*Zp, n)
	for i := 0; i < n; i++ {
		arr[i], err = readZp(r)
		if err != nil {
			return nil, err
		}
	}
	return arr, nil
}

func writeZZarray(w io.Writer, arr []*Zp) (err error) {
	err = writeInt(w, len(arr))
	if err != nil {
		return
	}
	for _, z := range arr {
		err = writeZp(w, z)
		if err != nil {
			return
		}
	}
	return
}

func readZSet(r io.Reader) (*ZSet, error) {
	arr, err := readZZarray(r)
	if err != nil {
		return nil, err
	}
	zset := NewZSet()
	zset.AddSlice(arr)
	return zset, nil
}

func writeZSet(w io.Writer, zset *ZSet) error {
	return writeZZarray(w, zset.Items())
}

func readZp(r io.Reader) (*Zp, error) {
	buf := make([]byte, sksZpNbytes)
	_, err := r.Read(buf)
	if err != nil {
		return nil, err
	}
	v := big.NewInt(0).SetBytes(buf)
	z := &Zp{Int: v, P: P_SKS}
	z.Norm()
	return z, nil
}

func writeZp(w io.Writer, z *Zp) error {
	_, err := w.Write(z.Int.Bytes())
	return err
}

type ReconRqstPoly struct {
	Prefix  []byte
	Size    int
	Samples []*Zp
}

func (msg *ReconRqstPoly) MsgType() MsgType {
	return MsgTypeReconRqstPoly
}

func (msg *ReconRqstPoly) marshal(w io.Writer) (err error) {
	err = writeBitstring(w, msg.Prefix)
	if err != nil {
		return
	}
	err = writeInt(w, msg.Size)
	if err != nil {
		return
	}
	err = writeZZarray(w, msg.Samples)
	return
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
	Prefix   []byte
	Elements *ZSet
}

func (msg *ReconRqstFull) MsgType() MsgType {
	return MsgTypeReconRqstFull
}

func (msg *ReconRqstFull) marshal(w io.Writer) (err error) {
	err = writeBitstring(w, msg.Prefix)
	if err != nil {
		return
	}
	err = writeZSet(w, msg.Elements)
	return
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
	*ZSet
}

func (msg *Elements) MsgType() MsgType {
	return MsgTypeElements
}

func (msg *Elements) marshal(w io.Writer) (err error) {
	err = writeZSet(w, msg.ZSet)
	return
}

func (msg *Elements) unmarshal(r io.Reader) (err error) {
	msg.ZSet, err = readZSet(r)
	return
}

type FullElements struct {
	*ZSet
}

func (msg *FullElements) MsgType() MsgType {
	return MsgTypeFullElements
}

func (msg *FullElements) marshal(w io.Writer) (err error) {
	err = writeZSet(w, msg.ZSet)
	return
}

func (msg *FullElements) unmarshal(r io.Reader) (err error) {
	msg.ZSet, err = readZSet(r)
	return
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
	*textMsg
}

func (msg *Error) MsgType() MsgType {
	return MsgTypeError
}

type DbRqst struct {
	*textMsg
}

func (msg *DbRqst) MsgType() MsgType {
	return MsgTypeDbRqst
}

type DbRepl struct {
	*textMsg
}

func (msg *DbRepl) MsgType() MsgType {
	return MsgTypeDbRepl
}

type Config struct {
	Contents map[string]string
}

func (msg *Config) MsgType() MsgType {
	return MsgTypeConfig
}

func (msg *Config) marshal(w io.Writer) (err error) {
	err = writeInt(w, len(msg.Contents))
	if err != nil {
		return
	}
	for k, v := range msg.Contents {
		err = writeString(w, k)
		if err != nil {
			return
		}
		err = writeString(w, v)
		if err != nil {
			return
		}
	}
	return
}

func (msg *Config) unmarshal(r io.Reader) error {
	n, err := readInt(r)
	if err != nil {
		return err
	}
	msg.Contents = make(map[string]string)
	for i := 0; i < n; i++ {
		k, err := readString(r)
		if err != nil {
			return err
		}
		v, err := readString(r)
		if err != nil {
			return err
		}
		msg.Contents[k] = v
	}
	return nil
}

func ReadMsg(r io.Reader) (msg ReconMsg, err error) {
	buf := make([]byte, 1)
	_, err = r.Read(buf[:1])
	if err != nil {
		return nil, err
	}
	msgType := MsgType(buf[0])
	switch msgType {
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

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
	"bytes"
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

func (mt MsgType) String() string {
	switch mt {
	case MsgTypeReconRqstPoly:
		return "ReconRqstPoly"
	case MsgTypeReconRqstFull:
		return "ReconRqstFull"
	case MsgTypeElements:
		return "Elements"
	case MsgTypeFullElements:
		return "FullElements"
	case MsgTypeSyncFail:
		return "SyncFail"
	case MsgTypeDone:
		return "Done"
	case MsgTypeFlush:
		return "Flush"
	case MsgTypeError:
		return "Error"
	case MsgTypeDbRqst:
		return "DbRqst"
	case MsgTypeDbRepl:
		return "DbRepl"
	case MsgTypeConfig:
		return "Config"
	}
	return "Unknown"
}

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
	msg.Text, err = ReadString(r)
	return
}

func (msg *textMsg) marshal(w io.Writer) error {
	return WriteString(w, msg.Text)
}

type notImplMsg struct{}

func (msg *notImplMsg) unmarshal(r io.Reader) error {
	panic("not implemented")
}

func (msg *notImplMsg) marshal(w io.Writer) error {
	panic("not implemented")
}

func ReadInt(r io.Reader) (n int, err error) {
	buf := make([]byte, 4)
	_, err = io.ReadFull(r, buf)
	n = int(binary.BigEndian.Uint32(buf))
	return
}

func WriteInt(w io.Writer, n int) (err error) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(n))
	_, err = w.Write(buf)
	return
}

func ReadString(r io.Reader) (string, error) {
	var n int
	n, err := ReadInt(r)
	if err != nil || n == 0 {
		return "", err
	}
	buf := make([]byte, n)
	_, err = io.ReadFull(r, buf)
	return string(buf), err
}

func WriteString(w io.Writer, text string) (err error) {
	err = WriteInt(w, len(text))
	if err != nil {
		return
	}
	_, err = w.Write([]byte(text))
	return
}

func ReadBitstring(r io.Reader) (*Bitstring, error) {
	var n int
	n, err := ReadInt(r)
	if err != nil {
		return nil, err
	}
	bs := NewBitstring(n)
	if n == 0 {
		return bs, nil
	}
	buf := make([]byte, bs.ByteLen())
	_, err = io.ReadFull(r, buf)
	bs.SetBytes(buf)
	return bs, err
}

func WriteBitstring(w io.Writer, bs *Bitstring) (err error) {
	err = WriteInt(w, bs.BitLen())
	if err != nil {
		return
	}
	_, err = w.Write(bs.Bytes())
	return
}

func ReadZZarray(r io.Reader) ([]*Zp, error) {
	n, err := ReadInt(r)
	if err != nil {
		return nil, err
	}
	arr := make([]*Zp, n)
	for i := 0; i < n; i++ {
		arr[i], err = ReadZp(r)
		if err != nil {
			return nil, err
		}
	}
	return arr, nil
}

func WriteZZarray(w io.Writer, arr []*Zp) (err error) {
	err = WriteInt(w, len(arr))
	if err != nil {
		return
	}
	for _, z := range arr {
		err = WriteZp(w, z)
		if err != nil {
			return
		}
	}
	return
}

func ReadZSet(r io.Reader) (*ZSet, error) {
	arr, err := ReadZZarray(r)
	if err != nil {
		return nil, err
	}
	zset := NewZSet()
	zset.AddSlice(arr)
	return zset, nil
}

func WriteZSet(w io.Writer, zset *ZSet) error {
	return WriteZZarray(w, zset.Items())
}

func ReadZp(r io.Reader) (*Zp, error) {
	buf := make([]byte, sksZpNbytes)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}
	v := big.NewInt(0).SetBytes(ReverseBytes(buf))
	z := &Zp{Int: v, P: P_SKS}
	z.Norm()
	return z, nil
}

func WriteZp(w io.Writer, z *Zp) (err error) {
	num := z.Int.Bytes()
	_, err = w.Write(ReverseBytes(num))
	if err != nil {
		return
	}
	pad := make([]byte, sksZpNbytes-len(num))
	_, err = w.Write(pad)
	return
}

type ReconRqstPoly struct {
	Prefix  *Bitstring
	Size    int
	Samples []*Zp
}

func (msg *ReconRqstPoly) MsgType() MsgType {
	return MsgTypeReconRqstPoly
}

func (msg *ReconRqstPoly) String() string {
	return fmt.Sprintf("%v: prefix=%v size=%v elements=%v",
		msg.MsgType(), msg.Prefix, msg.Size, msg.Samples)
}

func (msg *ReconRqstPoly) marshal(w io.Writer) (err error) {
	err = WriteBitstring(w, msg.Prefix)
	if err != nil {
		return
	}
	err = WriteInt(w, msg.Size)
	if err != nil {
		return
	}
	err = WriteZZarray(w, msg.Samples)
	return
}

func (msg *ReconRqstPoly) unmarshal(r io.Reader) (err error) {
	msg.Prefix, err = ReadBitstring(r)
	if err != nil {
		return
	}
	msg.Size, err = ReadInt(r)
	if err != nil {
		return
	}
	msg.Samples, err = ReadZZarray(r)
	return
}

type ReconRqstFull struct {
	Prefix   *Bitstring
	Elements *ZSet
}

func (msg *ReconRqstFull) String() string {
	return fmt.Sprintf("%v: prefix=%v elements=%v",
		msg.MsgType(), msg.Prefix, msg.Elements)
}

func (msg *ReconRqstFull) MsgType() MsgType {
	return MsgTypeReconRqstFull
}

func (msg *ReconRqstFull) marshal(w io.Writer) (err error) {
	err = WriteBitstring(w, msg.Prefix)
	if err != nil {
		return
	}
	err = WriteZSet(w, msg.Elements)
	return
}

func (msg *ReconRqstFull) unmarshal(r io.Reader) (err error) {
	msg.Prefix, err = ReadBitstring(r)
	if err != nil {
		return
	}
	msg.Elements, err = ReadZSet(r)
	return
}

type Elements struct {
	*ZSet
}

func (msg *Elements) String() string {
	return fmt.Sprintf("%v", msg.MsgType())
}

func (msg *Elements) MsgType() MsgType {
	return MsgTypeElements
}

func (msg *Elements) marshal(w io.Writer) (err error) {
	err = WriteZSet(w, msg.ZSet)
	return
}

func (msg *Elements) unmarshal(r io.Reader) (err error) {
	msg.ZSet, err = ReadZSet(r)
	return
}

type FullElements struct {
	*ZSet
}

func (msg *FullElements) String() string {
	return fmt.Sprintf("%v", msg.MsgType())
}

func (msg *FullElements) MsgType() MsgType {
	return MsgTypeFullElements
}

func (msg *FullElements) marshal(w io.Writer) (err error) {
	err = WriteZSet(w, msg.ZSet)
	return
}

func (msg *FullElements) unmarshal(r io.Reader) (err error) {
	msg.ZSet, err = ReadZSet(r)
	return
}

type SyncFail struct {
	*emptyMsg
}

func (msg *SyncFail) String() string {
	return fmt.Sprintf("%v", msg.MsgType())
}

func (msg *SyncFail) MsgType() MsgType {
	return MsgTypeSyncFail
}

type Done struct {
	*emptyMsg
}

func (msg *Done) String() string {
	return fmt.Sprintf("%v", msg.MsgType())
}

func (msg *Done) MsgType() MsgType {
	return MsgTypeDone
}

type Flush struct {
	*emptyMsg
}

func (msg *Flush) String() string {
	return fmt.Sprintf("%v", msg.MsgType())
}

func (msg *Flush) MsgType() MsgType {
	return MsgTypeFlush
}

type Error struct {
	*textMsg
}

func (msg *Error) String() string {
	return fmt.Sprintf("%v: %v", msg.MsgType(), msg.Text)
}

func (msg *Error) MsgType() MsgType {
	return MsgTypeError
}

type DbRqst struct {
	*textMsg
}

func (msg *DbRqst) String() string {
	return fmt.Sprintf("%v: %v", msg.MsgType(), msg.Text)
}

func (msg *DbRqst) MsgType() MsgType {
	return MsgTypeDbRqst
}

type DbRepl struct {
	*textMsg
}

func (msg *DbRepl) String() string {
	return fmt.Sprintf("%v: %v", msg.MsgType(), msg.Text)
}

func (msg *DbRepl) MsgType() MsgType {
	return MsgTypeDbRepl
}

var RemoteConfigPassed string = "passed"
var RemoteConfigFailed string = "failed"

type Config struct {
	Version    string
	HttpPort   int
	BitQuantum int
	MBar       int
	Filters    string
	Custom     map[string]string
}

func (msg *Config) String() string {
	return fmt.Sprintf("%v: Version=%v HttpPort=%v BitQuantum=%v MBar=%v Filters=%s", msg.MsgType(),
		msg.Version, msg.HttpPort, msg.BitQuantum, msg.MBar, msg.Filters)
}

func (msg *Config) MsgType() MsgType {
	return MsgTypeConfig
}

func (msg *Config) marshal(w io.Writer) (err error) {
	if err = WriteInt(w, 5+len(msg.Custom)); err != nil {
		return
	}
	if err = WriteString(w, "version"); err != nil {
		return
	}
	if err = WriteString(w, msg.Version); err != nil {
		return
	}
	if err = WriteString(w, "http port"); err != nil {
		return
	}
	if err = WriteInt(w, 4); err != nil {
		return
	}
	if err = WriteInt(w, msg.HttpPort); err != nil {
		return
	}
	if err = WriteString(w, "bitquantum"); err != nil {
		return
	}
	if err = WriteInt(w, 4); err != nil {
		return
	}
	if err = WriteInt(w, msg.BitQuantum); err != nil {
		return
	}
	if err = WriteString(w, "mbar"); err != nil {
		return
	}
	if err = WriteInt(w, 4); err != nil {
		return
	}
	if err = WriteInt(w, msg.MBar); err != nil {
		return
	}
	if err = WriteString(w, "filters"); err != nil {
		return
	}
	if err = WriteString(w, msg.Filters); err != nil {
		return
	}
	if msg.Custom != nil {
		for k, v := range msg.Custom {
			if err = WriteString(w, k); err != nil {
				return
			}
			if err = WriteString(w, v); err != nil {
				return
			}
		}
	}
	return
}

func (msg *Config) unmarshal(r io.Reader) (err error) {
	var n int
	if n, err = ReadInt(r); err != nil {
		return err
	}
	msg.Custom = make(map[string]string)
	var ival int
	var k, v string
	for i := 0; i < n; i++ {
		k, err = ReadString(r)
		if err != nil {
			return err
		}
		switch k {
		case "http port":
			fallthrough
		case "bitquantum":
			fallthrough
		case "mbar":
			// Read the int length
			if ival, err = ReadInt(r); err != nil {
				return err
			} else if ival != 4 {
				return errors.New(fmt.Sprintf("Invalid length=%d for integer config value %s", ival, k))
			}
			// Read the int
			if ival, err = ReadInt(r); err != nil {
				return err
			}
		default:
			v, err = ReadString(r)
			if err != nil {
				return err
			}
		}
		switch k {
		case "version":
			msg.Version = v
		case "http port":
			msg.HttpPort = ival
		case "bitquantum":
			msg.BitQuantum = ival
		case "mbar":
			msg.MBar = ival
		case "filters":
			msg.Filters = v
		default:
			msg.Custom[k] = v
		}
	}
	return nil
}

func ReadMsg(r io.Reader) (msg ReconMsg, err error) {
	var msgSize int
	msgSize, err = ReadInt(r)
	if err != nil {
		return nil, err
	}
	msgBuf := make([]byte, msgSize)
	_, err = io.ReadFull(r, msgBuf)
	if err != nil {
		return nil, err
	}
	br := bytes.NewBuffer(msgBuf)
	buf := make([]byte, 1)
	_, err = io.ReadFull(br, buf[:1])
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
	err = msg.unmarshal(br)
	return
}

func WriteMsg(w io.Writer, msg ReconMsg) (err error) {
	bw := bytes.NewBuffer(nil)
	buf := make([]byte, 1)
	buf[0] = byte(msg.MsgType())
	_, err = bw.Write(buf)
	if err != nil {
		return
	}
	msg.marshal(bw)
	WriteInt(w, bw.Len())
	_, err = w.Write(bw.Bytes())
	return err
}

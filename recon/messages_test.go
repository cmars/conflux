package recon

import (
	"bytes"
	"github.com/bmizerany/assert"
	"testing"
)

func TestConfigRoundTrip(t *testing.T) {
	c := &Config{
		Version:    "3.1415",
		HttpPort:   11371,
		BitQuantum: 2,
		MBar:       5}
	buf := bytes.NewBuffer(nil)
	err := c.marshal(buf)
	assert.Equal(t, nil, err)
	t.Logf("config=%x", buf)
	c2 := new(Config)
	err = c2.unmarshal(bytes.NewBuffer(buf.Bytes()))
	assert.Equal(t, nil, err)
	assert.Equal(t, c.Version, c2.Version)
	assert.Equal(t, c.HttpPort, c2.HttpPort)
	assert.Equal(t, c.BitQuantum, c2.BitQuantum)
	assert.Equal(t, c.MBar, c2.MBar)
}

func TestConfigMsgRoundTrip(t *testing.T) {
	c := &Config{
		Version:    "3.1415",
		HttpPort:   11371,
		BitQuantum: 2,
		MBar:       5}
	buf := bytes.NewBuffer(nil)
	err := WriteMsg(buf, c)
	assert.Equal(t, nil, err)
	msg, err := ReadMsg(bytes.NewBuffer(buf.Bytes()))
	assert.Equal(t, nil, err)
	c2 := msg.(*Config)
	assert.Equal(t, c.Version, c2.Version)
	assert.Equal(t, c.HttpPort, c2.HttpPort)
	assert.Equal(t, c.BitQuantum, c2.BitQuantum)
	assert.Equal(t, c.MBar, c2.MBar)
}

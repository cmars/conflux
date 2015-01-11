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
	"github.com/bmizerany/assert"
	"testing"
)

func TestConfigRoundTrip(t *testing.T) {
	c := &Config{
		Version:    "3.1415",
		HTTPPort:   11371,
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
	assert.Equal(t, c.HTTPPort, c2.HTTPPort)
	assert.Equal(t, c.BitQuantum, c2.BitQuantum)
	assert.Equal(t, c.MBar, c2.MBar)
}

func TestConfigMsgRoundTrip(t *testing.T) {
	c := &Config{
		Version:    "3.1415",
		HTTPPort:   11371,
		BitQuantum: 2,
		MBar:       5}
	buf := bytes.NewBuffer(nil)
	err := WriteMsg(buf, c)
	assert.Equal(t, nil, err)
	msg, err := ReadMsg(bytes.NewBuffer(buf.Bytes()))
	assert.Equal(t, nil, err)
	c2 := msg.(*Config)
	assert.Equal(t, c.Version, c2.Version)
	assert.Equal(t, c.HTTPPort, c2.HTTPPort)
	assert.Equal(t, c.BitQuantum, c2.BitQuantum)
	assert.Equal(t, c.MBar, c2.MBar)
}

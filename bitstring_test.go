/*
   conflux - Distributed database synchronization library
	Based on the algorithm described in
		"Set Reconciliation with Nearly Optimal	Communication Complexity",
			Yaron Minsky, Ari Trachtenberg, and Richard Zippel, 2004.

   Copyright (c) 2012-2015  Casey Marshall <cmars@cmarstech.com>

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package conflux

import (
	"testing"

	gc "gopkg.in/check.v1"
)

func Test(t *testing.T) { gc.TestingT(t) }

type BitstringSuite struct{}

var _ = gc.Suite(&BitstringSuite{})

func (s *BitstringSuite) TestSet(c *gc.C) {
	var bs *Bitstring
	// bitstring len=1
	bs = NewBitstring(1)
	c.Assert(bs.String(), gc.Equals, "0")
	bs.Flip(0)
	c.Assert(bs.String(), gc.Equals, "1")
	c.Assert(bs.Bytes()[0], gc.Equals, byte(0x80))
	// bitstring len=2
	bs = NewBitstring(2)
	c.Assert(bs.String(), gc.Equals, "00")
	bs.Flip(0)
	c.Assert(bs.String(), gc.Equals, "10")
	c.Assert(bs.Bytes()[0], gc.Equals, byte(0x80))
	bs.Flip(1)
	c.Assert(bs.String(), gc.Equals, "11")
	c.Assert(bs.Bytes()[0], gc.Equals, byte(0xc0))
	bs.Flip(0)
	c.Assert(bs.String(), gc.Equals, "01")
	c.Assert(bs.Bytes()[0], gc.Equals, byte(0x40))
	// bitstring len=16
	bs = NewBitstring(16)
	c.Assert(bs.String(), gc.Equals, "0000000000000000")
	bs.Set(0)
	bs.Set(15)
	c.Assert(bs.String(), gc.Equals, "1000000000000001")
	c.Assert(bs.Bytes()[0], gc.Equals, byte(0x80))
	c.Assert(bs.Bytes()[1], gc.Equals, byte(0x01))
}

func (s *BitstringSuite) TestBsBytes(c *gc.C) {
	bs := NewBitstring(16)
	bs.SetBytes([]byte{0x80, 0x00})
	for i := 0; i < bs.BitLen(); i++ {
		switch i {
		case 0:
			c.Assert(1, gc.Equals, bs.Get(i))
		default:
			c.Assert(0, gc.Equals, bs.Get(i))
		}
	}
}

func (s *BitstringSuite) TestZpToBitstring(c *gc.C) {
	// 00's
	zs := []*Zp{
		Zs(P_SKS, "54945054303302140323349777569652159744"),
		Zs(P_SKS, "301824390735659941098168552847110967299"),
		Zs(P_SKS, "244727299682701342167768131406454086662"),
		Zs(P_SKS, "246090505779456321483693264299682785547"),
		Zs(P_SKS, "132666079786438034357736690869858972940"),
		Zs(P_SKS, "77943753696469936247570506393661277454"),
		Zs(P_SKS, "23196332603806579862361573649796228117"),
		Zs(P_SKS, "4716213446676942567518507102644048922"),
		Zs(P_SKS, "296958268935570641352142910860288566305"),
		Zs(P_SKS, "68302849918166164850536779468406147620"),
		Zs(P_SKS, "218289647857890898753469351137063165732"),
		Zs(P_SKS, "49493891938832871357367248144831830823"),
		Zs(P_SKS, "48291393928127338850139259452183259432"),
		Zs(P_SKS, "86222875303132381523404440898289042729"),
		Zs(P_SKS, "141808492182097342190378424004993438762"),
		Zs(P_SKS, "335945044879925574404988722388729177130"),
		Zs(P_SKS, "236652262168326400305360829310456888110")}
	for _, z := range zs {
		bs := NewZpBitstring(z)
		c.Assert(bs.Get(0), gc.Equals, 0)
		c.Assert(bs.Get(1), gc.Equals, 0)
	}
	// 01's
	zs = []*Zp{
		Zs(P_SKS, "251126436454877830379605469450395348293"),
		Zs(P_SKS, "326459174278123784017559830530989911369"),
		Zs(P_SKS, "33880488004771349616788442031172397900"),
		Zs(P_SKS, "52260317194205405422862037231440764495"),
		Zs(P_SKS, "111966997212661832286471884984785895763"),
		Zs(P_SKS, "65346596413500442902105433236945513815"),
		Zs(P_SKS, "336173260736631828030752156996804317528"),
		Zs(P_SKS, "12549734565921827638994232318473088346"),
		Zs(P_SKS, "236987695883303042196824159729755015004"),
		Zs(P_SKS, "323119199726242591092367701897211489888"),
		Zs(P_SKS, "41584217555254899291679977414635303265"),
		Zs(P_SKS, "118348940079828526226895879854757946209"),
		Zs(P_SKS, "76747152908027187555920496063053721192"),
		Zs(P_SKS, "141520615381718155000336085124075411305"),
		Zs(P_SKS, "12644319991398156892717186701440448617"),
		Zs(P_SKS, "6412217881018912834671730377842624107"),
		Zs(P_SKS, "40272626613761839643433023788839454318"),
		Zs(P_SKS, "317014674893435005836306380764370532983"),
		Zs(P_SKS, "311101531782501702629491213596113502586")}
	for _, z := range zs {
		bs := NewZpBitstring(z)
		c.Assert(bs.Get(0), gc.Equals, 0)
		c.Assert(bs.Get(1), gc.Equals, 1)
	}
	zs = []*Zp{
		Zs(P_SKS, "188716858420292079269415903308294938757"),
		Zs(P_SKS, "59683129049585326195019115368974177413"),
		Zs(P_SKS, "120554134733908208956936621136810387334"),
		Zs(P_SKS, "118284951518112845084965537305254483334"),
		Zs(P_SKS, "334377276241926936152366343232870849927"),
		Zs(P_SKS, "77310576102193849850198786368460166024"),
		Zs(P_SKS, "48414329405458959169482475701469749386"),
		Zs(P_SKS, "250015874231309260492838193181798330515"),
		Zs(P_SKS, "159774163460744271274851987570681581206"),
		Zs(P_SKS, "143198666233767249362774217511755743896"),
		Zs(P_SKS, "240912385064508366145523910482350846106"),
		Zs(P_SKS, "137051103418540437753971021562365500061"),
		Zs(P_SKS, "327716597715155576166988551942487950757"),
		Zs(P_SKS, "183616583934478418670273555233235445158"),
		Zs(P_SKS, "205743196898220792903183384302974895272"),
		Zs(P_SKS, "186204001228737102171059261656423711656"),
		Zs(P_SKS, "252570316860507925711101109364354724777"),
		Zs(P_SKS, "244999449118738179307733396365253793195"),
		Zs(P_SKS, "8050877573458140058716086496555437487"),
		Zs(P_SKS, "53287660286265266199637380204079727539"),
		Zs(P_SKS, "20018245473587526137270198427261386422")}
	for _, z := range zs {
		bs := NewZpBitstring(z)
		c.Assert(bs.Get(0), gc.Equals, 1)
		c.Assert(bs.Get(1), gc.Equals, 0)
	}
}

/*
   conflux - Distributed database synchronization library
	Based on the algorithm described in
		"Set Reconciliation with Nearly Optimal	Communication Complexity",
			Yaron Minsky, Ari Trachtenberg, and Richard Zippel, 2004.

   Copyright (c) 2012-2015  Casey Marshall <cmars@cmarstech.com>

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

// Package recon provides the SKS reconciliation protocol, prefix tree interface
// and an in-memory prefix-tree implementation.
//
// The Conflux recon API is versioned with gopkg. Use in your projects with:
//
// import "gopkg.in/hockeypuck/conflux.v2/recon"
//
package recon

import (
	"fmt"
	"net"
	"strings"

	"github.com/BurntSushi/toml"
	"gopkg.in/errgo.v1"
)

type PartnerMap map[string]Partner

type PTreeConfig struct {
	ThreshMult int `toml:"threshMult"`
	BitQuantum int `toml:"bitQuantum"`
	MBar       int `toml:"mBar"`
}

// Settings holds the configuration settings for the local reconciliation peer.
type Settings struct {
	PTreeConfig

	Version   string     `toml:"version"`
	LogName   string     `toml:"logname" json:"-"`
	HTTPAddr  string     `toml:"httpAddr"`
	HTTPNet   network    `toml:"httpNet" json:"-"`
	ReconAddr string     `toml:"reconAddr"`
	ReconNet  network    `toml:"reconNet" json:"-"`
	Partners  PartnerMap `toml:"partner"`
	Filters   []string   `toml:"filters"`

	// Backwards-compatible keys
	CompatHTTPPort     int      `toml:"httpPort" json:"-"`
	CompatReconPort    int      `toml:"reconPort" json:"-"`
	CompatPartnerAddrs []string `toml:"partners" json:"-"`

	GossipIntervalSecs          int `toml:"gossipIntervalSecs" json:"-"`
	MaxOutstandingReconRequests int `toml:"maxOutstandingReconRequests" json:"-"`
}

type Partner struct {
	HTTPAddr  string  `toml:"httpAddr"`
	HTTPNet   network `toml:"httpNet" json:"-"`
	ReconAddr string  `toml:"reconAddr"`
	ReconNet  network `toml:"reconNet" json:"-"`
}

type network string

const (
	NetworkDefault = network("")
	NetworkTCP     = network("tcp")
	NetworkUnix    = network("unix")
)

// String implements the fmt.Stringer interface.
func (n network) String() string {
	if n == "" {
		return string(NetworkTCP)
	}
	return string(n)
}

func (n network) Resolve(addr string) (net.Addr, error) {
	switch n {
	case NetworkDefault, NetworkTCP:
		return net.ResolveTCPAddr("tcp", addr)
	case NetworkUnix:
		return net.ResolveUnixAddr("unix", addr)
	}
	return nil, errgo.Newf("don't know how to resolve network %q address %q", n, addr)
}

const (
	DefaultVersion                     = "1.1.3"
	DefaultLogName                     = "conflux.recon"
	DefaultHTTPAddr                    = ":11371"
	DefaultReconAddr                   = ":11370"
	DefaultGossipIntervalSecs          = 60
	DefaultMaxOutstandingReconRequests = 100

	DefaultThreshMult = 10
	DefaultBitQuantum = 2
	DefaultMBar       = 5
)

var defaultPTreeConfig = PTreeConfig{
	ThreshMult: DefaultThreshMult,
	BitQuantum: DefaultBitQuantum,
	MBar:       DefaultMBar,
}

var defaultSettings = Settings{
	PTreeConfig: defaultPTreeConfig,

	Version:   DefaultVersion,
	LogName:   DefaultLogName,
	HTTPAddr:  DefaultHTTPAddr,
	ReconAddr: DefaultReconAddr,

	GossipIntervalSecs:          DefaultGossipIntervalSecs,
	MaxOutstandingReconRequests: DefaultMaxOutstandingReconRequests,
}

// Resolve resolves network addresses and backwards-compatible settings. Use
// Resolve after decoding from TOML.
func (s *Settings) Resolve() error {
	if s.CompatHTTPPort != 0 {
		s.HTTPAddr = fmt.Sprintf(":%d", s.CompatHTTPPort)
	}
	if s.CompatReconPort != 0 {
		s.ReconAddr = fmt.Sprintf(":%d", s.CompatReconPort)
	}
	if len(s.CompatPartnerAddrs) > 0 {
		s.Partners = PartnerMap{}
		for _, partnerAddr := range s.CompatPartnerAddrs {
			host, _, err := net.SplitHostPort(partnerAddr)
			if err != nil {
				return errgo.Notef(err, "invalid 'partners' address %q", partnerAddr)
			}
			p := Partner{
				HTTPAddr:  fmt.Sprintf("%s:11371", host),
				ReconAddr: partnerAddr,
			}
			s.Partners[host] = p
		}
	}

	_, err := s.HTTPNet.Resolve(s.HTTPAddr)
	if err != nil {
		return errgo.Notef(err, "invalid httpNet %q httpAddr %q", s.HTTPNet, s.HTTPAddr)
	}
	_, err = s.ReconNet.Resolve(s.ReconAddr)
	if err != nil {
		return errgo.Notef(err, "invalid reconNet %q reconAddr %q", s.ReconNet, s.ReconAddr)
	}

	return nil
}

// ParseSettings parses a TOML-formatted string representation into Settings.
func ParseSettings(data string) (*Settings, error) {
	var doc struct {
		Conflux struct {
			Recon Settings `toml:"recon"`
		} `toml:"conflux"`
	}
	defaults := DefaultSettings()
	doc.Conflux.Recon = *defaults
	_, err := toml.Decode(data, &doc)
	if err != nil {
		return nil, errgo.Mask(err)
	}

	settings := &doc.Conflux.Recon
	err = settings.Resolve()
	if err != nil {
		return nil, errgo.Mask(err)
	}
	return settings, nil
}

// DefaultSettings returns default peer configuration settings.
func DefaultSettings() *Settings {
	settings := defaultSettings
	settings.Partners = make(PartnerMap)
	return &settings
}

func resolveHTTPPortTCP(addr net.Addr) (int, bool) {
	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		return 0, false
	}
	return tcpAddr.Port, true
}

var resolveHTTPPort = resolveHTTPPortTCP

// Config returns a recon protocol config message that described this
// peer's configuration settings.
func (s *Settings) Config() (*Config, error) {
	config := &Config{
		Version:    s.Version,
		BitQuantum: s.BitQuantum,
		MBar:       s.MBar,
		Filters:    strings.Join(s.Filters, ","),
	}

	// Try to obtain httpPort
	addr, err := s.HTTPNet.Resolve(s.HTTPAddr)
	if err != nil {
		return nil, errgo.Notef(err, "invalid httpNet %q httpAddr %q", s.HTTPNet, s.HTTPAddr)
	}
	port, ok := resolveHTTPPort(addr)
	if !ok {
		return nil, errgo.Newf("cannot determine httpPort from httpNet %q httpAddr %q", s.HTTPNet, s.HTTPAddr)
	}
	config.HTTPPort = port
	return config, nil
}

// SplitThreshold returns the maximum number of elements a prefix tree node may
// contain before creating child nodes and distributing the elements among them.
func (c *PTreeConfig) SplitThreshold() int {
	return c.ThreshMult * c.MBar
}

// JoinThreshold returns the minimum cumulative number of elements under a
// prefix tree parent node, below which all child nodes are merged into the
// parent.
func (c *PTreeConfig) JoinThreshold() int {
	return c.SplitThreshold() / 2
}

// NumSamples returns the number of sample points used for interpolation.
// This must match among all reconciliation peers.
func (c *PTreeConfig) NumSamples() int {
	return c.MBar + 1
}

// PartnerAddrs returns the resolved network addresses of configured partner
// peers.
func (s *Settings) PartnerAddrs() ([]net.Addr, error) {
	var addrs []net.Addr
	for _, partner := range s.Partners {
		addr, err := partner.ReconNet.Resolve(partner.ReconAddr)
		if err != nil {
			return nil, errgo.Mask(err)
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

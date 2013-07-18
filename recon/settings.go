package recon

import (
	"bytes"
	"github.com/pelletier/go-toml"
	"net"
	"strconv"
	"strings"
)

type Settings struct {
	*toml.TomlTree
	splitThreshold int
	joinThreshold  int
	numSamples     int
}

func (s *Settings) GetString(key string, defaultValue string) string {
	if s, is := s.GetDefault(key, defaultValue).(string); is {
		return s
	}
	return defaultValue
}

func (s *Settings) GetStrings(key string) (value []string) {
	if strs, is := s.Get(key).([]interface{}); is {
		for _, v := range strs {
			if str, is := v.(string); is {
				value = append(value, str)
			}
		}
	}
	return
}

func (s *Settings) GetInt(key string, defaultValue int) int {
	switch v := s.GetDefault(key, defaultValue).(type) {
	case int:
		return v
	case string:
		i, err := strconv.Atoi(v)
		if err != nil {
			panic(err)
		}
		s.Set(key, i)
		return i
	}
	return defaultValue
}

func (s *Settings) Version() string {
	return s.GetString("conflux.recon.version", "1.1.3")
}

func (s *Settings) LogName() string {
	return s.GetString("conflux.recon.logname", "conflux.recon")
}

func (s *Settings) HttpPort() int {
	return s.GetInt("conflux.recon.httpPort", 11371)
}

func (s *Settings) ReconPort() int {
	return s.GetInt("conflux.recon.reconPort", 11370)
}

func (s *Settings) Partners() []string {
	return s.GetStrings("conflux.recon.partners")
}

func (s *Settings) Filters() []string {
	return s.GetStrings("conflux.recon.filters")
}

func (s *Settings) ThreshMult() int {
	return s.GetInt("conflux.recon.threshMult", DefaultThreshMult)
}

func (s *Settings) BitQuantum() int {
	return s.GetInt("conflux.recon.bitQuantum", DefaultBitQuantum)
}

func (s *Settings) MBar() int {
	return s.GetInt("conflux.recon.mBar", DefaultMBar)
}

func (s *Settings) SplitThreshold() int {
	return s.splitThreshold
}

func (s *Settings) JoinThreshold() int {
	return s.joinThreshold
}

func (s *Settings) NumSamples() int {
	return s.numSamples
}

func (s *Settings) GossipIntervalSecs() int {
	return s.GetInt("conflux.recon.gossipIntervalSecs", 60)
}

func (s *Settings) MaxOutstandingReconRequests() int {
	return s.GetInt("conflux.recon.maxOutstandingReconRequests", 100)
}

func DefaultSettings() (settings *Settings) {
	buf := bytes.NewBuffer(nil)
	var tree *toml.TomlTree
	var err error
	if tree, err = toml.Load(buf.String()); err != nil {
		panic(err) // unlikely
	}
	return NewSettings(tree)
}

func NewSettings(tree *toml.TomlTree) (settings *Settings) {
	settings = &Settings{tree, DefaultSplitThreshold, DefaultJoinThreshold, DefaultNumSamples}
	settings.updateDerived()
	return
}

func (s *Settings) Config() *Config {
	return &Config{
		Version:    s.Version(),
		HttpPort:   s.HttpPort(),
		BitQuantum: s.BitQuantum(),
		MBar:       s.MBar(),
		Filters:    strings.Join(s.Filters(), ",")}
}

func (s *Settings) updateDerived() {
	s.splitThreshold = s.ThreshMult() * s.MBar()
	s.joinThreshold = s.splitThreshold / 2
	s.numSamples = s.MBar() + 1
}

func LoadSettings(path string) (*Settings, error) {
	var tree *toml.TomlTree
	var err error
	if tree, err = toml.LoadFile(path); err != nil {
		return nil, err
	}
	return NewSettings(tree), nil
}

func (s *Settings) PartnerAddrs() (addrs []net.Addr, err error) {
	for _, partner := range s.Partners() {
		if partner == "" {
			continue
		}
		addr, err := net.ResolveTCPAddr("tcp", partner)
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, addr)
	}
	return
}

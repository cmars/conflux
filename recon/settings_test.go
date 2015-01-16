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

package recon

import (
	"testing"

	gc "gopkg.in/check.v1"
)

func Test(t *testing.T) { gc.TestingT(t) }

type SettingsSuite struct{}

var _ = gc.Suite(&SettingsSuite{})

func (s *SettingsSuite) TestParse(c *gc.C) {
	testCases := []struct {
		desc     string
		toml     string
		settings *Settings
		err      string
	}{{
		"empty string",
		``,
		&defaultSettings,
		"",
	}, {
		"field setting with some defaults",
		`
[conflux.recon]
version="2.3.4"
logname="blammo"
filters=["something","else"]
`,
		&Settings{
			PTreeConfig:                 defaultPTreeConfig,
			Version:                     "2.3.4",
			LogName:                     "blammo",
			Filters:                     []string{"something", "else"},
			HTTPAddr:                    ":11371",
			ReconAddr:                   ":11370",
			Partners:                    PartnerMap{},
			GossipIntervalSecs:          DefaultGossipIntervalSecs,
			MaxOutstandingReconRequests: DefaultMaxOutstandingReconRequests,
		},
		"",
	}, {
		"field setting override some defaults",
		`
[conflux.recon]
version="2.3.4"
logname="blammo"
httpAddr="12.23.34.45:11371"
reconAddr="[2001:db8:85a3::8a2e:370:7334]:11370"
filters=["something","else"]
`,
		&Settings{
			PTreeConfig:                 defaultPTreeConfig,
			Version:                     "2.3.4",
			LogName:                     "blammo",
			HTTPAddr:                    "12.23.34.45:11371",
			ReconAddr:                   "[2001:db8:85a3::8a2e:370:7334]:11370",
			Filters:                     []string{"something", "else"},
			Partners:                    PartnerMap{},
			GossipIntervalSecs:          DefaultGossipIntervalSecs,
			MaxOutstandingReconRequests: DefaultMaxOutstandingReconRequests,
		},
		"",
	}, {
		"invalid toml",
		`nope`,
		nil,
		`.*Expected key separator '=', but got '\\n' instead.*`,
	}, {
		"invalid http net",
		`
[conflux.recon]
httpNet="ansible"
`,
		nil,
		`.*don't know how to resolve network \"ansible\" address.*`,
	}, {
		"invalid http net",
		`
[conflux.recon]
httpNet="tcp"
httpAddr="/dev/null"
`,
		nil,
		`.*missing port in address /dev/null.*`,
	}, {
		"invalid recon net",
		`
[conflux.recon]
httpNet="tcp"
httpAddr="1.2.3.4:8080"
reconNet="floo"
reconAddr="flarb"
`,
		nil,
		`.*don't know how to resolve network \"floo\" address \"flarb\".*`,
	}, {
		"invalid recon addr",
		`
[conflux.recon]
httpNet="tcp"
httpAddr="1.2.3.4:8080"
reconNet="tcp"
reconAddr=":nope"
`,
		nil,
		`.*unknown port tcp/nope.*`,
	}, {
		"new-style recon partners",
		`
[conflux.recon]
httpAddr=":11371"
reconAddr=":11370"

[conflux.recon.partner.alice]
httpAddr="1.2.3.4:11371"
reconAddr="5.6.7.8:11370"

[conflux.recon.partner.bob]
httpAddr="4.3.2.1:11371"
reconAddr="8.7.6.5:11370"
`,
		&Settings{
			PTreeConfig:                 defaultPTreeConfig,
			Version:                     DefaultVersion,
			LogName:                     DefaultLogName,
			HTTPAddr:                    DefaultHTTPAddr,
			ReconAddr:                   DefaultReconAddr,
			GossipIntervalSecs:          DefaultGossipIntervalSecs,
			MaxOutstandingReconRequests: DefaultMaxOutstandingReconRequests,
			Partners: map[string]Partner{
				"alice": Partner{
					HTTPAddr:  "1.2.3.4:11371",
					ReconAddr: "5.6.7.8:11370",
				},
				"bob": Partner{
					HTTPAddr:  "4.3.2.1:11371",
					ReconAddr: "8.7.6.5:11370",
				},
			},
		},
		"",
	}, {
		"compat-style config",
		`
[conflux.recon]
httpPort=11371
reconPort=11370
partners=["1.2.3.4:11370","5.6.7.8:11370"]
`,
		&Settings{
			PTreeConfig:                 defaultPTreeConfig,
			Version:                     DefaultVersion,
			LogName:                     DefaultLogName,
			HTTPAddr:                    ":11371",
			ReconAddr:                   ":11370",
			CompatHTTPPort:              11371,
			CompatReconPort:             11370,
			GossipIntervalSecs:          DefaultGossipIntervalSecs,
			MaxOutstandingReconRequests: DefaultMaxOutstandingReconRequests,
			Partners: map[string]Partner{
				"1.2.3.4": Partner{
					HTTPAddr:  "1.2.3.4:11371",
					ReconAddr: "1.2.3.4:11370",
				},
				"5.6.7.8": Partner{
					HTTPAddr:  "5.6.7.8:11371",
					ReconAddr: "5.6.7.8:11370",
				},
			},
			CompatPartnerAddrs: []string{"1.2.3.4:11370", "5.6.7.8:11370"},
		},
		"",
	}}
	for i, testCase := range testCases {
		c.Logf("test#%d: %s", i, testCase.desc)
		settings, err := ParseSettings(testCase.toml)
		if err != nil {
			c.Check(err, gc.ErrorMatches, testCase.err)
		} else {
			c.Check(settings, gc.DeepEquals, testCase.settings)
		}
	}
}

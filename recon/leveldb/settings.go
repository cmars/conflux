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

package leveldb

import (
	"github.com/cmars/conflux/recon"
	"github.com/pelletier/go-toml"
)

type DbSettings struct {
	*recon.Settings
}

func (s *DbSettings) DbPath() string {
	return s.GetString("conflux.recon.leveldb.path", "/var/lib/hockeypuck/ptree-leveldb")
}

func NewSettings(tree *toml.TomlTree) *DbSettings {
	reconSettings := recon.NewSettings(tree)
	return &DbSettings{reconSettings}
}

func DefaultSettings() *DbSettings {
	reconSettings := recon.DefaultSettings()
	return &DbSettings{reconSettings}
}

func LoadSettings(path string) (*DbSettings, error) {
	reconSettings, err := recon.LoadSettings(path)
	return &DbSettings{reconSettings}, err
}

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
package pqptree

import (
	"github.com/cmars/conflux/recon"
)

type Settings struct {
	*recon.Settings
}

func (s *Settings) Driver() string {
	return s.GetString("conflux.recon.sql.driver", "postgres")
}

func (s *Settings) DSN() string {
	return s.GetString("conflux.recon.sql.dsn", "dbname=conflux host=/var/run/postgresql sslmode=disable")
}

func (s *Settings) Namespace() string {
	return s.GetString("conflux.recon.sql.ns", "conflux")
}

func NewSettings(reconSettings *recon.Settings) *Settings {
	return &Settings{reconSettings}
}

func DefaultSettings() *Settings {
	return NewSettings(recon.DefaultSettings())
}

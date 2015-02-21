package leveldb

import (
	"gopkg.in/hockeypuck/conflux.v2/recon"
)

type Config struct {
	Path string `toml:"path"`
}

type Settings struct {
	recon.Settings

	LevelDB Config `toml:"leveldb"`
}

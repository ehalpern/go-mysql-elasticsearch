package river

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

type SourceConfig struct {
	Schema string   `toml:"schema"`
	Tables []string `toml:"tables"`
}

type Config struct {
	ConfigFile     string
	MyAddr         string `toml:"my_addr"`
	MyUser         string `toml:"my_user"`
	MyPassword     string `toml:"my_pass"`

	ESAddr         string `toml:"es_addr"`

	StatAddr       string `toml:"stat_addr"`

	ServerID       uint32 `toml:"server_id"`
	Flavor         string `toml:"flavor"`
	DataDir        string `toml:"data_dir"`

	DumpExec       string `toml:"mysqldump"`

	Sources        []SourceConfig `toml:"source"`
	MaxBulkActions int `toml:"max_actions"`

	Rules          []*Rule `toml:"rule"`
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	c, err := NewConfig(string(data));
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.ConfigFile = name
	return c, nil
}

func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if c.MaxBulkActions == 0 {
		c.MaxBulkActions = 1
	}
	return &c, nil
}


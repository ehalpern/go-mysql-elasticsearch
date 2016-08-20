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
	ConfigFile   string

	DbHost       string `toml:"db_host"`
	DbUser       string `toml:"db_user"`
	DbPassword   string `toml:"db_pass"`
	DbSlaveID    uint32 `toml:"db_slave_id"`

	EsHost       string `toml:"es_host"`
	EsMaxActions int    `toml:"es_max_actions"`

	StatAddr     string `toml:"stat_addr"`

	DataDir      string `toml:"data_dir"`
	DumpExec     string

	Sources      []SourceConfig `toml:"source"`
	Rules        []*Rule `toml:"rule"`
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	c, err := NewConfig(string(data));
	if err != nil {
		return nil, err
	}
	c.ConfigFile = name
	return c, nil
}

func NewConfig(data string) (*Config, error) {
	c := NewDefaultConfig()
	if _, err := toml.Decode(data, c); err != nil {
		return nil, errors.Trace(err)
	}
	if c.EsMaxActions == 0 {
		c.EsMaxActions = 1
	}
	return c, nil
}

func NewDefaultConfig() *Config {
	return &Config{
		ConfigFile: "./etc/river.toml",
		DbHost: "127.0.0.1:3306",
		DbUser: "root",
		DbSlaveID: 1001,
		DbPassword: "",
		EsHost: "127.0.0.1:9200",
		EsMaxActions: 100,
		DataDir: "./var",
		DumpExec: "mydumper",
	}
}


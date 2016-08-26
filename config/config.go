package config

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
)

const (
	ServiceName = "mysql2es"
	ServiceDesc = "mysql to elasticsearch replication"
)

type Config struct {
	ConfigFile   string
	DataDir      string `toml:"data_dir"`
	DbHost       string `toml:"db_host"`
	DbUser       string `toml:"db_user"`
	DbPassword   string `toml:"db_pass"`
	DbSlaveID    uint32 `toml:"db_slave_id"`
	EsHost       string `toml:"es_host"`
	EsMaxActions int    `toml:"es_max_actions"`
	DumpExec     string
	Sources      []SourceConfig `toml:"source"`
	Rules        []*Rule `toml:"rule"`
}

type SourceConfig struct {
	Schema string   `toml:"schema"`
	Tables []string `toml:"tables"`
}

var Default = Config {
	"/etc/" + ServiceName + "/" + ServiceName + ".toml",
	"/var/lib/" + ServiceName,
	"127.0.0.1:3306",
	"root",
	"",
	1001,
	"127.0.0.1:9200",
	1,
	"mydumper",
	[]SourceConfig{},
	[]*Rule{},
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, err
	}

	c, err := NewConfig(string(data));
	if err != nil {
		return nil, err
	}
	c.ConfigFile = name
	return c, nil
}

func NewConfig(data string) (*Config, error) {
	c := Default
	if _, err := toml.Decode(data, &c); err != nil {
		return nil, err
	}
	if c.EsMaxActions == 0 {
		c.EsMaxActions = 1
	}
	return &c, nil
}
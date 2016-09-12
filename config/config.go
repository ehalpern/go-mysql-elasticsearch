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
	EsMaxBytes   int64  `toml:"es_max_bytes"`
	DumpExec     string `toml:"dump_exec"`
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
	0,
	99 * 1024 * 1024,
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
	return &c, nil
}
package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaults(t *testing.T) {
	c, err := NewConfig("")
	assert.Nil(t, err)
	assert.Equal(t, Default.DataDir, c.DataDir)
	assert.Equal(t, Default.DbHost, c.DbHost)
	assert.Equal(t, Default.DbUser, c.DbUser)
	assert.Equal(t, Default.DbPassword, c.DbPassword)
	assert.Equal(t, Default.DbSlaveID, c.DbSlaveID)
	assert.Equal(t, Default.EsHost, c.EsHost)
	assert.Equal(t, Default.EsMaxActions, c.EsMaxActions)
	assert.Equal(t, Default.EsMaxBytes, c.EsMaxBytes)
}

func TestOverrides(t *testing.T) {
	c, err := NewConfig(`
data_dir = "./var/test"
db_host = "db.test.com:3306"
db_user = "user1"
db_pass = "password1"
db_slave_id = 4
es_host = "es.test.com:9200"
es_max_actions = 50
es_max_bytes = 5000000
`)
	assert.Nil(t, err)
	assert.Equal(t, "./var/test", c.DataDir)
	assert.Equal(t, "db.test.com:3306", c.DbHost)
	assert.Equal(t, "user1", c.DbUser)
	assert.Equal(t, "password1", c.DbPassword)
	assert.Equal(t, uint32(4), c.DbSlaveID)
	assert.Equal(t, "es.test.com:9200", c.EsHost)
	assert.Equal(t, 50, c.EsMaxActions)
	assert.Equal(t, int64(5000000), c.EsMaxBytes)
}

func TestRules(t *testing.T) {
	cfg, err := NewConfig(`
[[source]]
schema = "test"
tables = ["table1", "table2"]
[[rule]]
schema = "test"
table  = "table1"
index  = "table1_idx"
indexFile = "table1.json"
type   = "table1_type"
[[rule]]
schema = "test"
table  = "table2"
index  = "table2_idx"
indexFile = "table2.json"
type   = "table2_type"
parent = "table1_type"
`)
	assert.Nil(t, err)
	assert.Len(t, cfg.Sources, 1)
	assert.Len(t, cfg.Sources[0].Tables, 2)
	assert.Equal(t, []string{"table1", "table2"}, cfg.Sources[0].Tables)
	assert.Len(t, cfg.Rules, 2)
	assert.Equal(t, &Rule{"test", "table1", "table1_idx", "table1_type", "", "table1.json", nil, nil}, cfg.Rules[0])
	assert.Equal(t, &Rule{"test", "table2", "table2_idx", "table2_type", "table1_type", "table2.json", nil, nil}, cfg.Rules[1])
}

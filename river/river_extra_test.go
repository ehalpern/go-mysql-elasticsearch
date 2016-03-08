package river

import (
	"fmt"
	"os"

	. "gopkg.in/check.v1"
)

func (s *riverTestSuite) setupExtra(c *C) (r *River) {
	var err error

	schema := `
        CREATE TABLE IF NOT EXISTS %s (
            id INT,
            title VARCHAR(256),
            pid INT,
            PRIMARY KEY(id)) ENGINE=INNODB;
    `

	s.testExecute(c, "DROP TABLE IF EXISTS test_river_extra")
	s.testExecute(c, fmt.Sprintf(schema, "test_river_extra"))

	schema = `
        CREATE TABLE IF NOT EXISTS %s (
            id INT,
            PRIMARY KEY(id)) ENGINE=INNODB;
    `

	s.testExecute(c, "DROP TABLE IF EXISTS test_river_parent")
	s.testExecute(c, fmt.Sprintf(schema, "test_river_parent"))

	cfg := new(Config)
	cfg.MyAddr = *my_addr
	cfg.MyUser = *my_user
	cfg.MyPassword = *my_pass
	cfg.ESAddr = *es_addr

	cfg.ServerID = 1001
	cfg.Flavor = "mysql"

	cfg.DataDir = "/tmp/test_river_extra"
	cfg.DumpExec = "mydumper"

	cfg.StatAddr = "127.0.0.1:12800"

	os.RemoveAll(cfg.DataDir)

	cfg.Sources = []SourceConfig{SourceConfig{Schema: "test", Tables: []string{"test_river_extra", "test_river_parent"}}}

	cfg.Rules = []*Rule{
		&Rule{Schema: "test",
			Table: "test_river_parent",
			Index: "river",
			Type:  "river_extra_parent"},
		&Rule{Schema: "test",
			Table:  "test_river_extra",
			Index:  "river",
			Type:   "river_extra",
			Parent: "pid"}}

	r, err = NewRiver(cfg)
	c.Assert(err, IsNil)

	mapping := map[string]interface{}{
		"river_extra": map[string]interface{}{
			"_parent": map[string]string{"type": "river_extra_parent"},
		},
	}

	r.es.PutMapping().Index("river").Type("river_extra").BodyJson(mapping)

	return r
}

func (s *riverTestSuite) testPrepareExtraData(c *C) {
	s.testExecute(c, "INSERT INTO test_river_parent (id) VALUES (?)", 1)
	s.testExecute(c, "INSERT INTO test_river_extra (id, title, pid) VALUES (?, ?, ?)", 1, "first", 1)
	s.testExecute(c, "INSERT INTO test_river_extra (id, title, pid) VALUES (?, ?, ?)", 2, "second", 1)
	s.testExecute(c, "INSERT INTO test_river_extra (id, title, pid) VALUES (?, ?, ?)", 3, "third", 1)
	s.testExecute(c, "INSERT INTO test_river_extra (id, title, pid) VALUES (?, ?, ?)", 4, "fourth", 1)
}

func (s *riverTestSuite) testElasticExtraExists(c *C, id string, parent string, exist bool) {
	exists, _ := s.r.es.Exists().Index("river").Type("river_extra").Parent(parent).Id(id).Do()
	c.Assert(exists, Equals, true)
}

func (s *riverTestSuite) IgnoreTestRiverWithParent(c *C) {
	river := s.setupExtra(c)

	defer river.Close()

	s.testPrepareExtraData(c)

	go river.Run()

	<-river.canal.WaitDumpDone()

	s.testElasticExtraExists(c, "1", "1", true)

	s.testExecute(c, "DELETE FROM test_river_extra WHERE id = ?", 1)
	err := river.canal.CatchMasterPos(1)
	c.Assert(err, IsNil)

	s.testElasticExtraExists(c, "1", "1", false)
}

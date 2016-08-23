package river

import (
	"fmt"
	"os"

	. "gopkg.in/check.v1"
	"encoding/json"
	"github.com/ehalpern/go-mysql-elasticsearch/config"
)

const (
	testDb = "test"
	testIgnoreDb = "test_ignore"
	testExtraTable = "river_extra"
	testExtraIndex = "river"
	testExtraType = "extra"
	testParentTable = "test_river_parent"
	testParentType = "parent"
	testIgnoreTable = "test_river_ignore"
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
	s.testPrepareTable(c, testDb, testExtraTable, schema)

	schema = `
        CREATE TABLE IF NOT EXISTS %s (
            id INT,
            PRIMARY KEY(id)) ENGINE=INNODB;
    `
	s.testPrepareTable(c, testDb, testParentTable, schema)

	s.testPrepareTable(c, testIgnoreDb, testExtraTable, schema)
	s.testPrepareTable(c, testDb, testIgnoreTable, schema)

	cfg := new(config.Config)
	cfg.DbHost = *my_addr
	cfg.DbUser = *my_user
	cfg.DbPassword = *my_pass
	cfg.EsHost = *es_addr

	cfg.DbSlaveID = 3001

	cfg.DataDir = "/tmp/test_river_extra"
	cfg.EsMaxActions = 0 // forces flush on every replication event; required for
	                       // TestSchemaUpgrade

	os.RemoveAll(cfg.DataDir)

	cfg.Sources = []config.SourceConfig{config.SourceConfig{Schema: testDb, Tables: []string{testExtraTable, testParentTable}}}

	cfg.Rules = []*config.Rule{
		&config.Rule{Schema: testDb,
			Table: testParentTable,
			Index: testExtraIndex,
			Type:  testParentType},
		&config.Rule{Schema: testDb,
			Table:  testExtraTable,
			Index:  testExtraIndex,
			Type:   testExtraType,
			Parent: "pid"}}

	r, err = NewRiver(cfg)
	c.Assert(err, IsNil)

	mapping := map[string]interface{} {
		"mappings": map[string]interface{} {
			testExtraType: map[string]interface{} {
				"_parent": map[string]string{"type": testParentType},
			},
		},
	}
	r.es.DeleteIndex(testExtraIndex).Do() // ignore failures
	_, err = r.es.CreateIndex(testExtraIndex).BodyJson(mapping).Do()
	c.Assert(err, IsNil)

	return r
}

func (s *riverTestSuite) testPrepareTable(c *C, db string, table string, schema string) {
	fullName := db + "." + table
	s.dbExec(c, "CREATE DATABASE IF NOT EXISTS " + db)
	s.dbExec(c, "DROP TABLE IF EXISTS " + fullName)
	s.dbExec(c, fmt.Sprintf(schema, fullName))
}

func (s *riverTestSuite) testPrepareExtraData(c *C) {
	s.dbExec(c, "INSERT INTO "+testParentTable+" (id) VALUES (?)", 1)

	s.dbExec(c, "INSERT INTO "+testExtraTable+" (id, title, pid) VALUES (?, ?, ?)", 1, "first", 1)
	s.dbExec(c, "INSERT INTO "+testExtraTable+" (id, title, pid) VALUES (?, ?, ?)", 2, "second", 1)
	s.dbExec(c, "INSERT INTO "+testExtraTable+" (id, title, pid) VALUES (?, ?, ?)", 3, "third", 1)
	s.dbExec(c, "INSERT INTO "+testExtraTable+" (id, title, pid) VALUES (?, ?, ?)", 4, "fourth", 1)
}

func (s *riverTestSuite) testElasticExtraExists(c *C, id string, parent string, exist bool) {
	req := s.r.es.Exists().Index(testExtraIndex).Type(testExtraType)
	if parent != "" {
		req = req.Parent(parent)
	}
	exists, _ := req.Id(id).Do()
	c.Assert(exists, Equals, exist)
}

func (s *riverTestSuite) testElasticExtraDoc(c *C, id string) map[string]interface{} {
	result, err := s.r.es.Get().Index(testExtraIndex).Type(testExtraType).Parent("1").Id(id).Do()
	c.Assert(err, IsNil)
	bytes, err := result.Source.MarshalJSON()
	c.Assert(err, IsNil)
	var returnedDoc map[string]interface{}
	err = json.Unmarshal(bytes, &returnedDoc)
	c.Assert(err, IsNil)
	return returnedDoc
}

func (s *riverTestSuite) TestRiverWithParent(c *C) {
	river := s.setupExtra(c)

	defer river.Close()

	s.testPrepareExtraData(c)

	go river.Run()

	<-river.canal.WaitDumpDone()

	s.testElasticExtraExists(c, "1", "1", true)

	// Make sure inserting into ignored tables doesn't break anything
	s.dbExec(c, "INSERT INTO "+testIgnoreDb+"."+testExtraTable+" (id) VALUES (?)", 1)
	s.dbExec(c, "INSERT INTO "+testDb+"."+testIgnoreTable+" (id) VALUES (?)", 1)

	s.dbExec(c, "DELETE FROM "+testExtraTable+" WHERE id = ?", 1)
	err := river.canal.CatchMasterPos(10)
	c.Assert(err, IsNil)

	s.testElasticExtraExists(c, "1", "1", false)
}

func (s *riverTestSuite) TestSchemaUpgrade(c *C) {
	river := s.setupExtra(c)

	defer river.Close()

	s.testPrepareExtraData(c)

	go river.Run()

	<-river.canal.WaitDumpDone()

	s.testElasticExtraExists(c, "1", "1", true)

	// Make sure inserting into ignored tables doesn't break anything
	s.dbExec(c, "ALTER TABLE "+testExtraTable+" ADD new VARCHAR(256) DEFAULT 'not-set'")
	s.dbExec(c, "UPDATE "+testExtraTable+" SET new='set' WHERE id=1")
	err := river.canal.CatchMasterPos(10)
	c.Assert(err, IsNil)
	doc := s.testElasticExtraDoc(c, "1")
	c.Assert(doc["new"], Equals, "set")
}

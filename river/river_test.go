package river

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/ehalpern/go-mysql/client"
	"github.com/ehalpern/go-mysql-elasticsearch/config"
	. "gopkg.in/check.v1"
	"runtime/debug"
	"gopkg.in/olivere/elastic.v3"
)

var my_addr = flag.String("my_host", "127.0.0.1:3306", "MySQL addr")
var my_user = flag.String("my_user", "root", "MySQL user")
var my_pass = flag.String("my_pass", "", "MySQL password")
var es_addr = flag.String("es_addr", "127.0.0.1:9200", "Elasticsearch addr")
var useRds = flag.Bool("use_rds", false, "true if using RDS")

func Test(t *testing.T) {
	TestingT(t)
}

type riverTestSuite struct {
	c *client.Conn
	r *River
	es *EsTestClient
}

var _ = Suite(&riverTestSuite{})

func (s *riverTestSuite) SetUpSuite(c *C) {
	debug.SetTraceback("all")
	var err error
	s.c, err = client.Connect(*my_addr, *my_user, *my_pass, "test")
	c.Assert(err, IsNil)
	if !*useRds {
		s.dbExec(c, "SET SESSION binlog_format = 'ROW'")
	}

	s.setupTestTable(c, "test", "test_river")
	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.setupTestTable(c, "test", table)
	}

	cfg := new(config.Config)
	cfg.DbHost = *my_addr
	cfg.DbUser = *my_user
	cfg.DbPassword = *my_pass
	cfg.EsHost = *es_addr
	cfg.DbSlaveID = 2001
	cfg.DataDir = "/tmp/test_river"

	os.RemoveAll(cfg.DataDir)

	cfg.Sources = []config.SourceConfig{config.SourceConfig{Schema: "test", Tables: []string{"test_river", "test_river_[0-9]{4}"}}}

	cfg.Rules = []*config.Rule{
		&config.Rule{Schema: "test",
			Table:        "test_river",
			Index:        "river",
			Type:         "river",
			FieldMapping: map[string]string{"title": "es_title", "mylist": "es_mylist,list"},
		},

		&config.Rule{Schema: "test",
			Table:        "test_river_[0-9]{4}",
			Index:        "river",
			Type:         "river",
			FieldMapping: map[string]string{"title": "es_title", "mylist": "es_mylist,list"},
		},
	}

	s.r, err = NewRiver(cfg)
	c.Assert(err, IsNil)

	_, err = s.r.es.DeleteIndex("river").Do()
}

func (s *riverTestSuite) setupTestTable(c *C, db string, table string) {
	table = db + "." + table
	s.dbExec(c, "CREATE DATABASE IF NOT EXISTS " + db)
	schema := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            id INT,
            title VARCHAR(256),
            content VARCHAR(256),
            mylist VARCHAR(256),
            tenum ENUM("e1", "e2", "e3"),
            tset SET("a", "b", "c"),
            PRIMARY KEY(id)) ENGINE=INNODB;`, table)

	s.dbExec(c, "CREATE DATABASE IF NOT EXISTS " + db)
	s.dbExec(c, "DROP TABLE IF EXISTS " + table)
	s.dbExec(c, schema)
}

func (s *riverTestSuite) TearDownSuite(c *C) {
	if s.c != nil {
		s.c.Close()
	}

	if s.r != nil {
		s.r.Close()
	}
}

func (s *riverTestSuite) dbExec(c *C, query string, args ...interface{}) {
	_, err := s.c.Execute(query, args...)
	c.Assert(err, IsNil)
}

func (s *riverTestSuite) testPrepareData(c *C) {
	s.dbExec(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 1, "first", "hello go 1", "e1", "a,b")
	s.dbExec(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 2, "second", "hello mysql 2", "e2", "b,c")
	s.dbExec(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 3, "third", "hello elaticsearch 3", "e3", "c")
	s.dbExec(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 4, "fouth", "hello go-mysql-elasticserach 4", "e1", "a,b,c")

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.dbExec(c, fmt.Sprintf("INSERT INTO %s (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", table), 5+i, "abc", "hello", "e1", "a,b,c")
	}
}

func (s *riverTestSuite) testElasticGet(c *C, id string) (*elastic.GetResult, map[string]interface{}) {
	var source map[string]interface{}
	resp, err := s.r.es.Get().Index("river").Type("river").Id(id).Do()
	if elastic.IsNotFound(err) {
		return resp, nil
	}
	c.Assert(err, IsNil)
	bytes, err := resp.Source.MarshalJSON()
	c.Assert(err, IsNil)
	err = json.Unmarshal(bytes, &source)
	c.Assert(err, IsNil)
	return resp, source
}


func (s *riverTestSuite) testWaitSyncDone(c *C) {
	err := s.r.canal.CatchMasterPos(10)
	c.Assert(err, IsNil)
}

func (s *riverTestSuite) TestRiver(c *C) {
	s.testPrepareData(c)

	go s.r.Run()

	c.Logf("Waiting for dump to complete")
	<-s.r.canal.WaitDumpDone()
	c.Logf("Dump completed")

	r, source := s.testElasticGet(c, "1")
	c.Assert(r.Found, Equals, true)

	c.Assert(source["tenum"], Equals, "e1")
	c.Assert(source["tset"], Equals, "a,b")

	r, doc := s.testElasticGet(c, "100")
	c.Assert(doc, IsNil)

	for i := 0; i < 10; i++ {
		r, source = s.testElasticGet(c, fmt.Sprintf("%d", 5+i))
		c.Assert(r.Found, Equals, true)
		c.Assert(source["es_title"], Equals, "abc")
	}

	s.dbExec(c, "UPDATE test_river SET title = ?, tenum = ?, tset = ?, mylist = ? WHERE id = ?", "second 2", "e3", "a,b,c", "a,b,c", 2)
	s.dbExec(c, "DELETE FROM test_river WHERE id = ?", 1)
	s.dbExec(c, "UPDATE test_river SET title = ?, id = ? WHERE id = ?", "second 30", 30, 3)

	// so we can insert invalid data
	s.dbExec(c, `SET SESSION sql_mode="NO_ENGINE_SUBSTITUTION";`)

	// bad insert
	s.dbExec(c, "UPDATE test_river SET title = ?, tenum = ?, tset = ? WHERE id = ?", "second 2", "e5", "a,b,c,d", 4)

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.dbExec(c, fmt.Sprintf("UPDATE %s SET title = ? WHERE id = ?", table), "hello", 5+i)
	}

	c.Logf("Waiting for sync to complete")
	s.testWaitSyncDone(c)
	c.Logf("Sync completed")

	r, source = s.testElasticGet(c, "1")
	c.Assert(source, IsNil)

	r, source = s.testElasticGet(c, "2")
	c.Assert(r.Found, Equals, true)
	c.Assert(source["es_title"], Equals, "second 2")
	c.Assert(source["tenum"], Equals, "e3")
	c.Assert(source["tset"], Equals, "a,b,c")
	c.Assert(source["es_mylist"], DeepEquals, []interface{}{"a", "b", "c"})

	r, source = s.testElasticGet(c, "4")
	c.Assert(r.Found, Equals, true)
	c.Assert(source["tenum"], Equals, "")
	c.Assert(source["tset"], Equals, "a,b,c")

	r, source = s.testElasticGet(c, "3")
	c.Assert(source, IsNil)

	r, source = s.testElasticGet(c, "30")
	c.Assert(r.Found, Equals, true)
	c.Assert(source["es_title"], Equals, "second 30")

	for i := 0; i < 10; i++ {
		r, source = s.testElasticGet(c, fmt.Sprintf("%d", 5+i))
		c.Assert(r.Found, Equals, true)
		c.Assert(source["es_title"], Equals, "hello")
	}
}


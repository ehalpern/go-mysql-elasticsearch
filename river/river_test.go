package river

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/ehalpern/go-mysql/client"
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
}

var _ = Suite(&riverTestSuite{})

func (s *riverTestSuite) SetUpSuite(c *C) {
	debug.SetTraceback("single")
	var err error
	s.c, err = client.Connect(*my_addr, *my_user, *my_pass, "test")
	c.Assert(err, IsNil)
	if !*useRds {
		s.testExecute(c, "SET SESSION binlog_format = 'ROW'")
	}

	schema := `
        CREATE TABLE IF NOT EXISTS %s (
            id INT,
            title VARCHAR(256),
            content VARCHAR(256),
            mylist VARCHAR(256),
            tenum ENUM("e1", "e2", "e3"),
            tset SET("a", "b", "c"),
            PRIMARY KEY(id)) ENGINE=INNODB;
    `

	s.testExecute(c, "DROP TABLE IF EXISTS test_river")
	s.testExecute(c, fmt.Sprintf(schema, "test_river"))

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(c, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		s.testExecute(c, fmt.Sprintf(schema, table))
	}

	cfg := new(Config)
	cfg.MyAddr = *my_addr
	cfg.MyUser = *my_user
	cfg.MyPassword = *my_pass
	cfg.ESAddr = *es_addr

	cfg.ServerID = 1001
	cfg.Flavor = "mysql"

	cfg.DataDir = "/tmp/test_river"
	cfg.DumpExec = "mydumper"

	cfg.StatAddr = "127.0.0.1:12800"

	os.RemoveAll(cfg.DataDir)

	cfg.Sources = []SourceConfig{SourceConfig{Schema: "test", Tables: []string{"test_river", "test_river_[0-9]{4}"}}}

	cfg.Rules = []*Rule{
		&Rule{Schema: "test",
			Table:        "test_river",
			Index:        "river",
			Type:         "river",
			FieldMapping: map[string]string{"title": "es_title", "mylist": "es_mylist,list"},
		},

		&Rule{Schema: "test",
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

func (s *riverTestSuite) TearDownSuite(c *C) {
	if s.c != nil {
		s.c.Close()
	}

	if s.r != nil {
		s.r.Close()
	}
}

func (s *riverTestSuite) TestConfig(c *C) {
	str := `
my_addr = "127.0.0.1:3306"
my_user = "root"
my_pass = ""

es_addr = "127.0.0.1:9200"

data_dir = "./var"

[[source]]
schema = "test"

tables = ["test_river", "test_river_[0-9]{4}"]

[[rule]]
schema = "test"
table = "test_river"
index = "river"
type = "river"
parent = "pid"

    [rule.field]
    title = "es_title"
    mylist = "es_mylist,list"

[[rule]]
schema = "test"
table = "test_river_[0-9]{4}"
index = "river"
type = "river"

    [rule.field]
    title = "es_title"
    mylist = "es_mylist,list"

`

	cfg, err := NewConfig(str)
	c.Assert(err, IsNil)
	c.Assert(cfg.Sources, HasLen, 1)
	c.Assert(cfg.Sources[0].Tables, HasLen, 2)
	c.Assert(cfg.Rules, HasLen, 2)
}

func (s *riverTestSuite) testExecute(c *C, query string, args ...interface{}) {
	_, err := s.c.Execute(query, args...)
	c.Assert(err, IsNil)
}

func (s *riverTestSuite) testPrepareData(c *C) {
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 1, "first", "hello go 1", "e1", "a,b")
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 2, "second", "hello mysql 2", "e2", "b,c")
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 3, "third", "hello elaticsearch 3", "e3", "c")
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 4, "fouth", "hello go-mysql-elasticserach 4", "e1", "a,b,c")

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(c, fmt.Sprintf("INSERT INTO %s (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", table), 5+i, "abc", "hello", "e1", "a,b,c")
	}
}

func (s *riverTestSuite) testElasticGet(c *C, id string) (*elastic.GetResult, map[string]interface{}) {
	resp, err := s.r.es.Get().Index("river").Type("river").Id(id).Do()
	c.Assert(err, IsNil)
	bytes, err := resp.Source.MarshalJSON()
	c.Assert(err, IsNil)
	var source map[string]interface{}
	err = json.Unmarshal(bytes, source)
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

	<-s.r.canal.WaitDumpDone()

	r, source := s.testElasticGet(c, "1")
	c.Assert(r.Found, Equals, true)

	c.Assert(source["tenum"], Equals, "e1")
	c.Assert(source["tset"], Equals, "a,b")

	r, _ = s.testElasticGet(c, "100")
	c.Assert(r.Found, Equals, false)

	for i := 0; i < 10; i++ {
		r, source = s.testElasticGet(c, fmt.Sprintf("%d", 5+i))
		c.Assert(r.Found, Equals, true)
		c.Assert(source["es_title"], Equals, "abc")
	}

	s.testExecute(c, "UPDATE test_river SET title = ?, tenum = ?, tset = ?, mylist = ? WHERE id = ?", "second 2", "e3", "a,b,c", "a,b,c", 2)
	s.testExecute(c, "DELETE FROM test_river WHERE id = ?", 1)
	s.testExecute(c, "UPDATE test_river SET title = ?, id = ? WHERE id = ?", "second 30", 30, 3)

	// so we can insert invalid data
	s.testExecute(c, `SET SESSION sql_mode="NO_ENGINE_SUBSTITUTION";`)

	// bad insert
	s.testExecute(c, "UPDATE test_river SET title = ?, tenum = ?, tset = ? WHERE id = ?", "second 2", "e5", "a,b,c,d", 4)

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(c, fmt.Sprintf("UPDATE %s SET title = ? WHERE id = ?", table), "hello", 5+i)
	}

	s.testWaitSyncDone(c)

	r, source = s.testElasticGet(c, "1")
	c.Assert(r.Found, Equals, false)

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

	r, _ = s.testElasticGet(c, "3")
	c.Assert(r.Found, Equals, false)

	r, source = s.testElasticGet(c, "30")
	c.Assert(r.Found, Equals, true)
	c.Assert(source["es_title"], Equals, "second 30")

	for i := 0; i < 10; i++ {
		r, source = s.testElasticGet(c, fmt.Sprintf("%d", 5+i))
		c.Assert(r.Found, Equals, true)
		c.Assert(source["es_title"], Equals, "hello")
	}
}

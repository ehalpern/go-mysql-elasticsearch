package river

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime/debug"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	. "gopkg.in/check.v1"

	"github.com/ehalpern/go-mysql-elasticsearch/config"
	"gopkg.in/olivere/elastic.v3"
)

const (
	tSchema = "test"
	tTable = "test_river"
	tIndex = "river"
    tType = "river"
)

func tTable_(n int) string {
	return fmt.Sprintf(tTable +"_%04d", n)
}

var my_addr = flag.String("my_host", "127.0.0.1:3306", "MySQL addr")
var my_user = flag.String("my_user", "root", "MySQL user")
var my_pass = flag.String("my_pass", "", "MySQL password")
var es_addr = flag.String("es_addr", "127.0.0.1:9200", "Elasticsearch addr")
var useRds = flag.Bool("use_rds", false, "true if using RDS")

func Test(t *testing.T) {
	TestingT(t)
}

type riverTestSuite struct {
	db *sql.DB
	r  *River
	es *EsTestClient
}

var _ = Suite(&riverTestSuite{})

func (s *riverTestSuite) SetUpSuite(c *C) {
	debug.SetTraceback("all")
	var err error
	s.db, err = sql.Open("mysql", *my_user+":"+*my_pass+"@tcp("+*my_addr+")/"+ tSchema)
	c.Assert(err, IsNil)
	if !*useRds {
		s.dbExec(c, "SET SESSION binlog_format = 'ROW'")
	}

	s.dbCreate(c, tSchema, tTable)
	for i := 0; i < 10; i++ {
		s.dbCreate(c, tSchema, tTable_(i))
	}

	s.r = s.riverCreate(c, `
		db_host = "`+*my_addr+`"
		db_user = "`+*my_user+`"
		db_password = "`+*my_pass+`"
		es_host = "`+*es_addr+`"
		data_dir = "/tmp/mysql2es/test"

		[[source]]
			schema = "test"
			tables = ["test_river", "test_river_[0-9]{4}"]
			[[rule]]
				schema = "test"
				table = "test_river"
				index = "river"
				type  = "river"
				[rule.field]
					title = "es_title"
					mylist = "es_mylist,list"
			[[rule]]
				schema = "test"
				table = "test_river_[0-9]{4}"
				index = "river"
				type  = "river"
				[rule.field]
					title = "es_title"
					mylist = "es_mylist,list"
	`)

	_, err = s.r.es.DeleteIndex(tIndex).Do()
}

func (s *riverTestSuite) TearDownSuite(c *C) {
	if s.db != nil {
		s.db.Close()
	}
	if s.r != nil {
		s.r.Close()
	}
}



func (s *riverTestSuite) TestRiver(c *C) {
	s.dbInsertData(c)
	s.riverRun(c)

	r, source := s.esGet(c, "1")
	c.Assert(r.Found, Equals, true)

	c.Assert(source["tenum"], Equals, "e1")
	c.Assert(source["tset"], Equals, "a,b")

	r, doc := s.esGet(c, "100")
	c.Assert(doc, IsNil)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%d", 5+i)
		r, source = s.esGet(c, id)
		c.Assert(r.Found, Equals, true)
		c.Assert(source["es_title"], Equals, id + "th")
	}

	s.dbExec(c, "UPDATE "+ tTable +" SET title = ?, tenum = ?, tset = ?, mylist = ? WHERE id = ?", "second 2", "e3", "a,b,c", "a,b,c", 2)
	s.dbExec(c, "DELETE FROM "+ tTable +" WHERE id = ?", 1)
	s.dbExec(c, "UPDATE "+ tTable +" SET title = ?, id = ? WHERE id = ?", "second 30", 30, 3)

	// so we can insert invalid data
	s.dbExec(c, `SET SESSION sql_mode="NO_ENGINE_SUBSTITUTION";`)

	// bad insert
	s.dbExec(c, "UPDATE "+ tTable +" SET title = ?, tenum = ?, tset = ? WHERE id = ?", "second 2", "e5", "a,b,c,d", 4)

	for i := 0; i < 10; i++ {
		s.dbExec(c, "UPDATE "+ tTable_(i)+" SET title = ? WHERE id = ?", "hello", 5+i)
	}

	s.riverWaitForSync(c)

	r, source = s.esGet(c, "1")
	c.Assert(source, IsNil)

	r, source = s.esGet(c, "2")
	c.Assert(r.Found, Equals, true)
	c.Assert(source["es_title"], Equals, "second 2")
	c.Assert(source["tenum"], Equals, "e3")
	c.Assert(source["tset"], Equals, "a,b,c")
	c.Assert(source["es_mylist"], DeepEquals, []interface{}{"a", "b", "c"})

	r, source = s.esGet(c, "4")
	c.Assert(r.Found, Equals, true)
	c.Assert(source["tenum"], Equals, "")
	c.Assert(source["tset"], Equals, "a,b,c")

	r, source = s.esGet(c, "3")
	c.Assert(source, IsNil)

	r, source = s.esGet(c, "30")
	c.Assert(r.Found, Equals, true)
	c.Assert(source["es_title"], Equals, "second 30")

	for i := 0; i < 10; i++ {
		r, source = s.esGet(c, fmt.Sprintf("%d", 5+i))
		c.Assert(r.Found, Equals, true)
		c.Assert(source["es_title"], Equals, "hello")
	}
}


func (s *riverTestSuite) dbCreate(c *C, db string, tables ...string) {
	s.dbExec(c, "CREATE DATABASE IF NOT EXISTS " + db)
	for _, table := range tables {
		t := db + "." + table
		s.dbExec(c, "DROP TABLE IF EXISTS " + t)
		s.dbExec(c, `CREATE TABLE IF NOT EXISTS ` + t + `(
						id INT,
						title VARCHAR(256),
						content VARCHAR(256),
						mylist VARCHAR(256),
						tenum ENUM("e1", "e2", "e3"),
						tset SET("a", "b", "c"),
						PRIMARY KEY(id)) ENGINE=INNODB;`)
	}
}

func (s *riverTestSuite) dbInsertData(c *C) {
	values := []string{
		`1, '1st', 'hello 1', 'e1', 'a,b'`,
		`2, '2nd', 'hello 2', 'e2', 'b,c'`,
		`3, '3rd', 'hello 3', 'e3', 'c'`,
		`4, '4th', 'hello 4', 'e1', 'a,b,c'`,
	}

	for _, v := range values {
		s.dbExec(c, "INSERT INTO "+ tTable +" (id, title, content, tenum, tset) VALUES (" + v + ")")
	}

	for i := 0; i < 10; i++ {
		row := i + 5
		v := fmt.Sprintf("%d, '%dth', 'hello %d', 'e1', 'a,b,c'", row, row, row)
		s.dbExec(c, "INSERT INTO "+ tTable_(i)+" (id, title, content, tenum, tset) VALUES ("+v+")")
	}
}

func (s *riverTestSuite) dbExec(c *C, query string, args ...interface{}) {
	_, err := s.db.Exec(query, args...)
	if err != nil {
		c.Errorf("Error executing '%s': %v", query, err)
	}
}

func (s *riverTestSuite) esGet(c *C, id string) (*elastic.GetResult, map[string]interface{}) {
	var source map[string]interface{}
	resp, err := s.r.es.Get().Index(tIndex).Type(tType).Id(id).Do()
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

func (s *riverTestSuite) riverCreate(c *C, cfgString string) *River {
	cfg, err := config.NewConfig(cfgString)
	c.Assert(err, IsNil)
	os.RemoveAll(cfg.DataDir)
	r, err := NewRiver(cfg)
	c.Assert(err, IsNil)
	r.es.DeleteIndex(tIndex).Do() // error ok
	return r
}

func (s *riverTestSuite) riverRun(c *C) {
	c.Logf("waiting for dump...")
	go s.r.Run()
	<-s.r.canal.WaitDumpDone()
}

func (s *riverTestSuite) riverWaitForSync(c *C) {
	c.Logf("waiting for sync...")
	err := s.r.canal.CatchMasterPos(10)
	c.Assert(err, IsNil)
}



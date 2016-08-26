package river

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	. "gopkg.in/check.v1"

	"strings"

	"github.com/ehalpern/go-mysql-elasticsearch/config"
	"gopkg.in/olivere/elastic.v3"
)

const (
	tDB           = "river_test"
	tIndex        = tDB
	tTable        = "river"
	tType         = tTable
	tIgnoredTable = tTable + "_ignored"
	tChildTable   = tTable + "_child"
	tChildType    = tChildTable
)

func tTable_(n int) string {
	return fmt.Sprintf(tTable+"_%04d", n)
}

const (
	idField     = "id"
	parentField = "parent"
)

var (
	fields = []string{idField, "title", "content", "tenum", "tset"}
	values = [][]interface{}{
		{1, "1st", "hello 1", "e1", "a"},
		{2, "2nd", "hello 2", "e2", "a,b"},
		{3, "3rd", "hello 3", "e3", "a,b,c"},
		{4, "4th", "hello 4", "e1", "b,c"},
	}
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
	db *sql.DB
	r  *River
	es *EsTestClient
}

var _ = Suite(&riverTestSuite{})

func (s *riverTestSuite) SetUpSuite(c *C) {
	var err error
	s.db, err = sql.Open("mysql", *my_user+":"+*my_pass+"@tcp("+*my_addr+")/"+tDB)
	c.Assert(err, IsNil)
	s.dbExec(c, "CREATE DATABASE IF NOT EXISTS "+tDB)
	if !*useRds {
		s.dbExec(c, "SET SESSION binlog_format = 'ROW'")
	}
	schema := `(
		id INT,
		title VARCHAR(256),
		content VARCHAR(256),
		mylist VARCHAR(256),
		tenum ENUM("e1", "e2", "e3"),
		tset SET("a", "b", "c"),
		parent INT,
		PRIMARY KEY(id)
	) ENGINE=INNODB
	`
	s.dbCreate(c, tDB, schema, tTable)
	s.dbCreate(c, tDB, schema, tIgnoredTable)
	s.dbCreate(c, tDB, schema, tChildTable)
	for i := 0; i < len(values); i++ {
		s.dbCreate(c, tDB, schema, tTable_(i))
	}

	config := `
		db_host = "` + *my_addr + `"
		db_user = "` + *my_user + `"
		db_password = "` + *my_pass + `"
		es_host = "` + *es_addr + `"
		data_dir = "/tmp/mysql2es/test"

		[[source]]
			schema = "` + tDB + `"
			tables = ["` + tTable + `", "` + tTable + `_[0-3]{4}", "` + tChildTable + `"]
			[[rule]]
				schema = "` + tDB + `"
				index = "` + tIndex + `"
				table = "` + tTable + `"
				type  = "` + tType + `"
			[[rule]]
				schema = "` + tDB + `"
				index = "` + tIndex + `"
				table = "` + tTable + `_[0-3]{4}"
				type  = "` + tType + `"
			[[rule]]
				schema = "` + tDB + `"
				index = "` + tIndex + `"
				table = "` + tChildTable + `"
				type  = "` + tChildType + `"
				parent = "` + parentField + `"
	`
	s.r = s.riverCreate(c, config)

	_, err = s.r.es.DeleteIndex(tIndex).Do()
	mapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			tChildType: map[string]interface{}{
				"_parent": map[string]string{"type": tType},
			},
		},
	}
	_, err = s.r.es.CreateIndex(tIndex).BodyJson(mapping).Do()
	c.Assert(err, IsNil)
}

func (s *riverTestSuite) TearDownSuite(c *C) {
	if s.db != nil {
		s.db.Close()
	}
	if s.r != nil {
		s.r.Close()
	}
}

func (s *riverTestSuite) TestDumpAndReplication(c *C) {
	// Dump first 2 rows
	rowsToDump := values[:2]
	for _, r := range rowsToDump {
		s.dbInsert(c, tTable, fields, r)
	}
	s.riverRun(c)
	for _, r := range rowsToDump {
		s.esVerify(c, tIndex, tType, fields, r)
	}

	// Replicate 2nd 2 rows
	rowsToReplicate := values[2:]
	for _, r := range rowsToReplicate {
		s.dbInsert(c, tTable, fields, r)
	}
	s.riverWaitForSync(c)
	for _, r := range rowsToReplicate {
		s.esVerify(c, tIndex, tType, fields, r)
	}
}

func (s *riverTestSuite) TestUpdate(c *C) {
	s.riverRun(c)
	s.dbInsert(c, tTable, fields, values[0])

	updatedRow := make([]interface{}, len(values[0]))
	copy(updatedRow, values[0])

	updateFields := fields[:2]
	updatedRow[1] = "1st-prime"
	c.Logf("updatedRow: %v", updatedRow)
	updateValues := updatedRow[:2]
	s.dbUpdate(c, tTable, updateFields, updateValues)
	s.riverWaitForSync(c)
	s.esVerify(c, tIndex, tType, fields, updatedRow)
}

func (s *riverTestSuite) TestDelete(c *C) {
	s.riverRun(c)
	row := values[0]
	s.dbInsert(c, tTable, fields, row)
	key := s.fieldMap(fields, row)[idField]
	s.dbDelete(c, tTable, idField, key)
	s.riverWaitForSync(c)
	_, doc := s.esGet(c, tIndex, tType, key, "")
	c.Assert(doc, IsNil)
}

func (s *riverTestSuite) TestTableWildcards(c *C) {
	for i, row := range values {
		s.dbInsert(c, tTable_(i), fields, row)
	}
	s.riverRun(c)
	s.riverWaitForSync(c)
	for _, row := range values {
		s.esVerify(c, tIndex, tType, fields, row)
	}
}

func (s *riverTestSuite) TestSchemaUpgrade(c *C) {
	row := values[0]
	s.dbInsert(c, tTable, fields, row)
	s.riverRun(c)
	fm := s.fieldMap(fields, values[0])
	s.dbExec(c, "ALTER TABLE "+tTable+" ADD new VARCHAR(256) DEFAULT 'not-set'")
	s.dbUpdate(c, tTable, []string{idField, "new"}, []interface{}{fm[idField], "set"})
	s.riverWaitForSync(c)
	_, doc := s.esGet(c, tIndex, tType, fm[idField], "")
	c.Assert(doc, NotNil)
	c.Assert(doc["new"], Equals, "set")
	// TODO: Make sure inserting into ignored tables doesn't break anything
}

func (s *riverTestSuite) TestDocWithParent(c *C) {
	s.riverRun(c)
	parentRow := values[0]
	s.dbInsert(c, tTable, fields, parentRow)
	// Add 'parent' field to the child field list
	childFields := make([]string, len(fields))
	copy(childFields, fields)
	childFields = append(childFields, parentField)
	// Add parent id to the child row
	childRow := make([]interface{}, len(parentRow))
	copy(childRow, parentRow)
	childRow = append(childRow, parentRow[0])
	s.dbInsert(c, tChildTable, childFields, childRow)
	s.riverWaitForSync(c)
	s.esVerify(c, tIndex, tChildType, childFields, childRow)
}

func (s *riverTestSuite) dbCreate(c *C, db string, schema string, tables ...string) {
	for _, table := range tables {
		t := db + "." + table
		s.dbExec(c, "DROP TABLE IF EXISTS "+t)
		s.dbExec(c, "CREATE TABLE IF NOT EXISTS "+t+" "+schema+";")
	}
}

func (s *riverTestSuite) dbExec(c *C, query string, args ...interface{}) {
	c.Logf("Executing '%s (%v)'", query, args)
	_, err := s.db.Exec(query, args...)
	if err != nil {
		c.Errorf("Error executing '%s (%v)': %v", query, args, err)
	}
}

func (s *riverTestSuite) dbInsert(c *C, table string, fields []string, values []interface{}) {
	placeholders := []string{}
	for range values {
		placeholders = append(placeholders, "?")
	}
	stmnt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table, strings.Join(fields, ","), strings.Join(placeholders, ","))
	s.dbExec(c, stmnt, values...)
}

func (s *riverTestSuite) dbUpdate(c *C, table string, fields []string, values []interface{}) {
	assignments := []string{}
	for _, f := range fields[1:] {
		assignments = append(assignments, fmt.Sprintf("%s = ?", f))
	}
	u := fmt.Sprintf("UPDATE %s SET %s WHERE %s = %v",
		table, strings.Join(assignments, ","), fields[0], values[0])
	s.dbExec(c, u, values[1:]...)
}

func (s *riverTestSuite) dbDelete(c *C, table string, keyField string, key interface{}) {
	stmnt := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", table, keyField)
	s.dbExec(c, stmnt, key)
}

func (s *riverTestSuite) esGet(c *C, idx string, typ string, id interface{}, parent interface{}) (*elastic.GetResult, map[string]interface{}) {
	var source map[string]interface{}
	resp, err := s.r.es.Get().Index(idx).Type(typ).Id(toString(id)).Parent(toString(parent)).Do()
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

func (s *riverTestSuite) esVerify(c *C, index string, typ string, fields []string, values []interface{}) {
	fm := s.fieldMap(fields, values)
	result, source := s.esGet(c, index, typ, fm[idField], fm[parentField])
	c.Assert(result.Found, Equals, true)
	for i, f := range fields {
		c.Assert(toString(source[f]), Equals, toString(values[i]))
	}
}

func (s *riverTestSuite) fieldMap(fields []string, values []interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	for i, f := range fields {
		m[f] = values[i]
	}
	return m
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

func toString(v interface{}) string {
	return fmt.Sprintf("%v", v)
}

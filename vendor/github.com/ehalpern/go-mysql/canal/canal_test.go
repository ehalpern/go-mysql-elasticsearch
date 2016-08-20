package canal

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/ehalpern/go-mysql/mysql"
	"github.com/siddontang/go/log"
	. "gopkg.in/check.v1"
)

var testHost = flag.String("host", "127.0.0.1", "MySQL host")

func Test(t *testing.T) {
	TestingT(t)
}

type canalTestSuite struct {
	c *Canal
}

var _ = Suite(&canalTestSuite{})

func (s *canalTestSuite) SetUpSuite(c *C) {
	cfg := NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:3306", *testHost)
	cfg.User = "root"
	cfg.Dump.TableDB = "test"
	cfg.Dump.Tables = []string{"canal_test"}
	os.RemoveAll(cfg.DataDir)
	var err error
	s.c, err = NewCanal(cfg)
	c.Assert(err, IsNil)
	s.c.RegRowsEventHandler(&testRowsEventHandler{})

	s.execute(c, "DROP TABLE IF EXISTS test.canal_test")
	sql := `
		CREATE TABLE test.canal_test (
            id int AUTO_INCREMENT,
            name varchar(100),
            PRIMARY KEY(id)
            )ENGINE=innodb;
    `
	s.execute(c, sql)
	s.execute(c, "INSERT INTO test.canal_test (name) VALUES (?), (?), (?)", "a", "b", "c")
	s.execute(c, "SET GLOBAL binlog_format = 'ROW'")

	err = s.c.Start()
	c.Assert(err, IsNil)
	<-s.c.WaitDumpDone()
}

func (s *canalTestSuite) TearDownSuite(c *C) {
	if s.c != nil {
		s.c.Close()
		s.c = nil
	}
}

func (s *canalTestSuite) execute(c *C, query string, args ...interface{}) *mysql.Result {
	r, err := s.c.Execute(query, args...)
	c.Assert(err, IsNil)
	return r
}

type testRowsEventHandler struct {
}

func (h *testRowsEventHandler) Do(e *RowsEvent) error {
	log.Debugf("%s %v\n", e.Action, e.Rows)
	return nil
}

func (h *testRowsEventHandler) Complete() error {
	return nil
}

func (h *testRowsEventHandler) String() string {
	return "testRowsEventHandler"
}

func (s *canalTestSuite) TestCanal(c *C) {
	for i := 1; i < 10; i++ {
		s.execute(c, "INSERT INTO test.canal_test (name) VALUES (?)", fmt.Sprintf("%d", i))
	}
	err := s.c.CatchMasterPos(100)
	c.Assert(err, IsNil)
}

func (s *canalTestSuite) SestSchemaChange(c *C) {
	s.execute(c, "ALTER TABLE test.canal_test ADD new VARCHAR(256) DEFAULT 'not-set'")
	s.execute(c, "INSERT INTO test.canal_test (`name`, `new`) VALUES ('20', 'set')")

	err := s.c.CatchMasterPos(100)
	c.Assert(err, IsNil)
	table, err := s.c.GetTable("test", "canal_test")
	c.Assert(err, IsNil)
	c.Assert(3, Equals, len(table.Columns))
}

// Delete generate an insert row event in the replication stream for a able that
// no longer exists when the stream is read. The is a fragile case because
// the code reteives schema for new tables it dicovers in the stream
func (s *canalTestSuite) TestDeletedTable(c *C) {
	s.c.Close()
	var err error = nil
	s.c, err = NewCanal(s.c.cfg)
	c.Assert(err, IsNil)
	s.c.RegRowsEventHandler(&testRowsEventHandler{})

	sql :=
		`CREATE TABLE IF NOT EXISTS test.canal_test2 (
			id int AUTO_INCREMENT,
			name varchar(100),
			PRIMARY KEY(id)
		)ENGINE=innodb;`
	s.execute(c, sql)
	s.execute(c, "INSERT INTO test.canal_test2 (`name`) VALUES ('foo')")
	s.execute(c, "DROP TABLE test.canal_test2")

	s.c.Start()
	<-s.c.WaitDumpDone()
	err = s.c.CatchMasterPos(100)
	c.Assert(err, IsNil)
}



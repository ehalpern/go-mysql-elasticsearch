package replication

import (
	"strings"
	"testing"
	"github.com/stretchr/testify/assert"
"github.com/siddontang/go/log"
)

func TestScanner(t *testing.T) {
	tokens := scanString("ALTER TABLE t1 ADD c1")
	assert.Equal(t, 5, len(tokens))
	tokens = scanString("ALTER TABLE 't1' ADD 'c1'")
	assert.Equal(t, 5, len(tokens), "tokens: %v", tokens)
	tokens = scanString("ALTER TABLE 't1  1' ADD c1")
	assert.Equal(t, 5, len(tokens), "tokens: %v", tokens)
	assert.Equal(t, "'t1  1'", tokens[2])
	tokens = scanString("ALTER   TABLE t1\n ADD c1")
	assert.Equal(t, 5, len(tokens))
}

func scanString(s string) []string {
	scanner := NewQuotedScanner(strings.NewReader(s))
	var tokens []string
	for scanner.Scan() {
		tokens = append(tokens, scanner.Text())
	}
	return tokens
}                                                                                                        //

func TestParseQuery(t *testing.T) {
	variations := [...]string {
		"ALTER TABLE t1 ADD c1 VARCHAR(256) DEFAULT",
		"alter table t1 add c1 varchar(256) default",
		"ALTER TABLE `t1` ADD `c1` VARCHAR(256) DEFAULT",
	}
	for _, v := range variations {
		q, err := ParseQuery(v)
		assert.NoError(t, err)
		log.Infof("query: %v", q)
		assert.Equal(t, "t1", q.Table)
		assert.Equal(t, AlterOp("ADD"), q.Operation)
		assert.Equal(t, "c1", q.Column)
		assert.Equal(t, "VARCHAR(256)", q.Type)
		assert.Equal(t, "DEFAULT", q.Extra)
	}

	_, err := ParseQuery("UPDATE TABLE t1 ADD c1 VARCHAR(256)")
	assert.Equal(t, ErrIgnored, err)

	q, err := ParseQuery("ALTER TABLE db1.t1 ADD c1 VARCHAR(256) DEFAULT")
	assert.NoError(t, err)
	assert.Equal(t, "db1", q.Schema)
	assert.Equal(t, "t1", q.Table)

	q, err = ParseQuery("ALTER TABLE `db1.t1` ADD c1 VARCHAR(256) DEFAULT")
	assert.NoError(t, err)
	assert.Equal(t, "", q.Schema)
	assert.Equal(t, "db1.t1", q.Table)

	// BUG: this doesn't work
	//q, err = ParseQuery("ALTER TABLE db1.`t1 2` ADD c1 VARCHAR(256) DEFAULT")
	//assert.NoError(t, err)
	//assert.Equal(t, "db1", q.Schema)
	//assert.Equal(t, "t1 2", q.Table)
}

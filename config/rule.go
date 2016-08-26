package config

import (
	"bytes"
	"fmt"

	"github.com/ehalpern/go-mysql/canal"
	"github.com/ehalpern/go-mysql/schema"
	"github.com/juju/errors"
)

// If you want to sync MySQL data into elasticsearch, you must set a rule to let use know how to do it.
// The mapping rule may thi: schema + table <-> index + document type.
// schema and table is for MySQL, index and document type is for Elasticsearch.
type Rule struct {
	Schema string `toml:"schema"`
	Table  string `toml:"table"`
	Index  string `toml:"index"`
	Type   string `toml:"type"`
	Parent string `toml:"parent"`
	IndexFile string `toml:"indexFile"`

	// Default, a MySQL table field name is mapped to Elasticsearch field name.
	// Sometimes, you want to use different name, e.g, the MySQL file name is title,
	// but in Elasticsearch, you want to name it my_title.
	FieldMapping map[string]string `toml:"field"`

	// MySQL table information
	TableInfo *schema.Table
}

func NewDefaultRule(schema string, table string) *Rule {
	r := new(Rule)

	r.Schema = schema
	r.Table = table
	r.Index = table
	r.Type = table
	r.FieldMapping = make(map[string]string)

	return r
}

func (r *Rule) Prepare() error {
	if r.FieldMapping == nil {
		r.FieldMapping = make(map[string]string)
	}

	if len(r.Index) == 0 {
		r.Index = r.Table
	}

	if len(r.Type) == 0 {
		r.Type = r.Index
	}

	return nil
}

// Returns a doc id synthesized by concatenating all primary key values in the row.
// The resulting id will have the form pk1[:pk2[...]]
func (r *Rule) DocId(row []interface{}) (string, error) {
	pks, err := canal.GetPKValues(r.TableInfo, row)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer

	sep := ""
	for i, value := range pks {
		if value == nil {
			return "", errors.Errorf("The %ds PK value is nil", i)
		}

		buf.WriteString(fmt.Sprintf("%s%v", sep, value))
		sep = ":"
	}

	return buf.String(), nil
}


func (r *Rule) ParentId(row []interface{}) (string, error) {
	if len(r.Parent) == 0 {
		return "", nil
	} else {
		index := r.TableInfo.FindColumn(r.Parent)
		if index < 0 {
			return "", errors.Errorf("parent column '%s' not found in table '%s'", r.Parent, r.TableInfo.Name)
		}
		return fmt.Sprint(row[index]), nil
	}
}


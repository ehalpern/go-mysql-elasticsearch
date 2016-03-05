package river

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"path/filepath"
	"regexp"
	"sync"

	"github.com/juju/errors"
	"github.com/ehalpern/go-mysql/canal"
	"github.com/ehalpern/go-mysql-elasticsearch/elastic"
	"github.com/siddontang/go/log"
	"os"
	"strings"
)

// In Elasticsearch, river is a pluggable service within Elasticsearch pulling data then indexing it into Elasticsearch.
// We use this definition here too, although it may not run within Elasticsearch.
// Maybe later I can implement a acutal river in Elasticsearch, but I must learn java. :-)
type River struct {
	c *Config
	canal *canal.Canal
	rules map[string]*Rule
	quit chan struct{}
	wg   sync.WaitGroup
	es *elastic.Client
	st *stat
}

func NewRiver(c *Config) (*River, error) {
	r := new(River)

	r.c = c

	r.quit = make(chan struct{})

	r.rules = make(map[string]*Rule)

	var err error
	if err = r.newCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.prepareRule(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.prepareCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	// We must use binlog full row image
	if err = r.canal.CheckBinlogRowImage("FULL"); err != nil {
		return nil, errors.Trace(err)
	}

	r.es = elastic.NewClient(r.c.ESAddr)

	r.st = &stat{r: r}
	go r.st.Run(r.c.StatAddr)

	return r, nil
}

func (r *River) newCanal() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = r.c.MyAddr
	cfg.User = r.c.MyUser
	cfg.Password = r.c.MyPassword
	cfg.Flavor = r.c.Flavor
	cfg.DataDir = r.c.DataDir

	cfg.ServerID = r.c.ServerID
	cfg.Dump.ExecutionPath = r.c.DumpExec
	cfg.Dump.DiscardErr = false

	var err error
	r.canal, err = canal.NewCanal(cfg)
	return errors.Trace(err)
}

func (r *River) prepareCanal() error {
	var db string
	dbs := map[string]struct{}{}
	tables := make([]string, 0, len(r.rules))
	for _, rule := range r.rules {
		db = rule.Schema
		dbs[rule.Schema] = struct{}{}
		tables = append(tables, rule.Table)
	}

	if len(dbs) == 1 {
		// one db, we can shrink using table
		r.canal.AddDumpTables(db, tables...)
	} else {
		// many dbs, can only assign databases to dump
		keys := make([]string, 0, len(dbs))
		for key, _ := range dbs {
			keys = append(keys, key)
		}

		r.canal.AddDumpDatabases(keys...)
	}

	h := rowsEventHandler{r, make([]*elastic.BulkRequest, 0, r.c.MaxBulkItems)}
	log.Infof("Creating handler len: %d, cap: %d", len(h.bulkBuffer), cap(h.bulkBuffer))
	r.canal.RegRowsEventHandler(&h)

	return nil
}

func (r *River) newRule(schema, table string) error {
	key := ruleKey(schema, table)

	if _, ok := r.rules[key]; ok {
		return errors.Errorf("duplicate source %s, %s defined in config", schema, table)
	}

	r.rules[key] = newDefaultRule(schema, table)
	return nil
}

func (r *River) parseSource() (map[string][]string, error) {
	wildTables := make(map[string][]string, len(r.c.Sources))

	// first, check sources
	for _, s := range r.c.Sources {
		for _, table := range s.Tables {
			if len(s.Schema) == 0 {
				return nil, errors.Errorf("empty schema not allowed for source")
			}

			if regexp.QuoteMeta(table) != table {
				if _, ok := wildTables[ruleKey(s.Schema, table)]; ok {
					return nil, errors.Errorf("duplicate wildcard table defined for %s.%s", s.Schema, table)
				}

				tables := []string{}

				sql := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE
                    table_name RLIKE "%s" AND table_schema = "%s";`, table, s.Schema)

				res, err := r.canal.Execute(sql)
				if err != nil {
					return nil, errors.Trace(err)
				}

				for i := 0; i < res.Resultset.RowNumber(); i++ {
					f, _ := res.GetString(i, 0)
					err := r.newRule(s.Schema, f)
					if err != nil {
						return nil, errors.Trace(err)
					}

					tables = append(tables, f)
				}

				wildTables[ruleKey(s.Schema, table)] = tables
			} else {
				err := r.newRule(s.Schema, table)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}
	}

	if len(r.rules) == 0 {
		return nil, errors.Errorf("no source data defined")
	}

	return wildTables, nil
}

func (r *River) prepareRule() error {
	wildtables, err := r.parseSource()
	if err != nil {
		return errors.Trace(err)
	}

	if r.c.Rules != nil {
		// then, set custom mapping rule
		for _, rule := range r.c.Rules {
			if len(rule.Schema) == 0 {
				return errors.Errorf("empty schema not allowed for rule")
			}

			if regexp.QuoteMeta(rule.Table) != rule.Table {
				//wildcard table
				tables, ok := wildtables[ruleKey(rule.Schema, rule.Table)]
				if !ok {
					return errors.Errorf("wildcard table for %s.%s is not defined in source", rule.Schema, rule.Table)
				}

				if len(rule.Index) == 0 {
					return errors.Errorf("wildcard table rule %s.%s must have a index, can not empty", rule.Schema, rule.Table)
				}

				rule.prepare()

				for _, table := range tables {
					rr := r.rules[ruleKey(rule.Schema, table)]
					rr.Index = rule.Index
					rr.Type = rule.Type
					rr.Parent = rule.Parent
					rr.FieldMapping = rule.FieldMapping
				}
			} else {
				key := ruleKey(rule.Schema, rule.Table)
				if _, ok := r.rules[key]; !ok {
					return errors.Errorf("rule %s, %s not defined in source", rule.Schema, rule.Table)
				}
				rule.prepare()
				r.rules[key] = rule
			}
		}
	}

	for _, rule := range r.rules {
		if rule.TableInfo, err = r.canal.GetTable(rule.Schema, rule.Table); err != nil {
			return errors.Trace(err)
		}

		// table must have a PK for one column, multi columns may be supported later.

		if len(rule.TableInfo.PKColumns) != 1 {
			return errors.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
		}
	}

	return nil
}

func ruleKey(schema string, table string) string {
	return fmt.Sprintf("%s:%s", schema, table)
}

func readIndexFile(configDir string, rule *Rule) ([]byte, error) {
	if (rule.IndexFile != "") {
		// Index file explicitly specified. Fail if not found.
		path := rule.IndexFile
		if !strings.HasPrefix(rule.IndexFile, "/") {
			// indexFile is relative to config dir
			path = configDir + "/" + rule.IndexFile
		}
		log.Infof("Using index setting from %s", path)
		return ioutil.ReadFile(path)
	} else {
		var path string
		// No index file specified. Read file if default (<cfdDir>/<idx>.idx.josn) exists
		// strip trailing -[0-9]+ so indexes with version suffixes match a base settings file
		if m := regexp.MustCompile("(.+)-[0-9]+").FindStringSubmatch(rule.Index); len(m) == 0 {
			path = configDir + "/" + rule.Index + ".idx.json"
		} else {
			path = configDir + "/" + m[0] + ".idx.json"
		}
		data, err := ioutil.ReadFile(path)
		if os.IsNotExist(err) {
			return nil, nil
		} else {
			log.Infof("Using index settings from %s", path)
			return data, err
		}
	}
}

func (r *River) createIndexes() error {
	configDir := filepath.Dir(r.c.ConfigFile)
	for _, rule := range r.rules {
		data, err := readIndexFile(configDir, rule)
		if err != nil {
			return err
		} else if len(data) > 0 {
			var settings map[string]interface{}
			if err := json.Unmarshal(data, &settings); err != nil {
				return errors.Trace(err)
			}
			if err := r.createIndex(rule.Index, settings); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *River) createIndex(idx string, settings map[string]interface{}) error {
	if r.es.IndexExists(idx) {
		log.Warnf("Index '%s' already exists; settings and mappings not updated", idx)
		return nil
	}
	log.Infof("Creating index with settings from %v: %v", idx, settings)
	return r.es.CreateIndex(idx, settings)
}

func (r *River) Run() error {
	if err := r.createIndexes(); err != nil {
		return errors.Trace(err)
	}
	if err := r.canal.Start(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *River) Close() {
	log.Infof("closing river")
	close(r.quit)

	r.canal.Close()

	r.wg.Wait()
}

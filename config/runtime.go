package config

import (
	"fmt"
	"regexp"
	"github.com/ehalpern/go-mysql/canal"
	"github.com/juju/errors"
)

type Runtime struct {
	config *Config
	Rules map[string]*Rule
}

func NewRuntime(config *Config, canal *canal.Canal) (*Runtime, error) {
	rules, err := config.resolveRules(canal)
	if err != nil {
		return nil, err
	}
	return &Runtime{config, rules}, nil
}

func (c *Runtime) GetRule(schema string, table string) *Rule {
	return c.Rules[ruleKey(schema, table)]
}

func (c *Runtime) DBsAndTables() ([]string, []string) {
	dbSet := map[string]struct{}{}
	tables := make([]string, 0, len(c.Rules))
	for _, r := range c.Rules {
		dbSet[r.Schema] = struct{}{}
		tables = append(tables, r.Table)
	}
	dbs := []string{}
	for db := range dbSet {
		dbs = append(dbs, db)
	}
	return dbs, tables
}

func (c *Config) resolveRules(canal *canal.Canal) (map[string]*Rule, error) {
	ruleMap, wildtables, err := c.parseSource(canal)
	if err != nil {
		return nil, err
	}

	if c.Rules != nil {
		// then, set custom mapping rule
		for _, rule := range c.Rules {
			if len(rule.Schema) == 0 {
				return nil, errors.Errorf("empty schema not allowed for rule")
			}

			if regexp.QuoteMeta(rule.Table) != rule.Table {
				//wildcard table
				tables, ok := wildtables[ruleKey(rule.Schema, rule.Table)]
				if !ok {
					return nil, errors.Errorf("wildcard table for %s.%s is not defined in source", rule.Schema, rule.Table)
				}

				if len(rule.Index) == 0 {
					return nil, errors.Errorf("wildcard table rule %s.%s must have a index, can not empty", rule.Schema, rule.Table)
				}

				rule.Prepare()

				for _, table := range tables {
					rr := ruleMap[ruleKey(rule.Schema, table)]
					rr.Index = rule.Index
					rr.Type = rule.Type
					rr.Parent = rule.Parent
					rr.FieldMapping = rule.FieldMapping
				}
			} else {
				key := ruleKey(rule.Schema, rule.Table)
				if _, ok := ruleMap[key]; !ok {
					return nil, errors.Errorf("rule %s, %s not defined in source", rule.Schema, rule.Table)
				}
				rule.Prepare()
				ruleMap[key] = rule
			}
		}
	}

	for _, rule := range ruleMap {
		if rule.TableInfo, err = canal.GetTable(rule.Schema, rule.Table); err != nil {
			return nil, err
		}

		// table must have a PK for one column, multi columns may be supported later.

		if len(rule.TableInfo.PKColumns) != 1 {
			return nil, errors.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
		}
	}

	return ruleMap, nil
}

func (c *Config) parseSource(canal *canal.Canal) (map[string]*Rule, map[string][]string, error) {
	ruleMap := make(map[string]*Rule)
	wildTables := make(map[string][]string, len(c.Sources))

	// first, check sources
	for _, s := range c.Sources {
		for _, table := range s.Tables {
			if len(s.Schema) == 0 {
				return nil, nil, errors.Errorf("empty schema not allowed for source")
			}

			if regexp.QuoteMeta(table) != table {
				if _, ok := wildTables[ruleKey(s.Schema, table)]; ok {
					return nil, nil, errors.Errorf("duplicate wildcard table defined for %s.%s", s.Schema, table)
				}

				tables := []string{}

				sql := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE
                    table_name RLIKE "%s" AND table_schema = "%s";`, table, s.Schema)

				res, err := canal.Execute(sql)
				if err != nil {
					return nil, nil, err
				}

				for i := 0; i < res.Resultset.RowNumber(); i++ {
					f, _ := res.GetString(i, 0)
					err := c.newRule(ruleMap, s.Schema, f)
					if err != nil {
						return nil, nil, err
					}

					tables = append(tables, f)
				}

				wildTables[ruleKey(s.Schema, table)] = tables
			} else {
				err := c.newRule(ruleMap, s.Schema, table)
				if err != nil {
					return nil, nil, err
				}
			}
		}
	}

	if len(ruleMap) == 0 {
		return nil, nil, errors.Errorf("no source data defined")
	}

	return ruleMap, wildTables, nil
}

func (c *Config) newRule(ruleMap map[string]*Rule, schema, table string) error {
	key := ruleKey(schema, table)

	if _, ok := ruleMap[key]; ok {
		return errors.Errorf("duplicate source %s, %s defined in config", schema, table)
	}

	ruleMap[key] = NewDefaultRule(schema, table)
	return nil
}

func ruleKey(schema string, table string) string {
	return fmt.Sprintf("%s:%s", schema, table)
}


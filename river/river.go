package river

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/ehalpern/go-mysql/canal"
	"github.com/siddontang/go/log"
	"github.com/ehalpern/mysql2es/config"
	"gopkg.in/olivere/elastic.v3"
)

// In Elasticsearch, river is a plugable service within Elasticsearch pulling data then indexing it into Elasticsearch.
// We use this definition here too, although it may not run within Elasticsearch.
// Maybe later I can implement a acutal river in Elasticsearch, but I must learn java. :-)
type River struct {
	config *config.Config
	canal  *canal.Canal
	rules  *config.Runtime
	quit   chan struct{}
	wg     sync.WaitGroup
	es     *elastic.Client
	st     *stat
}

func NewRiver(c *config.Config) (*River, error) {
	r := new(River)
	r.config = c
	r.quit = make(chan struct{})

	if err := r.newCanal(); err != nil {
		return nil, err
	} else if r.rules, err = config.NewRuntime(c, r.canal); err != nil {
		return nil, err
	} else if r.es, err = elastic.NewClient(elastic.SetURL("http://" + r.config.EsHost)); err != nil {
		return nil, err
	} else if err := r.prepareCanal(); err != nil {
		return nil, err
	} else if err = r.canal.CheckBinlogRowImage("FULL"); err != nil {
		// We must use binlog full row image
		return nil, err
	}
	return r, nil
}

func (r *River) newCanal() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = r.config.DbHost
	cfg.User = r.config.DbUser
	cfg.Password = r.config.DbPassword
	cfg.Flavor = "mysql"
	cfg.DataDir = r.config.DataDir
	cfg.ServerID = r.config.DbSlaveID
	cfg.Dump.ExecutionPath = r.config.DumpExec
	cfg.Dump.DiscardErr = false
	var err error
	r.canal, err = canal.NewCanal(cfg)
	return err
}

func (r *River) prepareCanal() error {
	dbs, tables := r.rules.DBsAndTables()
	if len(dbs) == 1 {
		// one db, we can shrink using table
		r.canal.AddDumpTables(dbs[0], tables...)
	} else {
		// many dbs, can only assign databases to dump
		r.canal.AddDumpDatabases(dbs...)
	}

	s := syncer{r.rules, NewBulker(r.es, r.config.EsMaxActions)}
	r.canal.RegRowsEventHandler(&s)

	return nil
}

func readIndexFile(configDir string, rule *config.Rule) ([]byte, error) {
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
	configDir := filepath.Dir(r.config.ConfigFile)
	for _, rule := range r.rules.Rules {
		data, err := readIndexFile(configDir, rule)
		if err != nil {
			return err
		} else if len(data) > 0 {
			var settings map[string]interface{}
			if err := json.Unmarshal(data, &settings); err != nil {
				return err
			}
			if err := r.createIndex(rule.Index, settings); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *River) createIndex(idx string, settings map[string]interface{}) error {
	exists, err := r.es.IndexExists(idx).Do()
	if exists {
		log.Warnf("Index '%s' already exists; settings and mappings not updated", idx)
		return nil
	}
	log.Infof("Creating index with settings from %v: %v", idx, settings)
	_, err = r.es.CreateIndex(idx).BodyJson(settings).Do()
	return err
}

func (r *River) Run() error {
	if err := r.createIndexes(); err != nil {
		return err
	}
	if err := r.canal.Start(); err != nil {
		return err
	}
	return nil
}

func (r *River) Close() {
	log.Infof("Closing river")
	close(r.quit)
	r.canal.Close()
	r.wg.Wait()
}

package river

import (
	"github.com/juju/errors"
	"github.com/ehalpern/go-mysql/canal"
	"github.com/siddontang/go/log"
	"github.com/ehalpern/go-mysql-elasticsearch/config"
)

type syncer struct {
	rules  map[string]*config.Rule
	bulker *Bulker
}

func (s *syncer) Do(e *canal.RowsEvent) error {
	if !s.ignoreEvent(e) {
		actions, err := Convert(s.rules, e)
		if err == nil {
			err = s.bulker.Add(actions)
		}
		if err != nil {
			log.Errorf("Handler failing due to %v", err)
			return canal.ErrHandleInterrupted
		}
	}
	return nil
}

func (s *syncer) ignoreEvent(e *canal.RowsEvent) bool {
	_, ok := s.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	if !ok {
		log.Debugf("Ignoring event for table not configured for replication: %v", ruleKey(e.Table.Schema, e.Table.Name))
	}
	return !ok
}

func (s *syncer) Complete() error {
	err := s.bulker.Submit()
	if err != nil {
		return errors.Wrap(err, canal.ErrHandleInterrupted)
	}
	return nil
}

func (s *syncer) String() string {
	return "ElasticSearchSyncer"
}


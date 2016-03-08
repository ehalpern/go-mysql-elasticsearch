package river

import (
	"github.com/juju/errors"
	"github.com/ehalpern/go-mysql/canal"
)

type syncer struct {
	status *stat
	rules  map[string]*Rule
	bulker *Bulker
}


func (s *syncer) Do(e *canal.RowsEvent) error {
	actions, err := Convert(s.rules, e)
	if err == nil {
		err = s.bulker.Add(actions)
	}
	if err != nil {
		return errors.Wrap(err, canal.ErrHandleInterrupted)
	}
	return nil
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


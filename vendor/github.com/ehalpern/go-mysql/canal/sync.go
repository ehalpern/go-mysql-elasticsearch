package canal

import (
	"time"

	"github.com/juju/errors"
	"github.com/ehalpern/go-mysql/mysql"
	"github.com/ehalpern/go-mysql/replication"
	"github.com/siddontang/go/log"
)

func (c *Canal) startSyncBinlog() error {
	pos := mysql.Position{c.master.Name, c.master.Position}
	log.Infof("Start sync'ing binlog from %v", pos)
	s, err := c.syncer.StartSync(pos)
	if err != nil {
		return errors.Errorf("Failed starting sync at %v: %v", pos, err)
	}

	originalTimeout := time.Second
	timeout := originalTimeout
	forceSavePos := false
	for {
		ev, err := s.GetEventTimeout(timeout)
		if err != nil && err != replication.ErrGetEventTimeout {
			return errors.Trace(err)
		} else if err == replication.ErrGetEventTimeout {
			if timeout == 2 * originalTimeout {
				log.Debugf("Flushing event handlers since sync has gone idle")
				if err := c.flushEventHandlers(); err != nil {
					log.Warnf("Error occurred during flush: %v", err)
				}
			}
			timeout = 2 * timeout
			continue
		}

		timeout = time.Second

		//next binlog pos
		pos.Pos = ev.Header.LogPos

		forceSavePos = false

		log.Debugf("Syncing %v", ev)
		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			c.flushEventHandlers()
			pos.Name = string(e.NextLogName)
			pos.Pos = uint32(e.Position)
			// r.ev <- pos
			forceSavePos = true
			log.Infof("Rotate binlog to %v", pos)
		case *replication.RowsEvent:
			// we only focus row based event
			if err = c.handleRowsEvent(ev); err != nil {
				log.Errorf("Error handling rows event: %v", err)
				return errors.Trace(err)
			}
		case *replication.QueryEvent:
			if err = c.handleQueryEvent(ev); err != nil {
				log.Errorf("Error handling rows event: %v", err)
				return errors.Trace(err)
			}
		default:
			log.Debugf("Ignored event: %+v", e)
		}
		c.master.Update(pos.Name, pos.Pos)
		c.master.Save(forceSavePos)
	}

	return nil
}

func (c *Canal) handleRowsEvent(e *replication.BinlogEvent) error {
	ev := e.Event.(*replication.RowsEvent)

	// Caveat: table may be altered at runtime.
	schema := string(ev.Table.Schema)
	table := string(ev.Table.Table)


	t, err := c.GetTable(schema, table)
	if err == errTableIgnored {
		// ignore
		return nil
	} else if err != nil {
		return errors.Trace(err)
	}
	var action string
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		action = InsertAction
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		action = DeleteAction
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		action = UpdateAction
	default:
		return errors.Errorf("%s not supported now", e.Header.EventType)
	}
	events := newRowsEvent(t, action, ev.Rows)
	return c.travelRowsEventHandler(events)
}

func (c *Canal) handleQueryEvent(e *replication.BinlogEvent) error {
	ev := e.Event.(*replication.QueryEvent)
	query, err := replication.ParseQuery(string(ev.Query))
	log.Debugf("query parsed: %v, %v", query, err)
	if err == replication.ErrIgnored {
		return nil
	} else if err != nil {
		log.Infof("failed to parse: %v, %v", string(ev.Query), err)
		return nil
	} else {
		schema := string(ev.Schema)
		if query.Schema != "" {
			// Schema overridden in query
			schema = query.Schema
		}
		table, err := c.GetTable(schema, query.Table)
		if err == errTableIgnored {
			// ignore
			return nil
		} else if err != nil {
			return errors.Trace(err)
		}

		switch query.Operation {
		case replication.ADD:
			// Flush everything before changing schema
			c.flushEventHandlers()
			table.AddColumn(query.Column, query.Type, query.Extra)
			log.Infof("Adding new column %v %v to %v.%v", query.Column, query.Type, schema, query.Table)
			break;
		case replication.MODIFY:
		case replication.DELETE:
		default:
		}
		return nil
	}
}

func (c *Canal) WaitUntilPos(pos mysql.Position, timeout int) error {
	if timeout <= 0 {
		timeout = 60
	}

	timer := time.NewTimer(time.Duration(timeout) * time.Second)
	for {
		curpos := c.master.Pos()
		select {
		case <-timer.C:
			return errors.Errorf("timed out waiting for position %v; only reached %v", pos, c.master.Pos())
		default:
			curpos = c.master.Pos()
			if curpos.Compare(pos) >= 0 {
				return nil
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	return nil
}

func (c *Canal) CatchMasterPos(timeout int) error {
	rr, err := c.Execute("SHOW MASTER STATUS")
	if err != nil {
		return errors.Trace(err)
	}

	name, _ := rr.GetString(0, 0)
	pos, _ := rr.GetInt(0, 1)

	return c.WaitUntilPos(mysql.Position{name, uint32(pos)}, timeout)
}

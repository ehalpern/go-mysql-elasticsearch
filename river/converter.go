package river

import (
	"reflect"
	"strings"

	"github.com/ehalpern/go-mysql/canal"
	"github.com/ehalpern/go-mysql/schema"
	"github.com/ehalpern/mysql2es/config"
	"github.com/juju/errors"
	"github.com/siddontang/go/log"

	"gopkg.in/olivere/elastic.v3"
)

var (
	ErrIgnoredEvent = errors.New("ignoring event for unexpected database or table")
)

const (
	fieldTypeList = "list"
)

// Converts database replication row events to elasticsearch bulk actions
func Convert(rules *config.Runtime, e *canal.RowsEvent) ([]elastic.BulkableRequest, error) {
	rule := rules.GetRule(e.Table.Schema, e.Table.Name)
	if rule == nil {
		return nil, errors.Errorf("no rule found for %s.%s", e.Table.Schema, e.Table.Name )
	}

	log.Debugf("Converting %v", rule)
	var reqs []elastic.BulkableRequest
	var err error

	switch e.Action {
	case canal.InsertAction:
		reqs, err = convertInsert(rule, e.Rows)
	case canal.DeleteAction:
		reqs, err = convertDelete(rule, e.Rows)
	case canal.UpdateAction:
		log.Debugf("Converting update: %+v", e.Rows)
		reqs, err = convertUpdate(rule, e.Rows)
		log.Debugf("Converted update: %+v", reqs)
	default:
		return nil, errors.Errorf("Unrecognized action action %s", e.Action)
	}

	if err != nil {
		return nil, errors.Errorf("Error adding %s to bulk request: %v", e.Action, err)
	}

	return reqs, nil
}

// for insert and delete
func convertAction(rule *config.Rule, action string, rows [][]interface{}) ([]elastic.BulkableRequest, error) {
	reqs := make([]elastic.BulkableRequest, 0, len(rows))

	for _, values := range rows {
		if id, err := rule.DocId(values); err != nil {
			return nil, err
		} else {
			var req elastic.BulkableRequest

			if parentId, err := rule.ParentId(values); err != nil {
				return nil, err
			} else if action == canal.DeleteAction {
				req = elastic.NewBulkDeleteRequest().Index(rule.Index).Type(rule.Type).Id(id).Routing(parentId)
			} else {
				doc := convertRow(rule, values)
				req = elastic.NewBulkIndexRequest().Index(rule.Index).Type(rule.Type).Id(id).Parent(parentId).Doc(doc)
			}
			reqs = append(reqs, req)
		}
	}

	return reqs, nil
}

func convertInsert(rule *config.Rule, rows [][]interface{}) ([]elastic.BulkableRequest, error) {
	return convertAction(rule, canal.InsertAction, rows)
}

func convertDelete(rule *config.Rule, rows [][]interface{}) ([]elastic.BulkableRequest, error) {
	return convertAction(rule, canal.DeleteAction, rows)
}

func convertUpdate(rule *config.Rule, rows [][]interface{}) ([]elastic.BulkableRequest, error) {
	if len(rows) % 2 != 0 {
		return nil, errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}

	reqs := make([]elastic.BulkableRequest, 0, len(rows))
	var err error = nil

	for i := 0; i < len(rows); i += 2 {
		beforeID, err := rule.DocId(rows[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		if beforeID == "" {
			log.Errorf("No id provided in before update row: %v\n", rows[i])
		}

		afterID, err := rule.DocId(rows[i+1])

		if err != nil {
			return nil, errors.Trace(err)
		}
		if afterID == "" {
			log.Errorf("No id provided in after update row: %v\n", rows[i+1])
		}

		beforeParentID, afterParentID := "", ""
		beforeParentID, err = rule.ParentId(rows[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		afterParentID, err = rule.ParentId(rows[i+1])
		if err != nil {
			return nil, errors.Trace(err)
		}

		var req elastic.BulkableRequest
		req = elastic.NewBulkUpdateRequest().Index(rule.Index).Type(rule.Type).Parent(beforeParentID).Id(beforeID).Routing(beforeParentID)

		if beforeID != afterID || beforeParentID != afterParentID {
			// if an id is changing, delete the old document and insert a new one
			req = elastic.NewBulkDeleteRequest().Index(rule.Index).Type(rule.Type).Id(beforeID).Routing(beforeParentID)
			reqs = append(reqs, req)
			temp, err := convertInsert(rule, [][]interface{}{rows[i+1]})
			if err == nil {
				req = temp[0]
			}
		} else {
			doc := convertUpdateRow(rule, rows[i], rows[i+1])
			req = elastic.NewBulkUpdateRequest().Index(rule.Index).Type(rule.Type).Parent(beforeParentID).Id(beforeID).Routing(beforeParentID).Doc(doc)
		}
		reqs = append(reqs, req)
	}

	return reqs, err
}

func convertColumnData(col *schema.TableColumn, value interface{}) interface{} {
	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// the column value is null
				return ""
			}

			return col.EnumValues[eNum]
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	case schema.TYPE_FLOAT:
		switch value := value.(type) {
		case int64:
			return float64(value)
		}
	}
	return value
}


func convertRow(rule *config.Rule, values []interface{}) map[string]interface{} {
	doc := make(map[string]interface{}, len(values))

	for i, c := range rule.TableInfo.Columns {
		fname, value := convertField(rule, &c, values[i])
		doc[fname] = value
	}
	return doc
}

func convertUpdateRow(rule *config.Rule, before []interface{}, after []interface{}) map[string]interface{} {
	doc := make(map[string]interface{}, len(after))
	for i, c := range rule.TableInfo.Columns {
		if len(after) <= i {
			// New column may have been to schema before update was processed. This is due
			// to design flaw in go-mysql that reads most recent schema when restarting
			// replication rather than updating schema based on replication log contents.
			break;
		}
		if len(before) <= i || !reflect.DeepEqual(before[i], after[i]) {
			// Update doc if field wasn't in or is different from original row
			field, value := convertField(rule, &c, after[i])
			doc[field] = value
		}
	}
	return doc
}

func convertField(rule *config.Rule, column *schema.TableColumn, value interface{}) (string, interface{}) {
	v := convertColumnData(column, value)
	for cname, s := range rule.FieldMapping {
		if cname == column.Name {
			fname, ftype := parseFieldMapping(cname, s)
			str, ok := v.(string)
			if ok == false {
				return cname, v
			} else if ftype == fieldTypeList {
				return fname, strings.Split(str, ",")
			} else {
				return fname, str
			}
		}
	}
	return column.Name, v
}

func parseFieldMapping(cname string, value string) (string, string) {
	fname := cname
	ftype := ""

	split := strings.Split(value, ",")

	if (split[0] != "") {
		fname = split[0]
	}
	if len(split) == 2 {
		ftype = split[1]
	}

	return fname, ftype
}


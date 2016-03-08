package river

import (
	"reflect"
	"strings"

	"github.com/ehalpern/go-mysql/canal"
	"github.com/ehalpern/go-mysql/schema"
	"github.com/juju/errors"
	"github.com/siddontang/go/log"

	"gopkg.in/olivere/elastic.v3"
)

const (
	fieldTypeList = "list"
)

// Converts database replication row events to elasticsearch bulk actions
func Convert(rules map[string]*Rule, e *canal.RowsEvent) ([]elastic.BulkableRequest, error) {
	key := ruleKey(e.Table.Schema, e.Table.Name)
	rule, ok := rules[key]
	if !ok {
		return nil, errors.Errorf("no rule found for %v", key )
	}

	var reqs []elastic.BulkableRequest
	var err error

	switch e.Action {
	case canal.InsertAction:
		reqs, err = convertInsert(rule, e.Rows)
	case canal.DeleteAction:
		reqs, err = convertDelete(rule, e.Rows)
	case canal.UpdateAction:
		reqs, err = convertUpdate(rule, e.Rows)
	default:
		return nil, errors.Errorf("Unrecognized action action %s", e.Action)
	}

	if err != nil {
		return nil, errors.Errorf("Error adding %s to bulk request: %v", e.Action, err)
	}

	return reqs, nil
}

// for insert and delete
func convertAction(rule *Rule, action string, rows [][]interface{}) ([]elastic.BulkableRequest, error) {
	reqs := make([]elastic.BulkableRequest, 0, len(rows))

	for _, values := range rows {
		id, err := rule.DocId(values)
		if err != nil {
			return nil, err
		}

		var req elastic.BulkableRequest

		if action == canal.DeleteAction {
			req = elastic.NewBulkDeleteRequest().Index(rule.Index).Type(rule.Type).Id(id)
		} else {
			parentID := ""
			if len(rule.Parent) > 0 {
				if parentID, err = rule.ParentId(values); err != nil {
					return nil, err
				}
			}

			doc := convertRow(rule, values)
			req = elastic.NewBulkIndexRequest().Index(rule.Index).Type(rule.Type).Parent(parentID).Id(id).Doc(doc)
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}

func convertInsert(rule *Rule, rows [][]interface{}) ([]elastic.BulkableRequest, error) {
	return convertAction(rule, canal.InsertAction, rows)
}

func convertDelete(rule *Rule, rows [][]interface{}) ([]elastic.BulkableRequest, error) {
	return convertAction(rule, canal.DeleteAction, rows)
}

func convertUpdate(rule *Rule, rows [][]interface{}) ([]elastic.BulkableRequest, error) {
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

		afterID, err := rule.DocId(rows[i+1])

		if err != nil {
			return nil, errors.Trace(err)
		}

		beforeParentID, afterParentID := "", ""
		if len(rule.Parent) > 0 {
			if beforeParentID, err = rule.ParentId(rows[i]); err != nil {
				return nil, errors.Trace(err)
			}
			if afterParentID, err = rule.ParentId(rows[i+1]); err != nil {
				return nil, errors.Trace(err)
			}
		}

		var req elastic.BulkableRequest
		req = elastic.NewBulkUpdateRequest().Index(rule.Index).Type(rule.Type).Parent(beforeParentID).Id(beforeID)

		if beforeID != afterID || beforeParentID != afterParentID {
			// if an id is changing, delete the old document and insert a new one
			req = elastic.NewBulkDeleteRequest().Index(rule.Index).Type(rule.Type).Id(beforeID)
			reqs = append(reqs, req)
			temp, err := convertInsert(rule, [][]interface{}{rows[i+1]})
			if err == nil {
				req = temp[0]
			}
		} else {
			doc := convertUpdateRow(rule, rows[i], rows[i+1])
			req = elastic.NewBulkUpdateRequest().Index(rule.Index).Type(rule.Type).Parent(beforeParentID).Id(beforeID).Doc(doc)
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
				// we insert invalid enum value before, so return empty
				log.Warnf("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
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
	}

	return value
}

func fieldMappingParts(k string, v string) (string, string, string) {
	composedField := strings.Split(v, ",")

	mysql := k
	elastic := composedField[0]
	fieldType := ""

	if 0 == len(elastic) {
		elastic = mysql
	}
	if 2 == len(composedField) {
		fieldType = composedField[1]
	}

	return mysql, elastic, fieldType
}

func convertRow(rule *Rule, values []interface{}) map[string]interface{} {
	valueMap := make(map[string]interface{}, len(values))

	for i, c := range rule.TableInfo.Columns {
		mapped := false
		for k, v := range rule.FieldMapping {
			mysql, elastic, fieldType := fieldMappingParts(k, v)
			if mysql == c.Name {
				mapped = true
				v := convertColumnData(&c, values[i])
				if fieldType == fieldTypeList {
					if str, ok := v.(string); ok {
						valueMap[elastic] = strings.Split(str, ",")
					} else {
						valueMap[elastic] = v
					}
				} else {
					valueMap[elastic] = v
				}
			}
		}
		if mapped == false {
			valueMap[c.Name] = convertColumnData(&c, values[i])
		}
	}
	return valueMap
}

func convertUpdateRow(rule *Rule, before []interface{}, after []interface{}) map[string]interface{} {
	doc := make(map[string]interface{}, len(before))
	for i, c := range rule.TableInfo.Columns {
		mapped := false
		if reflect.DeepEqual(before[i], after[i]) {
			//nothing changed
			continue
		}
		for k, v := range rule.FieldMapping {
			mysql, elastic, fieldType := fieldMappingParts(k, v)
			if mysql == c.Name {
				mapped = true
				v := convertColumnData(&c, after[i])
				str, ok := v.(string)
				if ok == false {
					doc[c.Name] = v
				} else {
					if fieldType == fieldTypeList {
						doc[elastic] = strings.Split(str, ",")
					} else {
						doc[elastic] = str
					}
				}
			}
		}
		if mapped == false {
			doc[c.Name] = convertColumnData(&c, after[i])
		}
	}
	return doc
}

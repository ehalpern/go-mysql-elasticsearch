package river

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/olivere/elastic.v3"
)

const index = "bulkertest"
const typ   = "bulkertype"


type testdoc struct {
	name string
}

func TestBulker(t *testing.T) {
	es := newEsTestClient(t).recreateIndex(index)
	actions := []elastic.BulkableRequest{
		insertAction("1"),
		insertAction("2"),
		insertAction("3"),
	}
	var maxActions int = 2

	bulker := NewBulker(es.es, maxActions)
	for i, req := range actions {
		if i == maxActions {
			break
		} else {
			bulker.Add([]elastic.BulkableRequest{req})
		}
	}
	es.refreshIndex(index)

	// bulker should not flush until maxActions + 1 are added
	hits := es.searchIndex(index).TotalHits()
	assert.Equal(t, int64(0), hits, "bulker submitted %v actions prematurely before buffering %v actions", hits, maxActions)

	// adding maxActions + 1 should cause the first batach (size maxActions) to be submitted
	bulker.Add([]elastic.BulkableRequest{actions[maxActions]})
	es.refreshIndex(index)

	hits = es.searchIndex(index).TotalHits()
	assert.Equal(t, int64(maxActions), hits, "bulker only submitted %v/%v actions", hits, maxActions)

	// submit should force the last action to be submitted
	bulker.Submit()
	es.refreshIndex(index)

	hits = es.searchIndex(index).TotalHits()
	assert.Equal(t, int64(maxActions + 1), hits, "bulker only submitted %v/%v actions", hits, maxActions + 1)
}

func insertAction(id string) elastic.BulkableRequest {
	return elastic.NewBulkIndexRequest().Index(index).Type(typ).Id(id).Doc(&testdoc{id})
}

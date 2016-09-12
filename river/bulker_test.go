package river

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/olivere/elastic.v3"
)

type testdoc struct {
	name string
}

func TestBulker(t *testing.T) {
	const bulkerIndex = "bulker"
	const bulkerType = "bulker"

	es := newEsTestClient(t).recreateIndex(bulkerIndex)
	actions := []elastic.BulkableRequest{
		insertAction(bulkerIndex, bulkerType, "1"),
		insertAction(bulkerIndex, bulkerType, "2"),
		insertAction(bulkerIndex, bulkerType, "3"),
	}
	var maxActions int = 2
	var maxBytes int64 = 5000

	bulker := NewBulker(es.es, maxActions, maxBytes)
	bulker.Add([]elastic.BulkableRequest{actions[0]})
	es.refresh(bulkerIndex)

	// bulker should not flush until maxActions are added
	hits := es.searchMatchAll(bulkerIndex).TotalHits()
	assert.Equal(t, int64(0), hits, "bulker submitted %v actions before buffer reached %v", hits, maxActions)

	// adding maxActions should cause the first batch to be submitted
	bulker.Add([]elastic.BulkableRequest{actions[1]})
	hits = es.refresh(bulkerIndex).searchMatchAll(bulkerIndex).TotalHits()
	assert.Equal(t, int64(maxActions), hits, "bulker only submitted %v/%v actions", hits, maxActions)

	// adding maxActions + 1 shouldn't cause a submit
	bulker.Add([]elastic.BulkableRequest{actions[2]})
	hits = es.refresh(bulkerIndex).searchMatchAll(bulkerIndex).TotalHits()
	assert.Equal(t, int64(maxActions), hits, "bulker submitted %v actions before buffer reached %v", hits, maxActions)

	// submit should force the last action to be submitted
	bulker.Submit()
	hits = es.refresh(bulkerIndex).searchMatchAll(bulkerIndex).TotalHits()
	assert.Equal(t, int64(maxActions + 1), hits, "bulker only submitted %v/%v actions", hits, maxActions + 1)
}

func insertAction(index string, typ string, id string) elastic.BulkableRequest {
	return elastic.NewBulkIndexRequest().Index(index).Type(typ).Id(id).Doc(&testdoc{id})
}

package river

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const esidx = "estest"
const estype = "estest"

func TestGetDocument(t *testing.T) {
	es := newEsTestClient(t).recreateIndex(esidx)
	id := "1"
	doc := map[string]interface{} { "name": id }
	es.putDocument(esidx, estype, id, "", doc)
	returned := es.getDocumentMap(esidx, estype, id, "")
	assert.ObjectsAreEqual(doc, returned)
}

func TestGetMissingDocument(t *testing.T) {
	es := newEsTestClient(t).recreateIndex(esidx)
	returned := es.getDocumentMap(esidx, estype, "1", "")
	assert.Nil(t, returned)
}

func TestSearchMatchAll(t *testing.T) {
	es := newEsTestClient(t).recreateIndex(esidx)
	id := "1"
	doc := map[string]interface{} { "name": id }
	es.putDocument(esidx, estype, id, "", doc).refresh(esidx)
	result := es.searchMatchAll(esidx)
	hits := result.TotalHits()
	assert.Equal(t, int64(1), hits)
}

func TestParentAndChild(t *testing.T) {
	esChildType := estype + "child"
	settings := map[string]interface{} {
		"mappings": map[string]interface{} {
			esChildType: map[string]interface{} {
				"_parent": map[string]string{
					"type": "parentIdx",
				},
				"_routing": map[string]bool {
					"required": false,
				},
			},
		},
	}
	es := newEsTestClient(t).
		recreateIndex(esidx).
		recreateIndexWithSettings(esidx, settings)
	id := "1"
	doc := map[string]interface{} { "name": id }
	childDoc := map[string]interface{} { "name": id, "parent": id }
	es.putDocument(esidx, estype, id, "", doc).refresh(esidx)
	es.putDocument(esidx, esChildType, id, id, childDoc).refresh(esidx)
	result := es.searchMatchAll(esidx)
	hits := result.TotalHits()
	assert.Equal(t, int64(2), hits)
}


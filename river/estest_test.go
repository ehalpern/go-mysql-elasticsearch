package river

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	estestIndex = "estest"
	estestType = "estest"
)

func TestGetDocument(t *testing.T) {
	es := newEsTestClient(t).recreateIndex(estestIndex)
	id := "1"
	doc := map[string]interface{} { "name": id }
	es.putDocument(estestIndex, estestType, id, "", doc)
	returned := es.getDocumentMap(estestIndex, estestType, id, "")
	assert.ObjectsAreEqual(doc, returned)
}

func TestGetMissingDocument(t *testing.T) {
	es := newEsTestClient(t).recreateIndex(estestIndex)
	returned := es.getDocumentMap(estestIndex, estestType, "1", "")
	assert.Nil(t, returned)
}

func TestSearchMatchAll(t *testing.T) {
	es := newEsTestClient(t).recreateIndex(estestIndex)
	id := "1"
	doc := map[string]interface{} { "name": id }
	es.putDocument(estestIndex, estestType, id, "", doc).refresh(estestIndex)
	result := es.searchMatchAll(estestIndex)
	hits := result.TotalHits()
	assert.Equal(t, int64(1), hits)
}

func TestParentAndChild(t *testing.T) {
	esChildType := estestType + "child"
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
		recreateIndex(estestIndex).
		recreateIndexWithSettings(estestIndex, settings)
	id := "1"
	doc := map[string]interface{} { "name": id }
	childDoc := map[string]interface{} { "name": id, "parent": id }
	es.putDocument(estestIndex, estestType, id, "", doc).refresh(estestIndex)
	es.putDocument(estestIndex, esChildType, id, id, childDoc).refresh(estestIndex)
	result := es.searchMatchAll(estestIndex)
	hits := result.TotalHits()
	assert.Equal(t, int64(2), hits)
}


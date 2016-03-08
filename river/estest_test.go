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
	es.putDocument(esidx, estype, id, doc)
	returned := es.getDocumentMap(esidx, estype, id)
	assert.ObjectsAreEqual(doc, returned)
}

func TestGetMissingDocument(t *testing.T) {
	es := newEsTestClient(t).recreateIndex(esidx)
	returned := es.getDocumentMap(esidx, estype, "1")
	assert.Nil(t, returned)
}

func TestSearchMatchAll(t *testing.T) {
	es := newEsTestClient(t).recreateIndex(esidx)
	id := "1"
	doc := map[string]interface{} { "name": id }
	es.putDocument(esidx, estype, id, doc).refresh(esidx)
	result := es.searchMatchAll(esidx)
	hits := result.TotalHits()
	assert.Equal(t, int64(1), hits)
}


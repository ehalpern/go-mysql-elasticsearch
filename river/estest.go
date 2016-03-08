package river

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/olivere/elastic.v3"
	"github.com/stretchr/testify/assert"
	"encoding/json"
)

type EsTestClient struct {
	es *elastic.Client
	t  *testing.T
}

func newEsTestClient(t *testing.T) *EsTestClient {
	c, err := elastic.NewClient()
	if (err != nil) {
		panic(fmt.Sprintf("failed to create elasticsearch client: %v", err))
	}
	return &EsTestClient{c, t}
}

func newEsTestClientWithTrace(t *testing.T) *EsTestClient {
	c, err := elastic.NewClient(elastic.SetTraceLog(log.New(os.Stdout, "", 0)))
	if (err != nil) {
		panic(fmt.Sprintf("failed to create elasticsearch client: %v", err))
	}
	return &EsTestClient{c, t}
}

func (tc *EsTestClient) createIndex(index string) *EsTestClient {
	_, err := tc.es.CreateIndex(index).Do()
	require.NoError(tc.t, err)
	return tc
}

func (tc *EsTestClient) deleteIndex(index string) *EsTestClient {
	_, err := tc.es.DeleteIndex(index).Do()
	if !elastic.IsNotFound(err) {
		require.NoError(tc.t, err)
	}
	return tc
}

func (tc *EsTestClient) recreateIndex(index string) *EsTestClient {
	return tc.deleteIndex(index).createIndex(index)
}

func (tc *EsTestClient) searchMatchAll(index string) *elastic.SearchResult {
	result, err := tc.es.Search(index).Do()
	require.NoError(tc.t, err)
	return result
}

func (tc *EsTestClient) putDocument(index string, typ string, id string, doc interface{}) *EsTestClient {
	_, err := tc.es.Index().Index(index).Type(typ).Id(id).BodyJson(doc).Do()
	require.NoError(tc.t, err)
	return tc
}

func (tc *EsTestClient) getDocument(index string, typ string, id string) *elastic.GetResult {
	result, err := tc.es.Get().Index(index).Type(typ).Id(id).Do()
	if elastic.IsNotFound(err) {
		return nil
	}
	require.NoError(tc.t, err)
	return result
}

func (tc *EsTestClient) getDocumentMap(index string, typ string, id string) map[string]interface{} {
	result := tc.getDocument(index, typ, id)
	if result == nil {
		return nil
	}
	assert.True(tc.t, result.Found)
	bytes, err := result.Source.MarshalJSON()
	assert.NoError(tc.t, err)
	var returnedDoc map[string]interface{}
	err = json.Unmarshal(bytes, &returnedDoc)
	assert.NoError(tc.t, err)
    return returnedDoc
}


// Force elasticsearch to finish indexing any new updates
func (tc *EsTestClient) refresh(index string) *EsTestClient {
	_, err := tc.es.Refresh(index).Do()
	require.NoError(tc.t, err)
	return tc
}
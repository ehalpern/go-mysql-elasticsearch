package river

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/olivere/elastic.v3"
)

type EsTestClient struct {
	es   *elastic.Client
	test *testing.T
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
	require.NoError(tc.test, err)
	return tc
}

func (tc *EsTestClient) deleteIndex(index string) *EsTestClient {
	_, err := tc.es.DeleteIndex(index).Do()
	require.NoError(tc.test, err)
	return tc
}

func (tc *EsTestClient) recreateIndex(index string) *EsTestClient {
	return tc.deleteIndex(index).createIndex(index)
}

func (tc *EsTestClient) searchIndex(index string) *elastic.SearchResult {
	result, err := tc.es.Search(index).Do()
	require.NoError(tc.test, err)
	return result
}

// Force elasticsearch to finish indexing any new updates
func (tc *EsTestClient) refreshIndex(index string) *EsTestClient {
	_, err := tc.es.Refresh(index).Do()
	require.NoError(tc.test, err)
	return tc
}
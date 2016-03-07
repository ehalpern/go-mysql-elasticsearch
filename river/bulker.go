package river

import (
	"gopkg.in/olivere/elastic.v3"
	"github.com/siddontang/go/log"
)

// Bulker is used to build and submit bulk requests to elasticsearch.
type Bulker struct {
	bulker       *elastic.BulkService  // Underlying service for building a submitting requests
	MaxActions   int                   // When this # of actions have been added, request is auto submitted
	Stats        *BulkerStats          // Statistics
	LastError    error                 // Error, if any, from last Submit
	LastResponse *elastic.BulkResponse // Response, if any, from last Submit
}

type BulkerStats struct {
	InsertCount int
	UpdateCount int
	DeleteCount int
	Total       int
}

// NewBulker constructs a new Bulker
func NewBulker(es *elastic.Client, maxActions int) *Bulker {
	if maxActions == 0 {
		maxActions = 1
	}
	return &Bulker{ es.Bulk(), maxActions, new(BulkerStats), nil, nil }
}

// Count returns the number of actions added since the last Submit
func (b *Bulker) Count() int {
	return b.bulker.NumberOfActions()
}

// Adds actions to be submitted in the next request. If adding these actions causes
// Count to exceed MaxActions, auto-submits the current batch by calling Submit.
func (b *Bulker) Add(actions []elastic.BulkableRequest) error {
	if b.bulker.NumberOfActions() + len(actions) > b.MaxActions {
		b.Submit()
	}
	if (b.LastError != nil) {
		return b.LastError
	}
	for _, req := range actions {
		switch req.(type) {
		case *elastic.BulkDeleteRequest:
			b.Stats.DeleteCount++
		case *elastic.BulkIndexRequest:
			b.Stats.InsertCount++
		case *elastic.BulkUpdateRequest:
			b.Stats.UpdateCount++
		}
		b.Stats.Total++
		b.bulker.Add(req)
	}
	return nil
}

// Submit submits the current batch of actions in bulk and resets Count to 0.
func (b *Bulker) Submit() (*elastic.BulkResponse, error) {
	size := b.bulker.NumberOfActions()
	log.Infof("Submitting bulk update of %d/%d to elasticsearch", size, b.MaxActions)

	b.LastResponse, b.LastError = b.bulker.Do()
	if b.LastError != nil {
		log.Infof("Bulk update failed due to %v", b.LastError)
		return nil, b.LastError
	}
	log.Infof("Bulk update succeeded with %d actions: %v", size, b.LastResponse)
	return b.LastResponse, b.LastError
}

package river

import (
	"bytes"
	"fmt"

	"gopkg.in/olivere/elastic.v3"
	"github.com/siddontang/go/log"
)

// Bulker is used to build and submit bulk requests to elasticsearch.
type Bulker struct {
	bulker       *elastic.BulkService  // Underlying service for building a submitting requests
	MaxActions   int                   // When this # of actions have been added, request is auto submitted
	MaxBytes     int64
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
func NewBulker(es *elastic.Client, maxActions int, maxBytes int64) *Bulker {
	if maxActions == 0 {
		maxActions = 1
	}
	return &Bulker{ es.Bulk(), maxActions, maxBytes, new(BulkerStats), nil, nil }
}

// Count returns the number of actions added since the last Submit
func (b *Bulker) Count() int {
	return b.bulker.NumberOfActions()
}

// Adds actions to be submitted in the next request. If adding these actions causes
// count to exceed MaxActions, auto-submits the current batch by calling Submit.
func (b *Bulker) Add(actions []elastic.BulkableRequest) error {

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

	if b.bulker.EstimatedSizeInBytes() >= b.MaxBytes {
		b.Submit()
	} else if (b.bulker.NumberOfActions() >= b.MaxActions) {
		b.Submit()
	}
	return b.LastError
}



// Submit submits the current batch of actions in bulk and resets Count to 0.
func (b *Bulker) Submit() error {
	size := b.bulker.NumberOfActions()
	if (size == 0) {
		return nil
	}
	b.LastResponse, b.LastError = b.bulker.Do()
	if b.LastError != nil {
		log.Errorf("Bulk update %d/%d failed due to %v: %+v", size, b.MaxActions, b.LastError, b.LastResponse)
		return b.LastError
	}
	if b.LastResponse.Errors {
		var buffer bytes.Buffer
		failed := b.LastResponse.Failed()
		count := len(failed)
		buffer.WriteString(fmt.Sprintf("%v actions failed in bulk update:\n", count))
		for i, er := range failed {
			buffer.WriteString(fmt.Sprintf("\t%v:%v\n", er, er.Error))
			if i == 2 {
				if count > 3 {
					buffer.WriteString(fmt.Sprintf("\t...\n"))
				}
				break;
			}
		}
		log.Errorf(buffer.String())
		// show bulk errors but continue
		//b.LastError = errors.Errorf("%v actions failed during bulk update", count)
	} else {
		log.Debugf("Bulk update %d/%d succeeded", size, b.MaxActions)
	}
	return b.LastError
}

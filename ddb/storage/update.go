package storage

import (
	"fmt"
	"time"

	"github.com/ocowchun/baddb/ddb/condition"
	"github.com/ocowchun/baddb/ddb/core"
	"github.com/ocowchun/baddb/ddb/update"
)

type UpdateRequest struct {
	Key             *core.Entry
	UpdateOperation *update.UpdateOperation
	TableName       string
	Condition       *condition.Condition
}

type UpdateResponse struct {
	OldEntry *core.Entry
	NewEntry *core.Entry
}

func (s *InnerStorage) Update(req *UpdateRequest) (*UpdateResponse, error) {
	txn, err := s.BeginTxn()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	res, err := s.UpdateWithTransaction(req, txn)
	if err != nil {
		return nil, err

	}

	return res, txn.Commit()
}

func (s *InnerStorage) UpdateWithTransaction(req *UpdateRequest, txn *Txn) (*UpdateResponse, error) {
	tableMetadata, ok := s.TableMetaDatas[req.TableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", req.TableName)
	}

	for {
		count := tableMetadata.unprocessedRequests.Load()
		if count == 0 {
			break
		}
		if tableMetadata.unprocessedRequests.CompareAndSwap(count, count-1) {
			return nil, ErrUnprocessed
		}
	}

	if tableMetadata.billingMode == core.BILLING_MODE_PROVISIONED {
		if !tableMetadata.writeRateLimiter.AllowN(time.Now(), 1) {
			return nil, RateLimitReachedError
		}
	}

	entry, err := s.GetWithTransaction(&GetRequest{
		Entry:          req.Key,
		ConsistentRead: true,
		TableName:      req.TableName,
	}, txn)
	if err != nil {
		return nil, err
	}

	if entry == nil {
		entry = &core.Entry{
			Body: make(map[string]core.AttributeValue),
		}
	}
	oldEntry := entry.Clone()

	if req.Condition != nil {
		matched, err := req.Condition.Check(entry)
		if err != nil {
			return nil, err
		}
		if !matched {
			return nil, &ConditionalCheckFailedException{"The conditional request failed"}
		}
	}

	// for non-exist key, add key after condition check to ensure it passed
	if len(entry.Body) == 0 {
		for k, v := range req.Key.Body {
			entry.Body[k] = v
		}
	}

	err = req.UpdateOperation.Perform(entry)
	if err != nil {
		return nil, err
	}

	entryWrapper := &EntryWrapper{
		Entry:     entry,
		IsDeleted: false,
		CreatedAt: time.Now(),
	}

	// condition checked in above
	err = s.put(entryWrapper, tableMetadata, nil, txn.tx)
	if err != nil {
		return nil, err
	}

	return &UpdateResponse{OldEntry: oldEntry, NewEntry: entry}, nil
}

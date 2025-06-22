package storage

import (
	"fmt"
	"time"

	"github.com/ocowchun/baddb/ddb/condition"
	"github.com/ocowchun/baddb/ddb/core"
)

type DeleteRequest struct {
	Entry     *core.Entry
	TableName string
	Condition *condition.Condition
}

func (s *InnerStorage) Delete(req *DeleteRequest) error {
	txn, err := s.BeginTxn()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	err = s.DeleteWithTransaction(req, txn)
	if err != nil {
		return err

	}

	return txn.Commit()
}

func (s *InnerStorage) DeleteWithTransaction(req *DeleteRequest, txn *Txn) error {
	tableMetadata, ok := s.TableMetaDatas[req.TableName]
	if !ok {
		return fmt.Errorf("table %s not found", req.TableName)
	}

	for {
		count := tableMetadata.unprocessedRequests.Load()
		if count == 0 {
			break
		}
		if tableMetadata.unprocessedRequests.CompareAndSwap(count, count-1) {
			return ErrUnprocessed
		}
	}

	if tableMetadata.billingMode == core.BILLING_MODE_PROVISIONED {
		if !tableMetadata.writeRateLimiter.AllowN(time.Now(), 1) {
			return RateLimitReachedError
		}
	}

	entryWrapper := &EntryWrapper{
		Entry:     req.Entry,
		IsDeleted: true,
		CreatedAt: time.Now(),
	}
	return s.put(entryWrapper, tableMetadata, req.Condition, txn.tx)
}

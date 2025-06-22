package storage

import (
	"fmt"
	"time"

	"github.com/ocowchun/baddb/ddb/core"
)

type GetRequest struct {
	Entry          *core.Entry
	ConsistentRead bool
	TableName      string
}

func (s *InnerStorage) Get(req *GetRequest) (*core.Entry, error) {
	txn, err := s.BeginTxn()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	entry, err := s.GetWithTransaction(req, txn)
	if err != nil {
		return nil, err
	}

	return entry, txn.Commit()
}

func (s *InnerStorage) GetWithTransaction(req *GetRequest, txn *Txn) (*core.Entry, error) {
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
		n := 1
		if req.ConsistentRead {
			n = 2
		}
		if !tableMetadata.readRateLimiter.AllowN(time.Now(), n) {
			return nil, RateLimitReachedError
		}
	}

	primaryKey, err := s.buildTablePrimaryKey(req.Entry, tableMetadata)
	if err != nil {
		return nil, nil
	}

	tuple, err := s.getTuple(primaryKey.Bytes(), tableMetadata.Name, txn.tx)
	if err != nil {
		return nil, err
	}
	if tuple == nil {
		return nil, nil
	}

	readTs, err := s.readTs(req.TableName, false)
	if err != nil {
		return nil, err
	}
	return tuple.getEntry(req.ConsistentRead, readTs, false), nil
}

func (s *InnerStorage) readTs(tableName string, isGsi bool) (time.Time, error) {
	m := s.TableMetaDatas[tableName]

	if isGsi {
		return time.Now().Add(time.Second * time.Duration(m.gsiDelaySeconds*-1)), nil
	} else {
		return time.Now().Add(time.Second * time.Duration(m.tableDelaySeconds*-1)), nil
	}
}

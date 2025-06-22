package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ocowchun/baddb/ddb/condition"
	"github.com/ocowchun/baddb/ddb/core"
)

type PutRequest struct {
	Entry     *core.Entry
	TableName string
	Condition *condition.Condition
}

func (s *InnerStorage) Put(req *PutRequest) error {
	txn, err := s.BeginTxn()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	if req.TableName == METADATA_TABLE_NAME {
		tableMetadata, err := s.extractTableMetadata(req.Entry)
		if err != nil {
			return err
		}

		err = s.updateTableMetadata(tableMetadata)
		if err != nil {
			return err
		}
		return txn.Commit()

	}

	err = s.PutWithTransaction(req, txn)
	if err != nil {
		return err

	}

	return txn.Commit()
}

func (s *InnerStorage) PutWithTransaction(req *PutRequest, txn *Txn) error {
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
		IsDeleted: false,
		CreatedAt: time.Now(),
	}

	err := s.put(entryWrapper, tableMetadata, req.Condition, txn.tx)
	if err != nil {
		return err
	}

	return nil
}

func (s *InnerStorage) put(entry *EntryWrapper, table *InnerTableMetadata, condition *condition.Condition, txn *sql.Tx) error {
	primaryKey, err := s.buildTablePrimaryKey(entry.Entry, table)
	if err != nil {
		return err
	}

	tuple, err := s.getTuple(primaryKey.Bytes(), table.Name, txn)
	if err != nil {
		return err
	}

	if tuple == nil {
		if condition != nil {
			matched, err := condition.Check(&core.Entry{Body: make(map[string]core.AttributeValue)})

			// improve error handling
			if err != nil || !matched {
				return err
			} else if !matched {
				return &ConditionalCheckFailedException{"The conditional request failed"}
			}
		}

		stmt, err := txn.Prepare("insert into " + table.Name + "(primary_key, body, partition_key, sort_key, shard_id) values(?, ?, ?, ?, ?)")

		tuple = &Tuple{
			Entries: make([]EntryWrapper, 0),
		}
		tuple.addEntry(entry)
		body, err := json.Marshal(&tuple)
		if err != nil {
			return err
		}

		_, err = stmt.Exec(primaryKey.Bytes(), body, primaryKey.PartitionKey, primaryKey.SortKey, buildShardId(primaryKey.PartitionKey))
		if err != nil {
			return err
		}
		defer stmt.Close()

		err = s.syncGlobalSecondaryIndices(primaryKey, entry, txn, table)
		if err != nil {
			return err
		}
	} else {
		if condition != nil {
			currentEntry := tuple.currentEntry()
			if currentEntry == nil {
				currentEntry = &core.Entry{Body: make(map[string]core.AttributeValue)}
			}
			matched, err := condition.Check(currentEntry)
			// improve error handling
			if err != nil {
				return err
			} else if !matched {
				return &ConditionalCheckFailedException{"The conditional request failed"}
			}
		}

		stmt, err := txn.Prepare("update " + table.Name + " set body = ? where primary_key = ?")

		tuple.addEntry(entry)
		body, err := json.Marshal(&tuple)
		if err != nil {
			return err
		}
		_, err = stmt.Exec(body, primaryKey.Bytes())
		if err != nil {
			return err
		}
		defer stmt.Close()

		err = s.syncGlobalSecondaryIndices(primaryKey, entry, txn, table)
		if err != nil {
			return err
		}
	}

	return nil
}

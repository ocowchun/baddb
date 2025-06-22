package storage

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ocowchun/baddb/ddb/core"
	"github.com/ocowchun/baddb/ddb/query"
	"github.com/ocowchun/baddb/ddb/scan"
)

type QueryResponse struct {
	Entries      []*core.Entry
	ScannedCount int32
}

func (s *InnerStorage) Query(req *query.Query) (*QueryResponse, error) {
	txn, err := s.BeginTxn()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	tableMetadata, ok := s.TableMetaDatas[req.TableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", req.TableName)
	}

	res := &QueryResponse{}

	tableName := tableMetadata.Name
	rateLimiter := tableMetadata.readRateLimiter
	isGsi := false
	if req.IndexName != nil {
		gsi, ok := tableMetadata.GlobalSecondaryIndexSettings[*req.IndexName]
		if !ok {
			return nil, fmt.Errorf("index %s not found", *req.IndexName)
		}
		tableName = gsi.IndexTableName
		rateLimiter = gsi.readRateLimiter
		isGsi = true
	}

	// Prepare the query statement
	queryStmt := "SELECT body FROM " + tableName + " WHERE partition_key = ?"
	args := []interface{}{req.PartitionKey}

	if req.ExclusiveStartKey != nil {
		if req.ScanIndexForward {
			queryStmt += " AND primary_key > ?"
		} else {
			queryStmt += " AND primary_key < ?"
		}
		args = append(args, req.ExclusiveStartKey)
	}

	queryStmt += " ORDER BY sort_key "
	if req.ScanIndexForward {
		queryStmt += " ASC"
		queryStmt += ", primary_key ASC "
	} else {
		queryStmt += " DESC"
		queryStmt += ", primary_key DESC "
	}

	readTs, err := s.readTs(req.TableName, isGsi)
	if err != nil {
		return nil, err
	}

	rows, err := txn.tx.Query(queryStmt, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*core.Entry
	scannedCount := 0

	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}

		if tableMetadata.billingMode == core.BILLING_MODE_PROVISIONED {
			n := 1
			if req.ConsistentRead {
				n = 2
			}
			if !rateLimiter.AllowN(time.Now(), n) {
				return nil, RateLimitReachedError
			}
		}

		var tuple Tuple
		scannedCount += 1
		if err := json.Unmarshal(body, &tuple); err != nil {
			return nil, err
		}

		entry := tuple.getEntry(req.ConsistentRead, readTs, isGsi)

		if entry != nil {
			if req.SortKeyPredicate != nil {
				match, err := (*req.SortKeyPredicate)(entry)
				if err != nil {
					return nil, err
				}
				if !match {
					continue
				}
			}
			if req.Filter != nil {
				matched, err := req.Filter.Check(entry)
				if err != nil {
					return nil, err
				}
				if !matched {
					continue
				}
			}
			results = append(results, entry)
		}

		if len(results) >= req.Limit {
			break
		}
	}
	res.Entries = results
	res.ScannedCount = int32(scannedCount)

	return res, txn.Commit()
}

func (s *InnerStorage) QueryItemCount(tableName string) (int64, error) {
	txn, err := s.BeginTxn()
	if err != nil {
		return 0, err
	}
	defer txn.Rollback()

	tableMetadata, ok := s.TableMetaDatas[tableName]
	if !ok {
		return 0, fmt.Errorf("table %s not found", tableName)
	}

	queryStmt := "select body from " + tableMetadata.Name
	args := make([]interface{}, 0)
	rows, err := txn.tx.Query(queryStmt, args...)
	if err != nil {
		return 0, err
	}

	var count int64
	readTs := time.Now()
	for rows.Next() {
		var body []byte
		err = rows.Scan(&body)
		if err != nil {
			return 0, err
		}

		var tuple Tuple
		err = json.Unmarshal(body, &tuple)
		if err != nil {
			return 0, err
		}

		entry := tuple.getEntry(true, readTs, false)
		if entry != nil {
			count++
		}
	}

	return count, txn.Commit()
}

type ScanResponse struct {
	Entries      []*core.Entry
	ScannedCount int32
}

func (s *InnerStorage) Scan(req *scan.Request) (*ScanResponse, error) {
	// TODO: refactor duplicate code with Query
	txn, err := s.BeginTxn()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	tableMetadata, ok := s.TableMetaDatas[req.TableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", req.TableName)
	}

	res := &ScanResponse{}

	tableName := tableMetadata.Name
	rateLimiter := tableMetadata.readRateLimiter
	isGsi := false
	if req.IndexName != nil {
		gsi, ok := tableMetadata.GlobalSecondaryIndexSettings[*req.IndexName]
		if !ok {
			return nil, fmt.Errorf("index %s not found", *req.IndexName)
		}
		tableName = gsi.IndexTableName
		rateLimiter = gsi.readRateLimiter
		isGsi = true
	}

	queryStmt := "SELECT body FROM " + tableName + " WHERE 1=1"
	args := []interface{}{}
	if req.ExclusiveStartKey != nil {
		queryStmt += " AND primary_key > ?"
		args = append(args, req.ExclusiveStartKey)
	}
	if req.TotalSegments != nil && req.Segment != nil {
		queryStmt += " AND shard_id % ? = ?"
		args = append(args, *req.TotalSegments)
		args = append(args, *req.Segment)
	}

	queryStmt += " ORDER BY primary_key"

	readTs, err := s.readTs(req.TableName, isGsi)
	if err != nil {
		return nil, err
	}

	rows, err := txn.tx.Query(queryStmt, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*core.Entry
	scannedCount := 0

	i := int32(0)
	for rows.Next() {
		if tableMetadata.billingMode == core.BILLING_MODE_PROVISIONED {
			n := 1
			if req.ConsistentRead {
				n = 2
			}
			if !rateLimiter.AllowN(time.Now(), n) {
				return nil, RateLimitReachedError
			}
		}

		scannedCount += 1
		var tuple Tuple
		var body []byte

		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(body, &tuple); err != nil {
			return nil, err
		}
		entry := tuple.getEntry(req.ConsistentRead, readTs, isGsi)

		if entry != nil {
			if req.Filter != nil {
				matched, err := req.Filter.Check(entry)
				if err != nil {
					return nil, err
				}
				if !matched {
					i++
					continue
				}
			}

			results = append(results, entry)
		}

		if len(results) >= req.Limit {
			break
		}
		i++
	}
	res.Entries = results
	res.ScannedCount = int32(scannedCount)
	return res, txn.Commit()
}

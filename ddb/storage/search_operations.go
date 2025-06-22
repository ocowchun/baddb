package storage

import (
	"database/sql"
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

type searchTableInfo struct {
	tableName   string
	rateLimiter interface{ AllowN(time.Time, int) bool }
	isGsi       bool
}

func (s *InnerStorage) resolveTableForSearch(tableMetadata *InnerTableMetadata, indexName *string) (*searchTableInfo, error) {
	info := &searchTableInfo{
		tableName:   tableMetadata.Name,
		rateLimiter: tableMetadata.readRateLimiter,
		isGsi:       false,
	}

	if indexName != nil {
		gsi, ok := tableMetadata.GlobalSecondaryIndexSettings[*indexName]
		if !ok {
			return nil, fmt.Errorf("index %s not found", *indexName)
		}
		info.tableName = gsi.IndexTableName
		info.rateLimiter = gsi.readRateLimiter
		info.isGsi = true
	}

	return info, nil
}

// Common row processing for both Query and Scan
func (s *InnerStorage) processRowsForSearch(rows *sql.Rows, tableMetadata *InnerTableMetadata, tableInfo *searchTableInfo, readTs time.Time, consistentRead bool, limit int, filterFunc func(*core.Entry) (bool, error)) ([]*core.Entry, int32, error) {
	var entries []*core.Entry
	scannedCount := 0

	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, 0, err
		}

		// Rate limiting check
		if tableMetadata.billingMode == core.BILLING_MODE_PROVISIONED {
			n := 1
			if consistentRead {
				n = 2
			}
			if !tableInfo.rateLimiter.AllowN(time.Now(), n) {
				return nil, 0, RateLimitReachedError
			}
		}

		// Tuple processing
		var tuple Tuple
		scannedCount += 1
		if err := json.Unmarshal(body, &tuple); err != nil {
			return nil, 0, err
		}

		entry := tuple.getEntry(consistentRead, readTs, tableInfo.isGsi)

		if entry != nil {
			// Apply custom filtering logic
			if filterFunc != nil {
				shouldInclude, err := filterFunc(entry)
				if err != nil {
					return nil, 0, err
				}
				if !shouldInclude {
					continue
				}
			}
			entries = append(entries, entry)
		}

		if len(entries) >= limit {
			break
		}
	}

	return entries, int32(scannedCount), nil
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

	tableInfo, err := s.resolveTableForSearch(tableMetadata, req.IndexName)
	if err != nil {
		return nil, err
	}

	// Prepare the query statement
	queryStmt := "SELECT body FROM " + tableInfo.tableName + " WHERE partition_key = ?"
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

	readTs, err := s.readTs(req.TableName, tableInfo.isGsi)
	if err != nil {
		return nil, err
	}

	rows, err := txn.tx.Query(queryStmt, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Create filter function for Query-specific logic
	queryFilter := func(entry *core.Entry) (bool, error) {
		if req.SortKeyPredicate != nil {
			match, err := (*req.SortKeyPredicate)(entry)
			if err != nil {
				return false, err
			}
			if !match {
				return false, nil
			}
		}
		if req.Filter != nil {
			matched, err := req.Filter.Check(entry)
			if err != nil {
				return false, err
			}
			if !matched {
				return false, nil
			}
		}
		return true, nil
	}

	entries, scannedCount, err := s.processRowsForSearch(rows, tableMetadata, tableInfo, readTs, req.ConsistentRead, req.Limit, queryFilter)
	if err != nil {
		return nil, err
	}

	res.Entries = entries
	res.ScannedCount = scannedCount

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

	tableInfo, err := s.resolveTableForSearch(tableMetadata, req.IndexName)
	if err != nil {
		return nil, err
	}

	queryStmt := "SELECT body FROM " + tableInfo.tableName + " WHERE 1=1"
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

	readTs, err := s.readTs(req.TableName, tableInfo.isGsi)
	if err != nil {
		return nil, err
	}

	rows, err := txn.tx.Query(queryStmt, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Create filter function for Scan-specific logic
	scanFilter := func(entry *core.Entry) (bool, error) {
		if req.Filter != nil {
			matched, err := req.Filter.Check(entry)
			if err != nil {
				return false, err
			}
			if !matched {
				return false, nil
			}
		}
		return true, nil
	}

	entries, scannedCount, err := s.processRowsForSearch(rows, tableMetadata, tableInfo, readTs, req.ConsistentRead, req.Limit, scanFilter)
	if err != nil {
		return nil, err
	}

	res.Entries = entries
	res.ScannedCount = scannedCount
	return res, txn.Commit()
}

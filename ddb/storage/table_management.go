package storage

import (
	"fmt"
	"strconv"

	"github.com/ocowchun/baddb/ddb/core"
)

const METADATA_TABLE_NAME = "baddb_table_metadata"

type TableMetadata struct {
	tableName           string
	tableDelaySeconds   int
	gsiDelaySeconds     int
	unprocessedRequests uint32
}

// TODO: ensure update TableMetaDatas is thread safe
func (s *InnerStorage) extractTableMetadata(entry *core.Entry) (*TableMetadata, error) {
	nameAttr, ok := entry.Body["tableName"]
	if !ok {
		return nil, fmt.Errorf("missing tableName in entry")
	}
	if nameAttr.S == nil {
		return nil, fmt.Errorf("tableName should be S, but got %s", nameAttr)
	}
	tableName := *nameAttr.S

	var err error
	tableDelaySeconds := 0
	if tableDelayAttr, ok := entry.Body["tableDelaySeconds"]; ok {
		tableDelaySeconds, err = strconv.Atoi(*tableDelayAttr.N)
		if err != nil {
			return nil, err
		}
	}

	gsiDelaySeconds := 0
	if gsiDelayAttr, ok := entry.Body["gsiDelaySeconds"]; ok {
		gsiDelaySeconds, err = strconv.Atoi(*gsiDelayAttr.N)
		if err != nil {
			return nil, err
		}
	}

	unprocessedRequests := uint32(0)
	if unprocessedAttr, ok := entry.Body["unprocessedRequests"]; ok {
		val, err := strconv.Atoi(*unprocessedAttr.N)
		if err != nil {
			return nil, err
		}
		unprocessedRequests = uint32(val)
	}

	return &TableMetadata{
			tableName:           tableName,
			tableDelaySeconds:   tableDelaySeconds,
			gsiDelaySeconds:     gsiDelaySeconds,
			unprocessedRequests: unprocessedRequests,
		},
		nil
}

func (s *InnerStorage) updateTableMetadata(tableMetadata *TableMetadata) error {
	m, ok := s.TableMetaDatas[tableMetadata.tableName]
	if !ok {
		return fmt.Errorf("table %s not found", tableMetadata.tableName)
	}

	m.tableDelaySeconds = tableMetadata.tableDelaySeconds
	m.gsiDelaySeconds = tableMetadata.gsiDelaySeconds
	m.unprocessedRequests.Store(tableMetadata.unprocessedRequests)

	return nil
}

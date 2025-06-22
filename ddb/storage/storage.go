package storage

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/ocowchun/baddb/ddb/core"
	"golang.org/x/time/rate"
	"sync"
	"sync/atomic"
)

type InnerStorage struct {
	db             *sql.DB
	mutex          sync.Mutex
	TableMetaDatas map[string]*InnerTableMetadata
	counter        atomic.Int32
}

type InnerTableGlobalSecondaryIndexSetting struct {
	IndexTableName   string
	PartitionKeyName *string
	SortKeyName      *string
	NonKeyAttributes []string
	ProjectionType   core.ProjectionType
	readRateLimiter  *rate.Limiter
}

type InnerTableMetadata struct {
	Name string
	// TODO: create inner storage gsi struct to keep rate limiter
	GlobalSecondaryIndexSettings map[string]InnerTableGlobalSecondaryIndexSetting
	PartitionKeySchema           *core.KeySchema
	SortKeySchema                *core.KeySchema
	billingMode                  core.BillingMode
	readCapacityUnits            int
	writeCapacityUnits           int
	readRateLimiter              *rate.Limiter
	writeRateLimiter             *rate.Limiter
	tableDelaySeconds            int
	gsiDelaySeconds              int
	unprocessedRequests          atomic.Uint32
}

func NewInnerStorage() *InnerStorage {
	db, err := sql.Open("sqlite3", ":memory:")

	if err != nil {
		panic(err)
	}

	storage := &InnerStorage{
		db:             db,
		TableMetaDatas: make(map[string]*InnerTableMetadata),
		counter:        atomic.Int32{},
	}

	return storage
}

func (s *InnerStorage) newTableName() string {
	return fmt.Sprintf("table_%d", s.counter.Add(1))
}

func (s *InnerStorage) newGsiTableName() string {
	return fmt.Sprintf("gsi_%d", s.counter.Add(1))
}

func (s *InnerStorage) CreateTable(meta *core.TableMetaData) error {
	tableName := s.newTableName()
	sqlStmt := `
	create table ` + tableName + `(primary_key blob not null primary key, body blob, partition_key blob, sort_key blob, shard_id integer);
	delete from ` + tableName + `;
	create index idx_` + tableName + `_partiton_key_sort_key on ` + tableName + `(partition_key, sort_key);
	`

	billingMode := core.BILLING_MODE_PAY_PER_REQUEST
	readCapacity := 0
	writeCapacity := 0
	if meta.BillingMode == core.BILLING_MODE_PROVISIONED {
		billingMode = core.BILLING_MODE_PROVISIONED
		// For an item up to 4 KB, one read capacity unit (RCU) represents one strongly consistent read operation per
		// second, or two eventually consistent read operations per second.
		// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/provisioned-capacity-mode.html#read-write-capacity-units
		readCapacity = int(*meta.ProvisionedThroughput.ReadCapacityUnits) * 2
		writeCapacity = int(*meta.ProvisionedThroughput.WriteCapacityUnits)
	}
	readLimiter := rate.NewLimiter(rate.Limit(readCapacity), readCapacity)
	writeLimiter := rate.NewLimiter(rate.Limit(writeCapacity), writeCapacity)

	globalSecondarySettings := make(map[string]InnerTableGlobalSecondaryIndexSetting)
	for _, gsi := range meta.GlobalSecondaryIndexSettings {
		gsiTableName := s.newGsiTableName()
		sqlStmt += `
		create table ` + gsiTableName + ` (primary_key blob not null primary key, body blob, main_partition_key blob, main_sort_key blob, partition_key blob, sort_key blob, shard_id integer);
		delete from ` + gsiTableName + `;
		create index idx_` + gsiTableName + `_partition_key_sort_key on ` + gsiTableName + `(partition_key, sort_key);
		`

		readLimiter := rate.NewLimiter(rate.Limit(readCapacity), readCapacity)
		globalSecondarySettings[*gsi.IndexName] = InnerTableGlobalSecondaryIndexSetting{
			IndexTableName:   gsiTableName,
			PartitionKeyName: gsi.PartitionKeyName(),
			SortKeyName:      gsi.SortKeyName(),
			NonKeyAttributes: gsi.NonKeyAttributes,
			ProjectionType:   gsi.ProjectionType,
			readRateLimiter:  readLimiter,
		}
	}

	_, err := s.db.Exec(sqlStmt)
	if err != nil {
		return err
	}
	innerTableMetadata := &InnerTableMetadata{
		Name:                         tableName,
		GlobalSecondaryIndexSettings: globalSecondarySettings,
		PartitionKeySchema:           meta.PartitionKeySchema,
		SortKeySchema:                meta.SortKeySchema,
		billingMode:                  billingMode,
		readCapacityUnits:            readCapacity,
		writeCapacityUnits:           writeCapacity,
		readRateLimiter:              readLimiter,
		writeRateLimiter:             writeLimiter,
		tableDelaySeconds:            0,
		gsiDelaySeconds:              0,
		unprocessedRequests:          atomic.Uint32{},
	}
	s.TableMetaDatas[meta.Name] = innerTableMetadata

	return nil
}

type Txn struct {
	tx       *sql.Tx
	s        *InnerStorage
	isLocked atomic.Bool
}

func (txn *Txn) Commit() error {
	defer txn.unlock()

	return txn.tx.Commit()
}
func (txn *Txn) unlock() {
	for txn.isLocked.Load() {
		txn.s.mutex.Unlock()
		txn.isLocked.Store(false)
	}
}

func (txn *Txn) Rollback() error {
	defer txn.unlock()

	return txn.tx.Rollback()
}

func (s *InnerStorage) BeginTxn() (*Txn, error) {
	s.mutex.Lock()

	tx, err := s.db.Begin()
	if err != nil {
		s.mutex.Unlock()
		return nil, err
	}

	txn := &Txn{
		tx: tx,
		s:  s,
	}
	txn.isLocked.Store(true)

	return txn, nil
}

func (s *InnerStorage) buildTablePrimaryKey(entry *core.Entry, table *InnerTableMetadata) (*PrimaryKey, error) {
	primaryKey := &PrimaryKey{
		PartitionKey: make([]byte, 0),
		SortKey:      make([]byte, 0),
	}

	pk, ok := entry.Body[table.PartitionKeySchema.AttributeName]
	if !ok {
		return primaryKey, errors.New("partitionKey not found")
	}

	primaryKey.PartitionKey = pk.Bytes()

	if table.SortKeySchema != nil {
		sk, ok := entry.Body[table.SortKeySchema.AttributeName]
		if !ok {
			return primaryKey, errors.New("sortKey not found")
		}
		primaryKey.SortKey = sk.Bytes()
	}

	return primaryKey, nil
}

func (s *InnerStorage) getTuple(primaryKey []byte, tableName string, txn *sql.Tx) (*Tuple, error) {
	stmt, err := txn.Prepare("select body from " + tableName + " where primary_key = ?")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	body := []byte{}
	err = stmt.QueryRow(primaryKey).Scan(&body)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, err
	}

	var tuple Tuple
	err = json.Unmarshal(body, &tuple)
	if err != nil {
		return nil, err
	}

	return &tuple, nil
}

func (s *InnerStorage) syncGlobalSecondaryIndices(primaryKey *PrimaryKey, entry *EntryWrapper, txn *sql.Tx, table *InnerTableMetadata) error {
	for _, gsi := range table.GlobalSecondaryIndexSettings {
		// TODO: refactor it
		tableName := gsi.IndexTableName
		tuple, err := s.getTuple(primaryKey.Bytes(), tableName, txn)
		if err != nil {
			return err
		}

		var gsiPartitionKey []byte
		if _, ok := entry.Entry.Body[*gsi.PartitionKeyName]; ok {
			gsiPartitionKey = entry.Entry.Body[*gsi.PartitionKeyName].Bytes()
		}
		var gsiSortKey []byte
		if gsi.SortKeyName != nil {
			if _, ok := entry.Entry.Body[*gsi.SortKeyName]; ok {
				gsiSortKey = entry.Entry.Body[*gsi.SortKeyName].Bytes()
			}
		}

		gsiEntry := s.newGsiEntry(entry, gsi, table)

		if tuple == nil {

			stmt, err := txn.Prepare("insert into " + tableName + "(primary_key, body, main_partition_key, main_sort_key, partition_key, sort_key, shard_id) values(?, ?, ?, ?, ?, ?, ?)")

			tuple = &Tuple{
				Entries: make([]EntryWrapper, 0),
			}
			tuple.addEntry(gsiEntry)
			body, err := json.Marshal(&tuple)
			if err != nil {
				return err
			}

			_, err = stmt.Exec(primaryKey.Bytes(), body, primaryKey.PartitionKey, primaryKey.SortKey, gsiPartitionKey, gsiSortKey, buildShardId(gsiPartitionKey))
			if err != nil {
				return err
			}

			err = stmt.Close()
			if err != nil {
				return err
			}
		} else {
			stmt, err := txn.Prepare("update " + tableName + " set body = ?, partition_key = ?, sort_key = ?, shard_id = ? where primary_key = ?")

			tuple.addEntry(gsiEntry)
			body, err := json.Marshal(&tuple)
			if err != nil {
				return err
			}
			_, err = stmt.Exec(body, gsiPartitionKey, gsiSortKey, buildShardId(gsiPartitionKey), primaryKey.Bytes())
			if err != nil {
				return err
			}

			err = stmt.Close()
			if err != nil {
				return err
			}
		}

	}

	return nil
}

func (s *InnerStorage) newGsiEntry(entry *EntryWrapper, gsi InnerTableGlobalSecondaryIndexSetting, table *InnerTableMetadata) *EntryWrapper {
	gsiEntry := &core.Entry{
		Body: make(map[string]core.AttributeValue),
	}

	tablePartitionKeyName := table.PartitionKeySchema.AttributeName
	gsiEntry.Body[tablePartitionKeyName] = entry.Entry.Body[tablePartitionKeyName]
	if table.SortKeySchema != nil {
		tableSortKeyName := table.SortKeySchema.AttributeName
		gsiEntry.Body[tableSortKeyName] = entry.Entry.Body[tableSortKeyName]
	}
	gsiEntry.Body[*gsi.PartitionKeyName] = entry.Entry.Body[*gsi.PartitionKeyName]
	if gsi.SortKeyName != nil {
		gsiEntry.Body[*gsi.SortKeyName] = entry.Entry.Body[*gsi.SortKeyName]
	}
	if entry.IsDeleted {
		return &EntryWrapper{Entry: gsiEntry, IsDeleted: true, CreatedAt: entry.CreatedAt}
	}

	switch gsi.ProjectionType {
	case core.PROJECTION_TYPE_ALL:
		gsiEntry.Body = entry.Entry.Body
	case core.PROJECTION_TYPE_INCLUDE:
		for _, attr := range gsi.NonKeyAttributes {
			if val, ok := entry.Entry.Body[attr]; ok {
				gsiEntry.Body[attr] = val
			}
		}

	case core.PROJECTION_TYPE_KEYS_ONLY:
		// do nothing

	}

	return &EntryWrapper{Entry: gsiEntry, IsDeleted: false, CreatedAt: entry.CreatedAt}
}

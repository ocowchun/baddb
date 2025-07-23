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

func (m *InnerTableMetadata) Clone() *InnerTableMetadata {
	clone := &InnerTableMetadata{
		Name:                m.Name,
		billingMode:         m.billingMode,
		readCapacityUnits:   m.readCapacityUnits,
		writeCapacityUnits:  m.writeCapacityUnits,
		readRateLimiter:     m.readRateLimiter,
		writeRateLimiter:    m.writeRateLimiter,
		tableDelaySeconds:   m.tableDelaySeconds,
		gsiDelaySeconds:     m.gsiDelaySeconds,
		unprocessedRequests: atomic.Uint32{},
	}

	// Copy the unprocessed requests value
	clone.unprocessedRequests.Store(m.unprocessedRequests.Load())

	// Deep copy GlobalSecondaryIndexSettings
	if len(m.GlobalSecondaryIndexSettings) > 0 {
		clone.GlobalSecondaryIndexSettings = make(map[string]InnerTableGlobalSecondaryIndexSetting)
		for name, gsi := range m.GlobalSecondaryIndexSettings {
			clonedGSI := InnerTableGlobalSecondaryIndexSetting{
				IndexTableName:   gsi.IndexTableName,
				ProjectionType:   gsi.ProjectionType,
				readRateLimiter:  gsi.readRateLimiter,
			}

			if gsi.PartitionKeyName != nil {
				partitionKeyName := *gsi.PartitionKeyName
				clonedGSI.PartitionKeyName = &partitionKeyName
			}

			if gsi.SortKeyName != nil {
				sortKeyName := *gsi.SortKeyName
				clonedGSI.SortKeyName = &sortKeyName
			}

			if len(gsi.NonKeyAttributes) > 0 {
				clonedGSI.NonKeyAttributes = make([]string, len(gsi.NonKeyAttributes))
				copy(clonedGSI.NonKeyAttributes, gsi.NonKeyAttributes)
			}

			clone.GlobalSecondaryIndexSettings[name] = clonedGSI
		}
	}

	// Deep copy key schemas
	if m.PartitionKeySchema != nil {
		clone.PartitionKeySchema = &core.KeySchema{
			AttributeName: m.PartitionKeySchema.AttributeName,
			AttributeType: m.PartitionKeySchema.AttributeType,
		}
	}

	if m.SortKeySchema != nil {
		clone.SortKeySchema = &core.KeySchema{
			AttributeName: m.SortKeySchema.AttributeName,
			AttributeType: m.SortKeySchema.AttributeType,
		}
	}

	return clone
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
		readCapacity = meta.ProvisionedThroughput.ReadCapacityUnits * 2
		writeCapacity = meta.ProvisionedThroughput.WriteCapacityUnits
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
		if err := s.syncSingleGSI(primaryKey, entry, txn, table, gsi); err != nil {
			return err
		}
	}
	return nil
}

func (s *InnerStorage) syncSingleGSI(primaryKey *PrimaryKey, entry *EntryWrapper, txn *sql.Tx, table *InnerTableMetadata, gsi InnerTableGlobalSecondaryIndexSetting) error {
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
		if err != nil {
			return err
		}
		defer stmt.Close()

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
	} else {
		stmt, err := txn.Prepare("update " + tableName + " set body = ?, partition_key = ?, sort_key = ?, shard_id = ? where primary_key = ?")
		if err != nil {
			return err
		}
		defer stmt.Close()

		tuple.addEntry(gsiEntry)
		body, err := json.Marshal(&tuple)
		if err != nil {
			return err
		}
		_, err = stmt.Exec(body, gsiPartitionKey, gsiSortKey, buildShardId(gsiPartitionKey), primaryKey.Bytes())
		if err != nil {
			return err
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

type GSIOperation struct {
	Type         string // "CREATE", "UPDATE", "DELETE"
	GSIName      string
	CreateAction *CreateGSIAction
	UpdateAction *UpdateGSIAction
	DeleteAction *DeleteGSIAction
}

type CreateGSIAction struct {
	IndexName             *string
	KeySchema             []core.KeySchema
	Projection            *Projection
	PartitionKeyName      *string
	SortKeyName           *string
	NonKeyAttributes      []string
	ProjectionType        core.ProjectionType
	ProvisionedThroughput *core.ProvisionedThroughput
}

type UpdateGSIAction struct {
	ProvisionedThroughput *core.ProvisionedThroughput
}

type DeleteGSIAction struct {
	// No additional data needed for delete
}

type Projection struct {
	ProjectionType   core.ProjectionType
	NonKeyAttributes []string
}

func (s *InnerStorage) RunGSIUpdates(tableName string, operations []GSIOperation) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	tableMetadata, exists := s.TableMetaDatas[tableName]
	if !exists {
		return fmt.Errorf("table %s not found", tableName)
	}

	// Clone metadata for atomic updates - rollback on failure
	clonedMetadata := tableMetadata.Clone()

	// Begin transaction for atomic operation
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Execute all operations in the transaction using the cloned metadata
	for _, op := range operations {
		switch op.Type {
		case "CREATE":
			if err := s.executeGSICreateInTx(tx, clonedMetadata, op.GSIName, op.CreateAction); err != nil {
				return fmt.Errorf("failed to create GSI %s: %w", op.GSIName, err)
			}
		case "UPDATE":
			if err := s.executeGSIUpdateInTx(tx, clonedMetadata, op.GSIName, op.UpdateAction); err != nil {
				return fmt.Errorf("failed to update GSI %s: %w", op.GSIName, err)
			}
		case "DELETE":
			if err := s.executeGSIDeleteInTx(tx, clonedMetadata, op.GSIName, op.DeleteAction); err != nil {
				return fmt.Errorf("failed to delete GSI %s: %w", op.GSIName, err)
			}
		default:
			return fmt.Errorf("unknown GSI operation type: %s", op.Type)
		}
	}

	// Commit transaction - all operations succeed or all fail
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit GSI updates transaction: %w", err)
	}

	// Only update the actual metadata after successful commit
	s.TableMetaDatas[tableName] = clonedMetadata

	return nil
}

func (s *InnerStorage) executeGSICreateInTx(tx *sql.Tx, tableMetadata *InnerTableMetadata, gsiName string, action *CreateGSIAction) error {
	// Generate unique GSI table name
	gsiTableName := s.newGsiTableName()

	// Create GSI table with same schema as during table creation
	createTableSQL := `
		create table ` + gsiTableName + ` (primary_key blob not null primary key, body blob, main_partition_key blob, main_sort_key blob, partition_key blob, sort_key blob, shard_id integer);
		create index idx_` + gsiTableName + `_partition_key_sort_key on ` + gsiTableName + `(partition_key, sort_key);
	`

	if _, err := tx.Exec(createTableSQL); err != nil {
		return fmt.Errorf("failed to create GSI table %s: %w", gsiTableName, err)
	}

	// Set up rate limiter (default to no rate limiting for PAY_PER_REQUEST)
	var readLimiter *rate.Limiter
	if action.ProvisionedThroughput != nil {
		readCapacity := action.ProvisionedThroughput.ReadCapacityUnits * 2
		readLimiter = rate.NewLimiter(rate.Limit(readCapacity), readCapacity)
	}

	// Add to metadata
	tableMetadata.GlobalSecondaryIndexSettings[gsiName] = InnerTableGlobalSecondaryIndexSetting{
		IndexTableName:   gsiTableName,
		PartitionKeyName: action.PartitionKeyName,
		SortKeyName:      action.SortKeyName,
		NonKeyAttributes: action.NonKeyAttributes,
		ProjectionType:   action.ProjectionType,
		readRateLimiter:  readLimiter,
	}

	// Backfill existing data from main table to GSI
	if err := s.backfillGSIData(tx, tableMetadata, gsiName); err != nil {
		return fmt.Errorf("failed to backfill GSI data: %w", err)
	}

	return nil
}

func (s *InnerStorage) executeGSIUpdateInTx(tx *sql.Tx, tableMetadata *InnerTableMetadata, gsiName string, action *UpdateGSIAction) error {
	gsiSetting, exists := tableMetadata.GlobalSecondaryIndexSettings[gsiName]
	if !exists {
		return fmt.Errorf("GSI %s not found", gsiName)
	}

	// Update rate limiter based on throughput settings
	if action.ProvisionedThroughput != nil {
		// PROVISIONED mode - set rate limiter
		readCapacity := action.ProvisionedThroughput.ReadCapacityUnits * 2
		gsiSetting.readRateLimiter = rate.NewLimiter(rate.Limit(readCapacity), readCapacity)
	} else {
		// PAY_PER_REQUEST mode - disable rate limiting
		gsiSetting.readRateLimiter = nil
	}

	// Update metadata (in memory, committed with transaction)
	tableMetadata.GlobalSecondaryIndexSettings[gsiName] = gsiSetting

	// Note: No database schema changes needed for GSI throughput updates
	// The throughput settings are only stored in memory for rate limiting

	return nil
}

func (s *InnerStorage) executeGSIDeleteInTx(tx *sql.Tx, tableMetadata *InnerTableMetadata, gsiName string, action *DeleteGSIAction) error {
	gsiSetting, exists := tableMetadata.GlobalSecondaryIndexSettings[gsiName]
	if !exists {
		return fmt.Errorf("GSI %s not found", gsiName)
	}

	// Drop the GSI table
	dropTableSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", gsiSetting.IndexTableName)
	if _, err := tx.Exec(dropTableSQL); err != nil {
		return fmt.Errorf("failed to drop GSI table %s: %w", gsiSetting.IndexTableName, err)
	}

	delete(tableMetadata.GlobalSecondaryIndexSettings, gsiName)

	return nil
}

func (s *InnerStorage) backfillGSIData(tx *sql.Tx, tableMetadata *InnerTableMetadata, gsiName string) error {
	// Get the GSI settings
	gsiSetting, exists := tableMetadata.GlobalSecondaryIndexSettings[gsiName]
	if !exists {
		return fmt.Errorf("GSI %s not found in metadata", gsiName)
	}

	// Query all existing entries from the main table using the logical table name
	query := fmt.Sprintf("SELECT partition_key, sort_key, body FROM %s", tableMetadata.Name)
	rows, err := tx.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query main table for backfill: %w", err)
	}
	defer rows.Close()

	// Process each existing entry
	for rows.Next() {
		var partitionKey []byte
		var sortKey []byte
		var bodyBytes []byte

		if err := rows.Scan(&partitionKey, &sortKey, &bodyBytes); err != nil {
			return fmt.Errorf("failed to scan main table row: %w", err)
		}

		var tuple Tuple
		if err := json.Unmarshal(bodyBytes, &tuple); err != nil {
			return fmt.Errorf("failed to unmarshal tuple: %w", err)
		}

		// Reconstruct primary key
		primaryKey := &PrimaryKey{
			PartitionKey: partitionKey,
			SortKey:      sortKey,
		}

		// For each entry in the tuple, sync to the new GSI
		for _, entryWrapper := range tuple.Entries {
			if err := s.syncSingleGSI(primaryKey, &entryWrapper, tx, tableMetadata, gsiSetting); err != nil {
				return fmt.Errorf("failed to sync entry to GSI: %w", err)
			}
		}
	}

	return rows.Err()
}

package inner_storage

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/ocowchun/baddb/ddb/condition"
	"github.com/ocowchun/baddb/ddb/core"
	"github.com/ocowchun/baddb/ddb/query"
	"github.com/ocowchun/baddb/ddb/update"
	"golang.org/x/time/rate"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type QueryResponse struct {
	Entries      []*core.Entry
	ScannedCount int32
}

type EntryWithKey struct {
	Key   []byte
	Entry *core.Entry
}

type EntryWrapper struct {
	Entry     *core.Entry
	IsDeleted bool
	CreatedAt time.Time
}

type InnerStorage struct {
	db             *sql.DB
	rwMutex        sync.RWMutex
	TableMetaDatas map[string]*InnerTableMetadata
	counter        atomic.Int32

	// simulate how long to get the latest data, if syncDelay is 5 seconds then the latest data will be available after 5 seconds
	syncDelay time.Duration
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
		syncDelay:      time.Second * 5,
	}

	return storage
}

type TableMetadata struct {
	tableName         string
	tableDelaySeconds int
	gsiDelaySeconds   int
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
	create table ` + tableName + `(primary_key blob not null primary key, body blob, partition_key blob, sort_key blob);
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
		create table ` + gsiTableName + ` (primary_key blob not null primary key, body blob, main_partition_key blob, main_sort_key blob, partition_key blob, sort_key blob);
		delete from ` + gsiTableName + `;
		create index idx_` + gsiTableName + `_partition_key_sort_key on ` + gsiTableName + `(partition_key, sort_key);
		`

		readLimiter := rate.NewLimiter(rate.Limit(readCapacity), readCapacity)
		globalSecondarySettings[*gsi.IndexName] = InnerTableGlobalSecondaryIndexSetting{
			IndexTableName:   gsiTableName,
			PartitionKeyName: gsi.PartitionKeyName,
			SortKeyName:      gsi.SortKeyName,
			NonKeyAttributes: gsi.NonKeyAttributes,
			ProjectionType:   gsi.ProjectionType,
			readRateLimiter:  readLimiter,
		}
	}

	//meta.ProvisionedThroughput.readCapacity

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
	}
	fmt.Println("add innerTableMetadata", meta.Name, innerTableMetadata)
	s.TableMetaDatas[meta.Name] = innerTableMetadata

	return nil
}

func (s *InnerStorage) QueryItemCount(tableName string) (int64, error) {
	txn, err := s.BeginTxn(true)
	if err != nil {
		return 0, err
	}
	defer txn.Rollback()

	tableMetadata, ok := s.TableMetaDatas[tableName]
	if !ok {
		return 0, fmt.Errorf("table %s not found", tableName)
	}

	stmt, err := txn.tx.Prepare("select count(1) from " + tableMetadata.Name)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	var count int64
	err = stmt.QueryRow().Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, txn.Commit()
}

type Txn struct {
	tx       *sql.Tx
	s        *InnerStorage
	isLocked atomic.Bool
	readOnly bool
}

func (txn *Txn) Commit() error {
	txn.unlock()

	return txn.tx.Commit()
}
func (txn *Txn) unlock() {
	for txn.isLocked.Load() {
		if txn.isLocked.CompareAndSwap(true, false) {
			if txn.readOnly {
				txn.s.rwMutex.RUnlock()
			} else {
				txn.s.rwMutex.Unlock()
			}
		}
	}
}

func (txn *Txn) Rollback() error {
	txn.unlock()

	return txn.tx.Rollback()
}

func (s *InnerStorage) BeginTxn(readOnly bool) (*Txn, error) {
	if readOnly {
		s.rwMutex.RLock()
	} else {
		s.rwMutex.Lock()
	}

	tx, err := s.db.Begin()
	if err != nil {
		if readOnly {
			s.rwMutex.RUnlock()
		} else {
			s.rwMutex.Unlock()
		}
		return nil, err
	}

	txn := &Txn{
		tx:       tx,
		s:        s,
		readOnly: readOnly,
	}
	txn.isLocked.Store(true)

	return txn, nil
}

type PutRequest struct {
	Entry     *core.Entry
	TableName string
	Condition *condition.Condition
}

const METADATA_TABLE_NAME = "baddb_table_metadata"

func (s *InnerStorage) Put(req *PutRequest) error {
	txn, err := s.BeginTxn(false)
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

	return &TableMetadata{
			tableName:         tableName,
			tableDelaySeconds: tableDelaySeconds,
			gsiDelaySeconds:   gsiDelaySeconds,
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

	return nil
}

type UpdateRequest struct {
	Key             *core.Entry
	UpdateOperation *update.UpdateOperation
	TableName       string
	Condition       *condition.Condition
}

func (s *InnerStorage) Update(req *UpdateRequest) (*UpdateResponse, error) {
	txn, err := s.BeginTxn(false)
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

type UpdateResponse struct {
	OldEntry *core.Entry
	NewEntry *core.Entry
}

func (s *InnerStorage) UpdateWithTransaction(req *UpdateRequest, txn *Txn) (*UpdateResponse, error) {
	tableMetadata, ok := s.TableMetaDatas[req.TableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", req.TableName)
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

type DeleteRequest struct {
	Entry     *core.Entry
	TableName string
	Condition *condition.Condition
}

func (s *InnerStorage) Delete(req *DeleteRequest) error {
	txn, err := s.BeginTxn(false)
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

func (s *InnerStorage) PutWithTransaction(req *PutRequest, txn *Txn) error {
	tableMetadata, ok := s.TableMetaDatas[req.TableName]
	if !ok {
		return fmt.Errorf("table %s not found", req.TableName)
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

type ConditionalCheckFailedException struct {
	Message string
}

func (e *ConditionalCheckFailedException) Error() string {
	return e.Message
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

		stmt, err := txn.Prepare("insert into " + table.Name + "(primary_key, body, partition_key, sort_key) values(?, ?, ?, ?)")

		tuple = &Tuple{
			Entries: make([]EntryWrapper, 0),
		}
		tuple.addEntry(entry)
		body, err := json.Marshal(&tuple)
		if err != nil {
			return err
		}

		_, err = stmt.Exec(primaryKey.Bytes(), body, primaryKey.PartitionKey, primaryKey.SortKey)
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

			stmt, err := txn.Prepare("insert into " + tableName + "(primary_key, body, main_partition_key, main_sort_key, partition_key, sort_key) values(?, ?, ?, ?, ?, ?)")

			tuple = &Tuple{
				Entries: make([]EntryWrapper, 0),
			}
			tuple.addEntry(gsiEntry)
			body, err := json.Marshal(&tuple)
			if err != nil {
				return err
			}

			_, err = stmt.Exec(primaryKey.Bytes(), body, primaryKey.PartitionKey, primaryKey.SortKey, gsiPartitionKey, gsiSortKey)
			if err != nil {
				return err
			}

			err = stmt.Close()
			if err != nil {
				return err
			}
		} else {
			stmt, err := txn.Prepare("update " + tableName + " set body = ?, partition_key = ?, sort_key = ? where primary_key = ?")

			tuple.addEntry(gsiEntry)
			body, err := json.Marshal(&tuple)
			if err != nil {
				return err
			}
			_, err = stmt.Exec(body, gsiPartitionKey, gsiSortKey, primaryKey.Bytes())
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

type GetRequest struct {
	Entry          *core.Entry
	ConsistentRead bool
	TableName      string
}

func (s *InnerStorage) Get(req *GetRequest) (*core.Entry, error) {
	txn, err := s.BeginTxn(true)
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
	// TODO: remove syncDelay

	m := s.TableMetaDatas[tableName]
	fmt.Println("readTs, ", tableName)

	if isGsi {
		return time.Now().Add(time.Second * time.Duration(m.gsiDelaySeconds*-1)), nil
	} else {
		return time.Now().Add(time.Second * time.Duration(m.tableDelaySeconds*-1)), nil
	}
}

var (
	RateLimitReachedError = errors.New("rate limit reached")
)

func (s *InnerStorage) Query(req *query.Query) (*QueryResponse, error) {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()

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

	queryStmt += " LIMIT ?"
	// workaround to get enough result in case filter and deleted Entries
	// TODO: use more proper way to get enough result
	args = append(args, req.Limit*3)

	txn, err := s.BeginTxn(true)
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

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

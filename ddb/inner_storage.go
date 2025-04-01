package ddb

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/time/rate"
	"sync"
	"sync/atomic"
	"time"
)

type PrimaryKey struct {
	PartitionKey []byte
	SortKey      []byte
}

func (k *PrimaryKey) Bytes() []byte {
	bs := make([]byte, 0)
	bs = append(bs, k.PartitionKey...)

	//	// TODO: need a better way to generate primary key, current approach will failed for below case
	//	// pk: ab, sk: |cd and pk: ab|, sk: cd
	if len(k.SortKey) > 0 {
		bs = append(bs, []byte("|")...)
		bs = append(bs, k.SortKey...)
	}

	return bs
}

func (k *PrimaryKey) String() string {
	var out bytes.Buffer
	out.WriteString(fmt.Sprintf("(PartitionKey: '%s'", string(k.PartitionKey)))
	if len(k.SortKey) > 0 {
		out.WriteString(fmt.Sprintf(", SortKey: '%s'", string(k.SortKey)))
	}
	out.WriteString(")")
	return out.String()
}

type QueryResponse struct {
	Entries      []*Entry
	ScannedCount int32
}

type EntryWithKey struct {
	Key   []byte
	Entry *Entry
}

type EntryWrapper struct {
	Entry     *Entry
	IsDeleted bool
	CreatedAt time.Time
}

type Tuple struct {
	Entries []EntryWrapper
}

// return prevEntry, found
func (t *Tuple) prevEntry() *Entry {
	if len(t.Entries) < 2 {
		return nil
	} else {
		prevEntry := t.Entries[0]
		if prevEntry.IsDeleted {
			return nil
		}
		return prevEntry.Entry
	}
}

func (t *Tuple) getEntry(consistentRead bool, readTs time.Time) *Entry {
	if len(t.Entries) == 2 {
		if consistentRead || t.Entries[1].CreatedAt.Before(readTs) {
			if t.Entries[1].IsDeleted {
				return nil
			}
			return t.Entries[1].Entry
		} else {
			if t.Entries[0].IsDeleted {
				return nil
			}
			return t.Entries[0].Entry
		}
	} else if len(t.Entries) == 1 {
		if consistentRead || t.Entries[0].CreatedAt.Before(readTs) {
			if t.Entries[0].IsDeleted {
				return nil
			}
			return t.Entries[0].Entry
		} else {
			return nil
		}
	} else {
		// no entry
		return nil
	}
}

// return lastEntry, found
func (t *Tuple) currentEntry() *Entry {
	if len(t.Entries) == 0 {
		return nil
	} else {
		lastEntry := t.Entries[len(t.Entries)-1]
		if lastEntry.IsDeleted {
			return nil
		}
		return lastEntry.Entry
	}
}

// addEntry only keep last 2 Entries
func (t *Tuple) addEntry(entryWrapper *EntryWrapper) {

	t.Entries = append(t.Entries, *entryWrapper)
	if len(t.Entries) > 2 {
		t.Entries = t.Entries[1:]
	}
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
	ProjectionType   ProjectionType
	readRateLimiter  *rate.Limiter
}

type InnerTableMetadata struct {
	Name string
	// TODO: create inner storage gsi struct to keep rate limiter
	GlobalSecondaryIndexSettings map[string]InnerTableGlobalSecondaryIndexSetting
	PartitionKeySchema           *KeySchema
	SortKeySchema                *KeySchema
	billingMode                  BillingMode
	readCapacityUnits            int
	writeCapacityUnits           int
	readRateLimiter              *rate.Limiter
	writeRateLimiter             *rate.Limiter
}

func NewInnerStorage() *InnerStorage {
	db, err := sql.Open("sqlite3", ":memory:")

	if err != nil {
		panic(err)
	}

	return &InnerStorage{
		db:             db,
		TableMetaDatas: make(map[string]*InnerTableMetadata),
		counter:        atomic.Int32{},
		syncDelay:      time.Second * 5,
	}
}

func (s *InnerStorage) newTableName() string {
	return fmt.Sprintf("table_%d", s.counter.Add(1))
}

func (s *InnerStorage) newGsiTableName() string {
	return fmt.Sprintf("gsi_%d", s.counter.Add(1))
}

func (s *InnerStorage) CreateTable(meta *TableMetaData) error {
	tableName := s.newTableName()
	sqlStmt := `
	create table ` + tableName + `(primary_key blob not null primary key, body blob, partition_key blob, sort_key blob);
	delete from ` + tableName + `;
	create index idx_` + tableName + `_partiton_key_sort_key on ` + tableName + `(partition_key, sort_key);
	`

	billingMode := BILLING_MODE_PAY_PER_REQUEST
	readCapacity := 0
	writeCapacity := 0
	if meta.BillingMode == BILLING_MODE_PROVISIONED {
		billingMode = BILLING_MODE_PROVISIONED
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
	}
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
	Entry     *Entry
	TableName string
	Condition *Condition
}

func (s *InnerStorage) Put(req *PutRequest) error {
	txn, err := s.BeginTxn(false)
	if err != nil {
		return err
	}
	defer txn.Rollback()

	err = s.PutWithTransaction(req, txn)
	if err != nil {
		return err

	}

	return txn.Commit()
}

type UpdateRequest struct {
	Key             *Entry
	UpdateOperation *UpdateOperation
	TableName       string
	Condition       *Condition
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
	OldEntry *Entry
	NewEntry *Entry
}

func (s *InnerStorage) UpdateWithTransaction(req *UpdateRequest, txn *Txn) (*UpdateResponse, error) {
	tableMetadata, ok := s.TableMetaDatas[req.TableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", req.TableName)
	}

	if tableMetadata.billingMode == BILLING_MODE_PROVISIONED {
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
		entry = &Entry{
			Body: make(map[string]AttributeValue),
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
	Entry     *Entry
	TableName string
	Condition *Condition
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

	if tableMetadata.billingMode == BILLING_MODE_PROVISIONED {
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

	if tableMetadata.billingMode == BILLING_MODE_PROVISIONED {
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

func (s *InnerStorage) put(entry *EntryWrapper, table *InnerTableMetadata, condition *Condition, txn *sql.Tx) error {
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
			matched, err := condition.Check(&Entry{Body: make(map[string]AttributeValue)})

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
				currentEntry = &Entry{Body: make(map[string]AttributeValue)}
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

func (s *InnerStorage) buildTablePrimaryKey(entry *Entry, table *InnerTableMetadata) (*PrimaryKey, error) {
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
	gsiEntry := &Entry{
		Body: make(map[string]AttributeValue),
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
	case PROJECTION_TYPE_ALL:
		gsiEntry.Body = entry.Entry.Body
	case PROJECTION_TYPE_INCLUDE:
		for _, attr := range gsi.NonKeyAttributes {
			if val, ok := entry.Entry.Body[attr]; ok {
				gsiEntry.Body[attr] = val
			}
		}

	case PROJECTION_TYPE_KEYS_ONLY:
		// do nothing

	}

	return &EntryWrapper{Entry: gsiEntry, IsDeleted: false, CreatedAt: entry.CreatedAt}
}

type GetRequest struct {
	Entry          *Entry
	ConsistentRead bool
	TableName      string
}

func (s *InnerStorage) Get(req *GetRequest) (*Entry, error) {
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

func (s *InnerStorage) GetWithTransaction(req *GetRequest, txn *Txn) (*Entry, error) {
	tableMetadata, ok := s.TableMetaDatas[req.TableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", req.TableName)
	}

	if tableMetadata.billingMode == BILLING_MODE_PROVISIONED {
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

	readTs := s.readTs()
	return tuple.getEntry(req.ConsistentRead, readTs), nil
}

func (s *InnerStorage) readTs() time.Time {
	return time.Now().Add(s.syncDelay * -1)
}

var (
	RateLimitReachedError = errors.New("rate limit reached")
)

func (s *InnerStorage) Query(req *Query) (*QueryResponse, error) {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()

	tableMetadata, ok := s.TableMetaDatas[req.TableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", req.TableName)
	}

	res := &QueryResponse{}

	tableName := tableMetadata.Name
	rateLimiter := tableMetadata.readRateLimiter
	if req.IndexName != nil {
		gsi, ok := tableMetadata.GlobalSecondaryIndexSettings[*req.IndexName]
		if !ok {
			return nil, fmt.Errorf("index %s not found", *req.IndexName)
		}
		tableName = gsi.IndexTableName
		rateLimiter = gsi.readRateLimiter
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

	rows, err := s.db.Query(queryStmt, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*Entry
	scannedCount := 0
	readTs := s.readTs()
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}

		if tableMetadata.billingMode == BILLING_MODE_PROVISIONED {
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

		entry := tuple.getEntry(req.ConsistentRead, readTs)

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

	return res, nil
}

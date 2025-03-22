package ddb

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
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
	bs = append(bs, []byte("|")...)
	bs = append(bs, k.SortKey...)

	return bs
}

type QueryResponse struct {
	Entries      []*Entry
	ScannedCount int32
}

type EntryWithKey struct {
	Key   []byte
	Entry *Entry
}

type Entry struct {

	//PartitionKey *AttributeValue
	//SortKey      *AttributeValue
	Body map[string]AttributeValue
}

type EntryWrapper struct {
	Entry     *Entry
	IsDeleted bool
	CreatedAt time.Time
}

//type

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
}

type InnerTableMetadata struct {
	Name                         string
	GlobalSecondaryIndexSettings []GlobalSecondaryIndexSetting
	PartitionKeySchema           *KeySchema
	SortKeySchema                *KeySchema
}

func NewInnerStorage() *InnerStorage {
	db, err := sql.Open("sqlite3", ":memory:")

	if err != nil {
		panic(err)
	}

	return &InnerStorage{
		db:             db,
		TableMetaDatas: make(map[string]*InnerTableMetadata),
	}
}

func (s *InnerStorage) CreateTable(meta *TableMetaData) error {
	tableName := "table_" + meta.Name
	sqlStmt := `
	create table ` + tableName + `(primary_key blob not null primary key, body blob, partition_key blob, sort_key blob);
	delete from ` + tableName + `;
	create index idx_` + tableName + `_partiton_key_sort_key on ` + tableName + `(partition_key, sort_key);
	`

	globalSecondarySettings := meta.GlobalSecondaryIndexSettings
	for _, gsi := range globalSecondarySettings {
		gsiTableName := "gsi_" + tableName + "_" + *gsi.IndexName
		sqlStmt += `
		create table ` + gsiTableName + ` (primary_key blob not null primary key, body blob, main_partition_key blob, main_sort_key blob, partition_key blob, sort_key blob);
		delete from ` + gsiTableName + `;
		create index idx_` + gsiTableName + `_partition_key_sort_key on ` + gsiTableName + `(partition_key, sort_key);
		`
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
	}
	s.TableMetaDatas[meta.Name] = innerTableMetadata

	return nil
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
		tableName := "gsi_" + table.Name + "_" + *gsi.IndexName
		tuple, err := s.getTuple(primaryKey.Bytes(), tableName, txn)
		if err != nil {
			return err
		}

		var gsiPartitionKey []byte
		if _, ok := entry.Entry.Body[*gsi.PartitionKeyName]; ok {
			gsiPartitionKey = entry.Entry.Body[*gsi.PartitionKeyName].Bytes()
		}
		var gsiSortKey []byte
		if _, ok := entry.Entry.Body[*gsi.SortKeyName]; ok {
			gsiSortKey = entry.Entry.Body[*gsi.SortKeyName].Bytes()
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

func (s *InnerStorage) newGsiEntry(entry *EntryWrapper, gsi GlobalSecondaryIndexSetting, table *InnerTableMetadata) *EntryWrapper {
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
	gsiEntry.Body[*gsi.SortKeyName] = entry.Entry.Body[*gsi.SortKeyName]
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

	entry, err := s.GetWithTxn(req, txn)
	if err != nil {
		return nil, err
	}

	return entry, txn.Commit()
}

func (s *InnerStorage) GetWithTxn(req *GetRequest, txn *Txn) (*Entry, error) {
	tableMetadata, ok := s.TableMetaDatas[req.TableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", req.TableName)
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

	if req.ConsistentRead {
		return tuple.currentEntry(), nil
	} else {
		return tuple.prevEntry(), nil
	}
}

func (s *InnerStorage) Query(req *Query) (*QueryResponse, error) {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()

	tableMetadata, ok := s.TableMetaDatas[req.TableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", req.TableName)
	}

	res := &QueryResponse{}

	tableName := tableMetadata.Name
	if req.IndexName != nil {
		for _, gsi := range tableMetadata.GlobalSecondaryIndexSettings {
			if *gsi.IndexName == *req.IndexName {
				// TODO: refactor it
				tableName = "gsi_" + tableMetadata.Name + "_" + *gsi.IndexName
				break
			}
		}

		if tableName == tableMetadata.Name {
			return nil, fmt.Errorf("index %s not found", *req.IndexName)
		}
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
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}

		var tuple Tuple
		scannedCount += 1
		if err := json.Unmarshal(body, &tuple); err != nil {
			return nil, err
		}

		entry := tuple.prevEntry()
		if req.ConsistentRead {
			entry = tuple.currentEntry()
		}

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

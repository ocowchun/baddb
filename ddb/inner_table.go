package ddb

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"sync"
	"time"
)

type InnerTable struct {
	db                           *sql.DB
	rwMutex                      sync.RWMutex
	partitionKeyName             *string
	sortKeyName                  *string
	globalSecondaryIndexSettings []GlobalSecondaryIndexSetting
	//globalSecondaryIndexes []types.GlobalSecondaryIndex
}

func NewInnerTable(partitionKeyName *string, sortKeyName *string, globalSecondarySettings []GlobalSecondaryIndexSetting) *InnerTable {
	db, err := sql.Open("sqlite3", ":memory:")

	sqlStmt := `
	create table main_table (primary_key blob not null primary key, body blob, partition_key blob, sort_key blob);
	delete from main_table;
	create index idx_main_table_partiton_key_sort_key on main_table(partition_key, sort_key);
	`

	for _, gsi := range globalSecondarySettings {
		gsiTableName := "gsi_" + *gsi.IndexName
		sqlStmt += `
		create table ` + gsiTableName + ` (primary_key blob not null primary key, body blob, main_partition_key blob, main_sort_key blob, partition_key blob, sort_key blob);
		delete from ` + gsiTableName + `;
		create index idx_` + gsiTableName + `_partition_key_sort_key on gsi_` + *gsi.IndexName + `(partition_key, sort_key);
		`
	}

	_, err = db.Exec(sqlStmt)
	if err != nil {
		panic(err)
	}

	return &InnerTable{
		db:                           db,
		partitionKeyName:             partitionKeyName,
		sortKeyName:                  sortKeyName,
		globalSecondaryIndexSettings: globalSecondarySettings,
	}
}

func listTables(db *sql.DB) {
	query := `SELECT name FROM sqlite_master WHERE type='table'`
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("failed to list tables: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			log.Fatalf("failed to list tables: %v", err)
		}
	}
}
func (t *InnerTable) Put(entry *Entry) error {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	entryWrapper := &EntryWrapper{
		Entry:     entry,
		IsDeleted: false,
		CreatedAt: time.Now(),
	}
	return t.put(entryWrapper)
}

func (t *InnerTable) put(entry *EntryWrapper) error {
	primaryKey, err := t.buildTablePrimaryKey(entry.Entry)
	if err != nil {
		return err
	}

	txn, err := t.db.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	tuple, err := t.getTuple(primaryKey.Bytes(), "main_table", txn)
	if err != nil {
		return err
	}

	if tuple == nil {
		stmt, err := txn.Prepare("insert into main_table(primary_key, body, partition_key, sort_key) values(?, ?, ?, ?)")

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

		err = t.syncGlobalSecondaryIndices(primaryKey, entry, txn)
		if err != nil {
			return err
		}
	} else {
		stmt, err := txn.Prepare("update main_table set body = ? where primary_key = ?")

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

		err = t.syncGlobalSecondaryIndices(primaryKey, entry, txn)
		if err != nil {
			return err
		}
	}

	return txn.Commit()
}

func (t *InnerTable) Delete(req *DeleteRequest) error {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	entryWrapper := &EntryWrapper{
		Entry:     req.Entry,
		IsDeleted: true,
		CreatedAt: time.Now(),
	}
	return t.put(entryWrapper)
}

func (t *InnerTable) syncGlobalSecondaryIndices(primaryKey *PrimaryKey, entry *EntryWrapper, txn *sql.Tx) error {
	for _, gsi := range t.globalSecondaryIndexSettings {
		tableName := "gsi_" + *gsi.IndexName
		tuple, err := t.getTuple(primaryKey.Bytes(), tableName, txn)
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

		gsiEntry := t.newGsiEntry(entry, gsi)

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

func (t *InnerTable) getTuple(primaryKey []byte, tableName string, txn *sql.Tx) (*Tuple, error) {
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

func (t *InnerTable) newGsiEntry(entry *EntryWrapper, gsi GlobalSecondaryIndexSetting) *EntryWrapper {
	gsiEntry := &Entry{
		Body: make(map[string]AttributeValue),
	}
	gsiEntry.Body[*t.partitionKeyName] = entry.Entry.Body[*t.partitionKeyName]
	gsiEntry.Body[*t.sortKeyName] = entry.Entry.Body[*t.sortKeyName]
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
}

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

func (t *InnerTable) buildTablePrimaryKey(entry *Entry) (*PrimaryKey, error) {
	primaryKey := &PrimaryKey{
		PartitionKey: make([]byte, 0),
		SortKey:      make([]byte, 0),
	}

	pk, ok := entry.Body[*t.partitionKeyName]
	if !ok {
		return primaryKey, errors.New("partitionKey not found")
	}

	primaryKey.PartitionKey = pk.Bytes()

	if t.sortKeyName != nil {
		sk, ok := entry.Body[*t.sortKeyName]
		if !ok {
			return primaryKey, errors.New("sortKey not found")
		}
		primaryKey.SortKey = sk.Bytes()
	}

	return primaryKey, nil
}

func (t *InnerTable) Get(req *GetRequest) (*Entry, error) {
	t.rwMutex.RLock()
	defer t.rwMutex.RUnlock()

	txn, err := t.db.Begin()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	primaryKey, err := t.buildTablePrimaryKey(req.Entry)
	if err != nil {
		return nil, nil
	}

	tuple, err := t.getTuple(primaryKey.Bytes(), "main_table", txn)
	if err != nil {
		return nil, err
	}
	if tuple == nil {
		return nil, nil
	}

	if req.ConsistentRead {
		return tuple.currentEntry(), txn.Commit()
	} else {
		return tuple.prevEntry(), txn.Commit()
	}
}

type QueryResponse struct {
	Entries      []*Entry
	ScannedCount int32
}

func (t *InnerTable) Query(req *Query) (*QueryResponse, error) {
	t.rwMutex.RLock()
	defer t.rwMutex.RUnlock()

	res := &QueryResponse{}

	tableName := "main_table"
	if req.IndexName != nil {
		for _, gsi := range t.globalSecondaryIndexSettings {
			if *gsi.IndexName == *req.IndexName {
				tableName = "gsi_" + *gsi.IndexName
				break
			}
		}

		if tableName == "main_table" {
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

	rows, err := t.db.Query(queryStmt, args...)
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

package ddb

import (
	"fmt"
	"log"
	"testing"
)

func createTestInnerStorageWithGSI(gsiSettings []GlobalSecondaryIndexSetting) *InnerStorage {
	storage := NewInnerStorage()
	tableMetaData := &TableMetaData{
		Name:                         "test",
		GlobalSecondaryIndexSettings: gsiSettings,
		PartitionKeySchema: &KeySchema{
			AttributeName: "partitionKey",
		},
		SortKeySchema: &KeySchema{
			AttributeName: "sortKey",
		},
	}
	err := storage.CreateTable(tableMetaData)
	if err != nil {
		panic(err)
	}

	return storage
}

func TestInnerStorageQueryWithGsiProjections(t *testing.T) {
	type testCase struct {
		projectionType ProjectionType
		attributeNames []string
	}

	testCases := []testCase{
		{
			projectionType: PROJECTION_TYPE_KEYS_ONLY,
			attributeNames: []string{
				"partitionKey",
				"sortKey",
				"gsi1PartitionKey",
				"gsi1SortKey",
			},
		},
		{
			projectionType: PROJECTION_TYPE_INCLUDE,
			attributeNames: []string{
				"partitionKey",
				"sortKey",
				"gsi1PartitionKey",
				"gsi1SortKey",
				"message",
			},
		},
		{
			projectionType: PROJECTION_TYPE_ALL,
			attributeNames: []string{
				"partitionKey",
				"sortKey",
				"gsi1PartitionKey",
				"gsi1SortKey",
				"message",
				"version",
			},
		},
	}

	for _, testCase := range testCases {
		log.Println("projectionType: ", testCase.projectionType)

		gsiName := "gsi1"
		gsiPartitionKeyName := "gsi1PartitionKey"
		gsiSortKeyName := "gsi1SortKey"
		gsiSettings := []GlobalSecondaryIndexSetting{
			{
				IndexName:        &gsiName,
				PartitionKeyName: &gsiPartitionKeyName,
				SortKeyName:      &gsiSortKeyName,
				ProjectionType:   testCase.projectionType,
				NonKeyAttributes: testCase.attributeNames,
			},
		}
		storage := createTestInnerStorageWithGSI(gsiSettings)

		// Insert entry
		body := make(map[string]AttributeValue)
		partitionKey := "foo"
		body["partitionKey"] = AttributeValue{S: &partitionKey}
		sortKey := "bar"
		body["sortKey"] = AttributeValue{S: &sortKey}
		gsiPartitionKey := "gsiFoo"
		body["gsi1PartitionKey"] = AttributeValue{S: &gsiPartitionKey}
		gsiSortKey := "gsiBar"
		body["gsi1SortKey"] = AttributeValue{S: &gsiSortKey}
		message := "hola"
		body["message"] = AttributeValue{S: &message}
		version := "1"
		body["version"] = AttributeValue{N: &version}
		entry := &Entry{
			Body: body,
		}

		err := storage.Put(&PutRequest{
			Entry:     entry,
			TableName: "test",
		})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		{
			partitionKey := []byte(gsiPartitionKey)
			query := &Query{
				IndexName:      &gsiName,
				PartitionKey:   &partitionKey,
				ConsistentRead: true,
				Limit:          1,
				TableName:      "test",
			}

			res, err := storage.Query(query)

			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			entries := res.Entries
			if len(entries) != 1 {
				t.Fatalf("Query failed: expected 1 entry but got %d", len(entries))
			}
			expectedEntry := &Entry{
				Body: make(map[string]AttributeValue),
			}
			for _, attributeName := range testCase.attributeNames {
				expectedEntry.Body[attributeName] = entry.Body[attributeName]
			}
			assertEntry(entries[0], expectedEntry, t)
		}

	}
}

func TestInnerStoragePutGetAndDelete(t *testing.T) {
	storage := createTestInnerStorageWithGSI([]GlobalSecondaryIndexSetting{})
	body := make(map[string]AttributeValue)
	partitionKey := "foo"
	body["partitionKey"] = AttributeValue{S: &partitionKey}
	sortKey := "bar"
	body["sortKey"] = AttributeValue{S: &sortKey}
	version := "1"
	body["version"] = AttributeValue{N: &version}
	entry := &Entry{
		Body: body,
	}
	tableName := "test"

	err := storage.Put(&PutRequest{
		Entry:     entry,
		TableName: "test",
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	getReq := &GetRequest{
		Entry:          entry,
		ConsistentRead: true,
		TableName:      tableName,
	}
	entry2, err := storage.Get(getReq)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	assertEntry(entry2, entry, t)

	getReq = &GetRequest{
		Entry:          entry,
		ConsistentRead: false,
		TableName:      tableName,
	}
	entry3, err := storage.Get(getReq)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	assertEntry(entry3, nil, t)

	bodyV2 := make(map[string]AttributeValue)
	bodyV2["partitionKey"] = AttributeValue{S: &partitionKey}
	bodyV2["sortKey"] = AttributeValue{S: &sortKey}
	versionV2 := "2"
	bodyV2["version"] = AttributeValue{N: &versionV2}
	entryV2 := &Entry{
		Body: bodyV2,
	}

	err = storage.Put(&PutRequest{
		Entry:     entryV2,
		TableName: "test",
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	getReq = &GetRequest{
		Entry:          entry,
		ConsistentRead: true,
		TableName:      tableName,
	}
	entry4, err := storage.Get(getReq)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	assertEntry(entry4, entryV2, t)

	getReq = &GetRequest{
		Entry:          entry,
		ConsistentRead: false,
		TableName:      tableName,
	}
	entry5, err := storage.Get(getReq)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	assertEntry(entry5, entry, t)

	bodyV3 := make(map[string]AttributeValue)
	bodyV3["partitionKey"] = AttributeValue{S: &partitionKey}
	bodyV3["sortKey"] = AttributeValue{S: &sortKey}
	deleteReq := &DeleteRequest{
		Entry: &Entry{
			Body: bodyV3,
		},
		TableName: tableName,
	}

	err = storage.Delete(deleteReq)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	getReq = &GetRequest{
		Entry:          entry,
		ConsistentRead: true,
		TableName:      tableName,
	}
	entry6, err := storage.Get(getReq)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	assertEntry(entry6, nil, t)

	getReq = &GetRequest{
		Entry:          entry,
		ConsistentRead: false,
		TableName:      tableName,
	}
	entry7, err := storage.Get(getReq)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	assertEntry(entry7, entryV2, t)
}

func assertEntry(actual *Entry, expected *Entry, t *testing.T) {
	t.Helper()
	if actual == nil && expected == nil {
		return
	} else if expected == nil {
		t.Fatalf("expected entry to be nil but got: %v", actual)
	} else if actual == nil {
		t.Fatalf("expected entry is not nil but got actual is nil")
	}

	if actual.Body == nil {
		t.Fatalf("Get failed: body is nil")
	}

	for k, v := range expected.Body {
		v2, ok := actual.Body[k]
		if !ok {
			t.Fatalf("Get failed: key %s not found", k)
		}

		if !v2.Equal(v) {
			t.Fatalf("Get failed: key=%s, value=%s, expected=%v", k, v2, v)
		}
	}
}

func TestInnerStorageQuery(t *testing.T) {
	storage := createTestInnerStorageWithGSI([]GlobalSecondaryIndexSetting{})
	count := 4
	i := 0
	expectedEntries := make([]*Entry, count)
	for i < count {
		body := make(map[string]AttributeValue)
		partitionKey := "foo"
		body["partitionKey"] = AttributeValue{S: &partitionKey}
		sortKey := fmt.Sprintf("bar%d", i)
		body["sortKey"] = AttributeValue{S: &sortKey}
		version := "1"
		body["version"] = AttributeValue{N: &version}
		entry := &Entry{
			Body: body,
		}

		err := storage.Put(&PutRequest{
			Entry:     entry,
			TableName: "test",
		})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		expectedEntries[i] = entry
		i += 1
	}

	// Test query with ScanIndexForward true
	{

		partitionKey := []byte("foo")
		req := &Query{
			PartitionKey:     &partitionKey,
			ScanIndexForward: true,
			Limit:            2,
			ConsistentRead:   true,
			TableName:        "test",
		}
		res, err := storage.Query(req)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		entries := res.Entries
		if len(entries) != 2 {
			t.Fatalf("Query failed: expected 2 Entries but got %d", len(entries))
		}
		assertEntry(entries[0], expectedEntries[0], t)
		assertEntry(entries[1], expectedEntries[1], t)
	}

	// Test query with ScanIndexForward false
	{
		partitionKey := []byte("foo")
		req := &Query{
			PartitionKey:     &partitionKey,
			ScanIndexForward: false,
			Limit:            2,
			ConsistentRead:   true,
			TableName:        "test",
		}
		res, err := storage.Query(req)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		entries := res.Entries
		if len(entries) != 2 {
			t.Fatalf("Query failed: expected 2 Entries but got %d", len(entries))
		}
		assertEntry(entries[0], expectedEntries[3], t)
		assertEntry(entries[1], expectedEntries[2], t)
	}

	// Test query with ExclusiveStartKey
	{
		partitionKey := []byte("foo")
		exclusiveSortKey := []byte("foo|bar1")
		req := &Query{
			PartitionKey:      &partitionKey,
			ScanIndexForward:  true,
			Limit:             2,
			ConsistentRead:    true,
			ExclusiveStartKey: &exclusiveSortKey,
			TableName:         "test",
		}
		res, err := storage.Query(req)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		entries := res.Entries
		if len(entries) != 2 {
			t.Fatalf("Query failed: expected 2 Entries but got %d", len(entries))
		}
		assertEntry(entries[0], expectedEntries[2], t)
		assertEntry(entries[1], expectedEntries[3], t)
	}

	// Test query with SortKeyPredicate
	{
		partitionKey := []byte("foo")
		sortKeyPredicate := func(entry *Entry) (bool, error) {
			sortKey, ok := entry.Body["sortKey"]
			if !ok {
				return false, nil
			}
			return *sortKey.S == "bar2", nil
		}
		req := &Query{
			PartitionKey:     &partitionKey,
			ScanIndexForward: true,
			Limit:            2,
			ConsistentRead:   true,
			SortKeyPredicate: (*Predicate)(&sortKeyPredicate),
			TableName:        "test",
		}

		res, err := storage.Query(req)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		entries := res.Entries
		if len(entries) != 1 {
			t.Fatalf("Query failed: expected 1 entry but got %d", len(entries))
		}
		assertEntry(entries[0], expectedEntries[2], t)
	}

}

func TestInnerStorageQueryWithGsi(t *testing.T) {
	gsiName := "gsi1"
	gsiPartitionKeyName := "gsi1PartitionKey"
	gsiSortKeyName := "gsi1SortKey"
	gsiSettings := []GlobalSecondaryIndexSetting{
		{
			IndexName:        &gsiName,
			PartitionKeyName: &gsiPartitionKeyName,
			SortKeyName:      &gsiSortKeyName,
			ProjectionType:   PROJECTION_TYPE_ALL,
		},
	}
	storage := createTestInnerStorageWithGSI(gsiSettings)
	count := 4
	i := 0
	expectedEntries := make([]*Entry, count)
	for i < count {
		body := make(map[string]AttributeValue)
		partitionKey := "foo"
		body["partitionKey"] = AttributeValue{S: &partitionKey}
		sortKey := fmt.Sprintf("bar%d", i)
		body["sortKey"] = AttributeValue{S: &sortKey}
		gsiPartitionKey := fmt.Sprintf("gsiFoo")
		body["gsi1PartitionKey"] = AttributeValue{S: &gsiPartitionKey}
		gsiSortKey := fmt.Sprintf("gsiBar%d", i)
		body["gsi1SortKey"] = AttributeValue{S: &gsiSortKey}
		version := "1"
		body["version"] = AttributeValue{N: &version}
		entry := &Entry{
			Body: body,
		}

		err := storage.Put(&PutRequest{
			Entry:     entry,
			TableName: "test",
		})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		expectedEntries[i] = entry
		i += 1
	}

	// Test query with ScanIndexForward true
	{

		partitionKey := []byte("gsiFoo")
		req := &Query{
			IndexName:        &gsiName,
			PartitionKey:     &partitionKey,
			ScanIndexForward: true,
			Limit:            2,
			ConsistentRead:   true,
			TableName:        "test",
		}
		res, err := storage.Query(req)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		entries := res.Entries
		if len(entries) != 2 {
			t.Fatalf("Query failed: expected 2 Entries but got %d", len(entries))
		}
		assertEntry(entries[0], expectedEntries[0], t)
		assertEntry(entries[1], expectedEntries[1], t)
	}

	// Test query with ScanIndexForward false
	{
		partitionKey := []byte("gsiFoo")
		req := &Query{
			IndexName:        &gsiName,
			PartitionKey:     &partitionKey,
			ScanIndexForward: false,
			Limit:            2,
			ConsistentRead:   true,
			TableName:        "test",
		}
		res, err := storage.Query(req)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		entries := res.Entries
		if len(entries) != 2 {
			t.Fatalf("Query failed: expected 2 Entries but got %d", len(entries))
		}
		assertEntry(entries[0], expectedEntries[3], t)
		assertEntry(entries[1], expectedEntries[2], t)
	}

	// Test query with ExclusiveStartKey
	{
		partitionKey := []byte("gsiFoo")
		exclusiveSortKey := []byte("foo|bar1")
		req := &Query{
			IndexName:         &gsiName,
			PartitionKey:      &partitionKey,
			ScanIndexForward:  true,
			Limit:             2,
			ConsistentRead:    true,
			ExclusiveStartKey: &exclusiveSortKey,
			TableName:         "test",
		}
		res, err := storage.Query(req)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		entries := res.Entries
		if len(entries) != 2 {
			t.Fatalf("Query failed: expected 2 Entries but got %d", len(entries))
		}
		assertEntry(entries[0], expectedEntries[2], t)
		assertEntry(entries[1], expectedEntries[3], t)
	}

	// Test query with SortKeyPredicate
	{
		partitionKey := []byte("gsiFoo")
		sortKeyPredicate := func(entry *Entry) (bool, error) {
			sortKey, ok := entry.Body["gsi1SortKey"]
			if !ok {
				return false, nil
			}
			return *sortKey.S == "gsiBar2", nil
		}
		req := &Query{
			IndexName:        &gsiName,
			PartitionKey:     &partitionKey,
			ScanIndexForward: true,
			Limit:            2,
			ConsistentRead:   true,
			SortKeyPredicate: (*Predicate)(&sortKeyPredicate),
			TableName:        "test",
		}

		res, err := storage.Query(req)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		entries := res.Entries
		if len(entries) != 1 {
			t.Fatalf("Query failed: expected 1 entry but got %d", len(entries))
		}
		assertEntry(entries[0], expectedEntries[2], t)
	}
}

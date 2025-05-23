package inner_storage

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ocowchun/baddb/ddb/core"
	"github.com/ocowchun/baddb/ddb/query"
	"github.com/ocowchun/baddb/ddb/update"
	"testing"
)

func createTestInnerStorageWithGSI(gsiSettings []core.GlobalSecondaryIndexSetting) *InnerStorage {
	return createTestInnerStorage(
		0,
		0,
		core.BILLING_MODE_PAY_PER_REQUEST,
		gsiSettings,
	)
}

func createTestInnerStorage(
	ReadCapacityUnits int64,
	WriteCapacityUnits int64,
	mode core.BillingMode,
	gsiSettings []core.GlobalSecondaryIndexSetting,
) *InnerStorage {
	storage := NewInnerStorage()
	tableMetaData := &core.TableMetaData{
		Name:                         "test",
		GlobalSecondaryIndexSettings: gsiSettings,
		PartitionKeySchema: &core.KeySchema{
			AttributeName: "partitionKey",
		},
		SortKeySchema: &core.KeySchema{
			AttributeName: "sortKey",
		},
		BillingMode: mode,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  &ReadCapacityUnits,
			WriteCapacityUnits: &WriteCapacityUnits,
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
		projectionType core.ProjectionType
		attributeNames []string
	}

	testCases := []testCase{
		{
			projectionType: core.PROJECTION_TYPE_KEYS_ONLY,
			attributeNames: []string{
				"partitionKey",
				"sortKey",
				"gsi1PartitionKey",
				"gsi1SortKey",
			},
		},
		{
			projectionType: core.PROJECTION_TYPE_INCLUDE,
			attributeNames: []string{
				"partitionKey",
				"sortKey",
				"gsi1PartitionKey",
				"gsi1SortKey",
				"message",
			},
		},
		{
			projectionType: core.PROJECTION_TYPE_ALL,
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

		gsiName := "gsi1"
		gsiPartitionKeyName := "gsi1PartitionKey"
		gsiSortKeyName := "gsi1SortKey"
		gsiSettings := []core.GlobalSecondaryIndexSetting{
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
		body := make(map[string]core.AttributeValue)
		partitionKey := "foo"
		body["partitionKey"] = core.AttributeValue{S: &partitionKey}
		sortKey := "bar"
		body["sortKey"] = core.AttributeValue{S: &sortKey}
		gsiPartitionKey := "gsiFoo"
		body["gsi1PartitionKey"] = core.AttributeValue{S: &gsiPartitionKey}
		gsiSortKey := "gsiBar"
		body["gsi1SortKey"] = core.AttributeValue{S: &gsiSortKey}
		message := "hola"
		body["message"] = core.AttributeValue{S: &message}
		version := "1"
		body["version"] = core.AttributeValue{N: &version}
		entry := &core.Entry{
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
			query := &query.Query{
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
			expectedEntry := &core.Entry{
				Body: make(map[string]core.AttributeValue),
			}
			for _, attributeName := range testCase.attributeNames {
				expectedEntry.Body[attributeName] = entry.Body[attributeName]
			}
			assertEntry(entries[0], expectedEntry, t)
		}

	}
}

func TestInnerStoragePutGetAndDelete(t *testing.T) {
	storage := createTestInnerStorageWithGSI([]core.GlobalSecondaryIndexSetting{})
	body := make(map[string]core.AttributeValue)
	partitionKey := "foo"
	body["partitionKey"] = core.AttributeValue{S: &partitionKey}
	sortKey := "bar"
	body["sortKey"] = core.AttributeValue{S: &sortKey}
	version := "1"
	body["version"] = core.AttributeValue{N: &version}
	entry := &core.Entry{
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

	bodyV2 := make(map[string]core.AttributeValue)
	bodyV2["partitionKey"] = core.AttributeValue{S: &partitionKey}
	bodyV2["sortKey"] = core.AttributeValue{S: &sortKey}
	versionV2 := "2"
	bodyV2["version"] = core.AttributeValue{N: &versionV2}
	entryV2 := &core.Entry{
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

	bodyV3 := make(map[string]core.AttributeValue)
	bodyV3["partitionKey"] = core.AttributeValue{S: &partitionKey}
	bodyV3["sortKey"] = core.AttributeValue{S: &sortKey}
	deleteReq := &DeleteRequest{
		Entry: &core.Entry{
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

func TestInnerStorageReadLimitReached(t *testing.T) {
	storage := createTestInnerStorage(
		1,
		1,
		core.BILLING_MODE_PROVISIONED,
		[]core.GlobalSecondaryIndexSetting{},
	)
	body := make(map[string]core.AttributeValue)
	partitionKey := "foo"
	body["partitionKey"] = core.AttributeValue{S: &partitionKey}
	sortKey := "bar"
	body["sortKey"] = core.AttributeValue{S: &sortKey}
	version := "1"
	body["version"] = core.AttributeValue{N: &version}
	entry := &core.Entry{
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

	for i := 0; i < 10; i++ {
		getReq := &GetRequest{
			Entry:          entry,
			ConsistentRead: true,
			TableName:      tableName,
		}
		_, err = storage.Get(getReq)
		if err != nil {
			if errors.Is(err, RateLimitReachedError) {
				return
			}

			t.Fatalf("Get failed: %v", err)
		}
	}

	if err == nil {
		t.Fatalf("Get should have failed with RateLimitReachedError")
	}
}

func TestInnerStorageWriteLimitReached(t *testing.T) {
	storage := createTestInnerStorage(
		1,
		1,
		core.BILLING_MODE_PROVISIONED,
		[]core.GlobalSecondaryIndexSetting{},
	)
	body := make(map[string]core.AttributeValue)
	partitionKey := "foo"
	body["partitionKey"] = core.AttributeValue{S: &partitionKey}
	sortKey := "bar"
	body["sortKey"] = core.AttributeValue{S: &sortKey}
	version := "1"
	body["version"] = core.AttributeValue{N: &version}
	entry := &core.Entry{
		Body: body,
	}

	var err error
	for i := 0; i < 10; i++ {
		err = storage.Put(&PutRequest{
			Entry:     entry,
			TableName: "test",
		})

		if err != nil {
			if errors.Is(err, RateLimitReachedError) {
				return
			}

			t.Fatalf("Get failed: %v", err)
		}
	}
	if err == nil {
		t.Fatalf("Put should have failed with RateLimitReachedError")
	}
}

func assertEntry(actual *core.Entry, expected *core.Entry, t *testing.T) {
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
	storage := createTestInnerStorageWithGSI([]core.GlobalSecondaryIndexSetting{})
	count := 4
	i := 0
	expectedEntries := make([]*core.Entry, count)
	for i < count {
		body := make(map[string]core.AttributeValue)
		partitionKey := "foo"
		body["partitionKey"] = core.AttributeValue{S: &partitionKey}
		sortKey := fmt.Sprintf("bar%d", i)
		body["sortKey"] = core.AttributeValue{S: &sortKey}
		version := "1"
		body["version"] = core.AttributeValue{N: &version}
		entry := &core.Entry{
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
		req := &query.Query{
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
		req := &query.Query{
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
		req := &query.Query{
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
		sortKeyPredicate := func(entry *core.Entry) (bool, error) {
			sortKey, ok := entry.Body["sortKey"]
			if !ok {
				return false, nil
			}
			return *sortKey.S == "bar2", nil
		}
		req := &query.Query{
			PartitionKey:     &partitionKey,
			ScanIndexForward: true,
			Limit:            2,
			ConsistentRead:   true,
			SortKeyPredicate: (*query.Predicate)(&sortKeyPredicate),
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

func TestInnerStorageQueryWithGsiNoSortKey(t *testing.T) {
	gsiName := "gsi1"
	gsiPartitionKeyName := "gsi1PartitionKey"
	gsiSettings := []core.GlobalSecondaryIndexSetting{
		{
			IndexName:        &gsiName,
			PartitionKeyName: &gsiPartitionKeyName,
			ProjectionType:   core.PROJECTION_TYPE_ALL,
		},
	}
	storage := createTestInnerStorageWithGSI(gsiSettings)
	count := 4
	i := 0
	expectedEntries := make([]*core.Entry, count)
	for i < count {
		body := make(map[string]core.AttributeValue)
		partitionKey := "foo"
		body["partitionKey"] = core.AttributeValue{S: &partitionKey}
		sortKey := fmt.Sprintf("bar%d", i)
		body["sortKey"] = core.AttributeValue{S: &sortKey}
		gsiPartitionKey := fmt.Sprintf("gsiFoo")
		body["gsi1PartitionKey"] = core.AttributeValue{S: &gsiPartitionKey}
		gsiSortKey := fmt.Sprintf("gsiBar%d", i)
		body["gsi1SortKey"] = core.AttributeValue{S: &gsiSortKey}
		version := "1"
		body["version"] = core.AttributeValue{N: &version}
		entry := &core.Entry{
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
		req := &query.Query{
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
}

func TestInnerStorageQueryWithGsi(t *testing.T) {
	gsiName := "gsi1"
	gsiPartitionKeyName := "gsi1PartitionKey"
	gsiSortKeyName := "gsi1SortKey"
	gsiSettings := []core.GlobalSecondaryIndexSetting{
		{
			IndexName:        &gsiName,
			PartitionKeyName: &gsiPartitionKeyName,
			SortKeyName:      &gsiSortKeyName,
			ProjectionType:   core.PROJECTION_TYPE_ALL,
		},
	}
	storage := createTestInnerStorageWithGSI(gsiSettings)
	count := 4
	i := 0
	expectedEntries := make([]*core.Entry, count)
	for i < count {
		body := make(map[string]core.AttributeValue)
		partitionKey := "foo"
		body["partitionKey"] = core.AttributeValue{S: &partitionKey}
		sortKey := fmt.Sprintf("bar%d", i)
		body["sortKey"] = core.AttributeValue{S: &sortKey}
		gsiPartitionKey := fmt.Sprintf("gsiFoo")
		body["gsi1PartitionKey"] = core.AttributeValue{S: &gsiPartitionKey}
		gsiSortKey := fmt.Sprintf("gsiBar%d", i)
		body["gsi1SortKey"] = core.AttributeValue{S: &gsiSortKey}
		version := "1"
		body["version"] = core.AttributeValue{N: &version}
		entry := &core.Entry{
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
		req := &query.Query{
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
		req := &query.Query{
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
		req := &query.Query{
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
		sortKeyPredicate := func(entry *core.Entry) (bool, error) {
			sortKey, ok := entry.Body["gsi1SortKey"]
			if !ok {
				return false, nil
			}
			return *sortKey.S == "gsiBar2", nil
		}
		req := &query.Query{
			IndexName:        &gsiName,
			PartitionKey:     &partitionKey,
			ScanIndexForward: true,
			Limit:            2,
			ConsistentRead:   true,
			SortKeyPredicate: (*query.Predicate)(&sortKeyPredicate),
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

func TestInnerStorageUpdate(t *testing.T) {
	storage := createTestInnerStorageWithGSI([]core.GlobalSecondaryIndexSetting{})
	tableName := "test"

	partitionKey := "foo"
	sortKey := "bar"

	tests := []struct {
		name                      string
		updateExpressionContent   string
		expressionAttributeNames  map[string]string
		expressionAttributeValues map[string]core.AttributeValue
		itemExists                bool
		expected                  map[string]core.AttributeValue
		expectError               bool
	}{
		{
			name:                    "Update existing attribute",
			updateExpressionContent: "SET version = :newVersion",
			expressionAttributeValues: map[string]core.AttributeValue{
				":newVersion": {N: aws.String("2")},
			},
			itemExists: true,
			expected: map[string]core.AttributeValue{
				"partitionKey": {S: &partitionKey},
				"sortKey":      {S: &sortKey},
				"version":      {N: aws.String("2")},
			},
			expectError: false,
		},
		{
			name:                    "Add new attribute",
			updateExpressionContent: "SET newAttribute = :newValue",
			expressionAttributeValues: map[string]core.AttributeValue{
				":newValue": {S: aws.String("newValue")},
			},
			itemExists: true,
			expected: map[string]core.AttributeValue{
				"partitionKey": {S: &partitionKey},
				"sortKey":      {S: &sortKey},
				"version":      {N: aws.String("1")},
				"newAttribute": {S: aws.String("newValue")},
			},
			expectError: false,
		},
		{
			name:                      "Remove existing attribute",
			updateExpressionContent:   "REMOVE version",
			expressionAttributeValues: map[string]core.AttributeValue{},
			itemExists:                true,
			expected: map[string]core.AttributeValue{
				"partitionKey": {S: &partitionKey},
				"sortKey":      {S: &sortKey},
			},
			expectError: false,
		},
		{
			name:                    "Update non-existent attribute",
			updateExpressionContent: "SET nonExistent = :value",
			expressionAttributeValues: map[string]core.AttributeValue{
				":value": {S: aws.String("value")},
			},
			itemExists: true,
			expected: map[string]core.AttributeValue{
				"partitionKey": {S: &partitionKey},
				"sortKey":      {S: &sortKey},
				"version":      {N: aws.String("1")},
				"nonExistent":  {S: aws.String("value")},
			},
			expectError: false,
		},
		{
			name:                    "Update with non-existent item",
			updateExpressionContent: "SET newAttribute = :newValue",
			expressionAttributeValues: map[string]core.AttributeValue{
				":newValue": {S: aws.String("newValue")},
			},
			itemExists: false,
			expected: map[string]core.AttributeValue{
				"partitionKey": {S: &partitionKey},
				"sortKey":      {S: &sortKey},
				"newAttribute": {S: aws.String("newValue")},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Insert initial entry
			if tt.itemExists {
				body := make(map[string]core.AttributeValue)
				partitionKey := "foo"
				body["partitionKey"] = core.AttributeValue{S: &partitionKey}
				sortKey := "bar"
				body["sortKey"] = core.AttributeValue{S: &sortKey}
				version := "1"
				body["version"] = core.AttributeValue{N: &version}
				entry := &core.Entry{
					Body: body,
				}

				err := storage.Put(&PutRequest{
					Entry:     entry,
					TableName: tableName,
				})
				if err != nil {
					t.Fatalf("Put failed: %v", err)
				}
			}

			body := make(map[string]core.AttributeValue)
			body["partitionKey"] = core.AttributeValue{S: &partitionKey}
			body["sortKey"] = core.AttributeValue{S: &sortKey}
			key := &core.Entry{
				Body: body,
			}

			operation, err := update.BuildUpdateOperation(
				tt.updateExpressionContent,
				tt.expressionAttributeNames,
				tt.expressionAttributeValues,
			)
			if err != nil {
				t.Fatalf("Unexpected error: %v, when build operation", err)
			}

			_, err = storage.Update(&UpdateRequest{
				Key:             key,
				UpdateOperation: operation,
				TableName:       tableName,
			})

			if (err != nil) != tt.expectError {
				t.Fatalf("Expected error: %v, got: %v", tt.expectError, err)
			}

			getReq := &GetRequest{
				Entry:          key,
				ConsistentRead: true,
				TableName:      tableName,
			}
			updatedEntry, err := storage.Get(getReq)
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			for key, expectedValue := range tt.expected {
				if val, ok := updatedEntry.Body[key]; !ok || !val.Equal(expectedValue) {
					t.Fatalf("Expected %v for body %s, got %v", expectedValue, key, val)
				}
			}
		})
	}
}

func TestInnerStorageQueryItemCount(t *testing.T) {
	storage := createTestInnerStorageWithGSI([]core.GlobalSecondaryIndexSetting{})
	tableName := "test"

	// Insert items
	for i := 0; i < 5; i++ {
		body := make(map[string]core.AttributeValue)
		partitionKey := fmt.Sprintf("foo%d", i)
		body["partitionKey"] = core.AttributeValue{S: &partitionKey}
		sortKey := fmt.Sprintf("bar%d", i)
		body["sortKey"] = core.AttributeValue{S: &sortKey}
		version := "1"
		body["version"] = core.AttributeValue{N: &version}
		entry := &core.Entry{
			Body: body,
		}

		err := storage.Put(&PutRequest{
			Entry:     entry,
			TableName: tableName,
		})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query item count
	count, err := storage.QueryItemCount(tableName)
	if err != nil {
		t.Fatalf("QueryItemCount failed: %v", err)
	}

	// Verify item count
	if count != 5 {
		t.Fatalf("Expected item count to be 5, but got %d", count)
	}
}

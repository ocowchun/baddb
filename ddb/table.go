package ddb

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ocowchun/baddb/ddb/expression"
	"time"
)

type TableMetaData struct {
	Name                         string
	AttributeDefinitions         []types.AttributeDefinition
	KeySchema                    []types.KeySchemaElement
	GlobalSecondaryIndexSettings []GlobalSecondaryIndexSetting
	LocalSecondaryIndexes        []types.LocalSecondaryIndex
	ProvisionedThroughput        *types.ProvisionedThroughput
	CreationDateTime             *time.Time
	partitionKeySchema           *types.KeySchemaElement
	sortKeySchema                *types.KeySchemaElement
}

type Table struct {
	meta  *TableMetaData
	inner *InnerTable
}

type ProjectionType uint8

const (
	PROJECTION_TYPE_KEYS_ONLY ProjectionType = iota
	PROJECTION_TYPE_INCLUDE
	PROJECTION_TYPE_ALL
)

type GlobalSecondaryIndexSetting struct {
	IndexName        *string
	PartitionKeyName *string
	SortKeyName      *string
	NonKeyAttributes []string
	ProjectionType   ProjectionType
}

func NewTable(meta *TableMetaData) *Table {
	partitionKeyName := meta.partitionKeySchema.AttributeName
	var sortKeyName *string
	if meta.sortKeySchema != nil {
		sortKeyName = meta.sortKeySchema.AttributeName
	}

	gsiSettings := meta.GlobalSecondaryIndexSettings

	return &Table{
		meta:  meta,
		inner: NewInnerTable(partitionKeyName, sortKeyName, gsiSettings),
	}
}

func (t *Table) Description() *types.TableDescription {
	tableDescription := &types.TableDescription{
		AttributeDefinitions: t.meta.AttributeDefinitions,
		CreationDateTime:     t.meta.CreationDateTime,

		TableName:   &t.meta.Name,
		TableStatus: types.TableStatusActive,
	}

	return tableDescription
}

func (t *Table) buildEntryWithKey(item map[string]types.AttributeValue) (*EntryWithKey, error) {
	entry := NewEntryFromItem(item)

	partitionKeyName := *t.meta.partitionKeySchema.AttributeName
	val, ok := entry.Body[partitionKeyName]
	if !ok {
		return nil, fmt.Errorf("HashKey %s not found in input", partitionKeyName)
	}
	hashKey := val.Bytes()
	primaryKey := hashKey

	if t.meta.sortKeySchema != nil {
		sortKeyName := *t.meta.sortKeySchema.AttributeName
		val, ok := entry.Body[sortKeyName]
		if !ok {
			return nil, fmt.Errorf("HashKey %s not found in input", sortKeyName)
		}
		primaryKey = append(primaryKey, []byte("|")...)
		primaryKey = append(primaryKey, val.Bytes()...)
	}

	entry2 := &EntryWithKey{
		Key:   primaryKey,
		Entry: entry,
	}

	return entry2, nil

}

func (t *Table) Put(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	entry := NewEntryFromItem(input.Item)

	err := t.inner.Put(entry)

	return nil, err
}

type DeleteRequest struct {
	Entry *Entry
	//key []byte
}

func (t *Table) Delete(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	entry := NewEntryFromItem(input.Key)

	req := &DeleteRequest{
		Entry: entry,
	}
	err := t.inner.Delete(req)

	output := &dynamodb.DeleteItemOutput{}

	return output, err
}

func (t *Table) Get(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	consistentRead := false
	if input.ConsistentRead != nil {
		consistentRead = *input.ConsistentRead
	}

	req := &GetRequest{
		Entry:          NewEntryFromItem(input.Key),
		ConsistentRead: consistentRead,
	}
	entry, err := t.inner.Get(req)

	if err != nil {
		return nil, err
	}
	if entry == nil {
		output := dynamodb.GetItemOutput{
			Item: make(map[string]types.AttributeValue),
		}
		return &output, nil
	}

	item := NewItemFromEntry(entry.Body)
	output := dynamodb.GetItemOutput{
		Item: item,
	}

	return &output, nil
}

func (t *Table) Query(context context.Context, input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	if input.KeyConditionExpression == nil {
		err := &ValidationException{
			Message: "Either the KeyConditions or KeyConditionExpression parameter must be specified in the request.",
		}
		return nil, err
	}

	keyConditionExpression, err := expression.ParseKeyConditionExpression(*input.KeyConditionExpression)
	if err != nil {
		err = &ValidationException{
			Message: "Invalid KeyConditionExpression: Syntax error;",
		}
		return nil, err
	}
	expressionAttributeValues := make(map[string]AttributeValue)
	for k, v := range input.ExpressionAttributeValues {
		expressionAttributeValues[k] = TransformDdbAttributeValue(v)
	}

	builder := QueryBuilder{
		KeyConditionExpression:    keyConditionExpression,
		ExpressionAttributeValues: expressionAttributeValues,
		ExpressionAttributeNames:  input.ExpressionAttributeNames,
		TableMetadata:             t.meta,
		ExclusiveStartKey:         input.ExclusiveStartKey,
		ConsistentRead:            input.ConsistentRead,
		Limit:                     input.Limit,
		IndexName:                 input.IndexName,
		ScanIndexForward:          input.ScanIndexForward,
	}

	query, err := builder.BuildQuery()
	if err != nil {
		return nil, err
	}

	res, err := t.inner.Query(query)
	if err != nil {
		return nil, err
	}
	entries := res.Entries
	items := make([]map[string]types.AttributeValue, len(entries))
	for i, entry := range entries {
		items[i] = NewItemFromEntry(entry.Body)
	}

	lastEvaluatedKey := make(map[string]types.AttributeValue)
	if len(entries) > 0 {
		lastEntry := entries[len(entries)-1]
		partitionKeyName := *t.meta.partitionKeySchema.AttributeName
		pk, ok := lastEntry.Body[partitionKeyName]
		if !ok {
			return nil, fmt.Errorf("can't found partition key in last entry")
		}
		lastEvaluatedKey[partitionKeyName] = pk.ToDdbAttributeValue()
		if t.meta.sortKeySchema != nil {
			sortKeyName := *t.meta.sortKeySchema.AttributeName
			sk, ok := lastEntry.Body[sortKeyName]
			if !ok {
				return nil, fmt.Errorf("can't found sort key in last entry")
			}
			lastEvaluatedKey[sortKeyName] = sk.ToDdbAttributeValue()
		}
	}

	output := &dynamodb.QueryOutput{
		Count:            int32(len(entries)),
		Items:            items,
		LastEvaluatedKey: lastEvaluatedKey,
		ScannedCount:     res.ScannedCount,
	}

	return output, nil
}

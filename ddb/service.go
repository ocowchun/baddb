package ddb

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ocowchun/baddb/expression"
	"sync"
	"time"
)

type Service struct {
	tableLock      sync.RWMutex
	tableMetadatas map[string]*TableMetaData
	storage        *InnerStorage
}

func NewDdbService() *Service {
	innerStorage := NewInnerStorage()
	return &Service{
		tableMetadatas: make(map[string]*TableMetaData),
		storage:        innerStorage,
	}
}

func (svc *Service) ListTables(ctx context.Context, input *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
	svc.tableLock.RLock()
	defer svc.tableLock.RUnlock()

	// TODO: implement paging
	tableNames := make([]string, 0)
	for tableName, _ := range svc.tableMetadatas {
		tableNames = append(tableNames, tableName)
	}
	output := &dynamodb.ListTablesOutput{
		TableNames: tableNames,
	}

	return output, nil
}

func (svc *Service) CreateTable(ctx context.Context, input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	svc.tableLock.Lock()
	defer svc.tableLock.Unlock()

	// TODO: add more check
	tableName := *input.TableName
	if _, ok := svc.tableMetadatas[tableName]; ok {
		msg := "Cannot create preexisting table"
		err := &types.ResourceInUseException{
			Message: &msg,
		}
		return nil, err
	}

	now := time.Now()
	var partitionKeySchema *KeySchema
	var sortKeySchema *KeySchema
	for _, keySchema := range input.KeySchema {
		if keySchema.KeyType == types.KeyTypeHash {
			partitionKeySchema = &KeySchema{
				AttributeName: *keySchema.AttributeName,
			}
		} else {
			sortKeySchema = &KeySchema{
				AttributeName: *keySchema.AttributeName,
			}
		}
	}
	if partitionKeySchema == nil {
		msg := "Partition key must be present"
		err := &ValidationException{
			Message: msg,
		}
		return nil, err
	}

	gsiSettings := make([]GlobalSecondaryIndexSetting, len(input.GlobalSecondaryIndexes))
	for i, gsi := range input.GlobalSecondaryIndexes {
		nonKeyAttributes := make([]string, len(gsi.Projection.NonKeyAttributes))
		for i, v := range gsi.Projection.NonKeyAttributes {
			nonKeyAttributes[i] = v
		}

		var partitionKeyName *string
		var sortKeyName *string
		for _, key := range gsi.KeySchema {
			if key.KeyType == types.KeyTypeHash {
				partitionKeyName = key.AttributeName
			} else if key.KeyType == types.KeyTypeRange {
				sortKeyName = key.AttributeName
			}
		}
		var projectionType ProjectionType
		switch gsi.Projection.ProjectionType {
		case types.ProjectionTypeKeysOnly:
			projectionType = PROJECTION_TYPE_KEYS_ONLY
		case types.ProjectionTypeInclude:
			projectionType = PROJECTION_TYPE_INCLUDE
		case types.ProjectionTypeAll:
			projectionType = PROJECTION_TYPE_ALL
		}

		gsiSettings[i] = GlobalSecondaryIndexSetting{
			IndexName:        gsi.IndexName,
			PartitionKeyName: partitionKeyName,
			SortKeyName:      sortKeyName,
			NonKeyAttributes: nonKeyAttributes,
			ProjectionType:   projectionType,
		}

	}

	meta := &TableMetaData{
		AttributeDefinitions:         input.AttributeDefinitions,
		GlobalSecondaryIndexSettings: gsiSettings,
		LocalSecondaryIndexes:        input.LocalSecondaryIndexes,
		ProvisionedThroughput:        input.ProvisionedThroughput,
		CreationDateTime:             &now,
		PartitionKeySchema:           partitionKeySchema,
		SortKeySchema:                sortKeySchema,
		Name:                         tableName,
	}
	//table := NewTable(&meta)
	err := svc.storage.CreateTable(meta)
	if err != nil {
		return nil, err
	}

	//svc.tables[tableName] = table
	svc.tableMetadatas[tableName] = meta

	output := dynamodb.CreateTableOutput{
		TableDescription: meta.Description(),
	}
	return &output, nil
}

func (svc *Service) BatchGetItem(ctx context.Context, input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error) {
	// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
	svc.tableLock.RLock()
	defer svc.tableLock.RUnlock()

	reqKeysCount := 0
	for _, r := range input.RequestItems {
		reqKeysCount += len(r.Keys)
	}

	if reqKeysCount >= 100 {
		msg := "Too many items requested for the BatchGetItem call"
		err := &ValidationException{
			Message: msg,
		}
		return nil, err
	}

	// purposely not handle some item to simulate unprocessed items
	missed := 0
	if reqKeysCount > 2 {
		missed = 2
	}

	responses := make(map[string][]map[string]types.AttributeValue)
	unprocessedKeys := make(map[string]types.KeysAndAttributes)

	for tableName, r := range input.RequestItems {
		_, ok := svc.tableMetadatas[tableName]
		if !ok {
			msg := "Cannot do operations on a non-existent table"
			err := &types.ResourceNotFoundException{
				Message: &msg,
			}
			return nil, err
		}

		for _, key := range r.Keys {
			if missed > 0 {
				unprocessedSummary, ok := unprocessedKeys[tableName]
				if !ok {
					unprocessedSummary = types.KeysAndAttributes{}
				}
				unprocessedSummary.Keys = append(unprocessedSummary.Keys, key)
				unprocessedKeys[tableName] = unprocessedSummary
				missed--
				continue
			}

			getItemInput := &dynamodb.GetItemInput{
				Key:                      key,
				TableName:                &tableName,
				AttributesToGet:          r.AttributesToGet,
				ConsistentRead:           r.ConsistentRead,
				ExpressionAttributeNames: r.ExpressionAttributeNames,
				ProjectionExpression:     r.ProjectionExpression,
			}
			item, err := svc.GetItem(ctx, getItemInput)
			if err != nil {
				return nil, err
			}

			if item.Item != nil {
				responseSummary, ok := responses[tableName]
				if !ok {
					responseSummary = make([]map[string]types.AttributeValue, 0)
				}
				responses[tableName] = append(responseSummary, item.Item)
			}
		}
	}

	output := &dynamodb.BatchGetItemOutput{
		Responses:       responses,
		UnprocessedKeys: unprocessedKeys,
	}

	return output, nil
}

func (svc *Service) BatchWriteItem(ctx context.Context, input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
	svc.tableLock.RLock()
	defer svc.tableLock.RUnlock()

	reqCount := 0
	for _, r := range input.RequestItems {
		reqCount += len(r)
	}

	if reqCount >= 25 {
		msg := "Too many items requested for the BatchWriteItem call"
		err := &ValidationException{
			Message: msg,
		}
		return nil, err
	}

	// purposely not handle some item to simulate unprocessed items
	missed := 0
	if reqCount > 2 {
		missed = 2
	}

	unprocessedItems := make(map[string][]types.WriteRequest)
	for tableName, requests := range input.RequestItems {
		_, ok := svc.tableMetadatas[tableName]
		if !ok {
			msg := "Cannot do operations on a non-existent table"
			err := &types.ResourceNotFoundException{
				Message: &msg,
			}
			return nil, err
		}

		for _, request := range requests {
			if missed > 0 {
				unprocessedSummary, ok := unprocessedItems[tableName]
				if !ok {
					unprocessedSummary = make([]types.WriteRequest, 0)
				}
				unprocessedItems[tableName] = append(unprocessedSummary, request)
				missed--
				continue
			}

			if request.PutRequest != nil {
				putItemInput := &dynamodb.PutItemInput{
					Item:      request.PutRequest.Item,
					TableName: &tableName,
				}
				_, err := svc.PutItem(ctx, putItemInput)
				if err != nil {
					return nil, err
				}
			} else if request.DeleteRequest != nil {
				deleteItemInput := &dynamodb.DeleteItemInput{
					Key:       request.DeleteRequest.Key,
					TableName: &tableName,
				}
				_, err := svc.DeleteItem(ctx, deleteItemInput)
				if err != nil {
					return nil, err
				}
			} else {
				msg := "Invalid request"
				err := &ValidationException{
					Message: msg,
				}
				return nil, err
			}

		}

	}

	output := &dynamodb.BatchWriteItemOutput{
		UnprocessedItems: unprocessedItems,
	}
	return output, nil
}

func (svc *Service) PutItem(ctx context.Context, input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	svc.tableLock.RLock()
	defer svc.tableLock.RUnlock()

	tableName := *input.TableName
	if _, ok := svc.tableMetadatas[tableName]; ok {
		entry := NewEntryFromItem(input.Item)

		var condition *Condition
		var err error
		if input.ConditionExpression != nil {
			condition, err = BuildCondition(
				*input.ConditionExpression,
				input.ExpressionAttributeNames,
				NewEntryFromItem(input.ExpressionAttributeValues).Body,
			)
			if err != nil {
				return nil, &ValidationException{
					Message: err.Error(),
				}
			}
		}

		req := &PutRequest{
			Entry:     entry,
			TableName: tableName,
			Condition: condition,
		}
		err = svc.storage.Put(req)
		if err != nil {
			return nil, err
		}
		//TODO: add PutItemOutput
		return nil, nil
	} else {
		msg := "Cannot do operations on a non-existent table"
		err := &types.ResourceNotFoundException{
			Message: &msg,
		}
		return nil, err
	}
}

func (svc *Service) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	svc.tableLock.RLock()
	defer svc.tableLock.RUnlock()

	tableName := *input.TableName
	if _, ok := svc.tableMetadatas[tableName]; ok {
		var condition *Condition
		var err error
		if input.ConditionExpression != nil {
			condition, err = BuildCondition(
				*input.ConditionExpression,
				input.ExpressionAttributeNames,
				NewEntryFromItem(input.ExpressionAttributeValues).Body,
			)
			if err != nil {
				return nil, &ValidationException{
					Message: err.Error(),
				}
			}
		}

		entry := NewEntryFromItem(input.Key)
		req := &DeleteRequest{
			Entry:     entry,
			TableName: tableName,
			Condition: condition,
		}

		err = svc.storage.Delete(req)
		if err != nil {
			return nil, err
		}
		output := &dynamodb.DeleteItemOutput{}

		return output, nil
	} else {
		fmt.Println("table not found")
		msg := "Cannot do operations on a non-existent table"
		err := &types.ResourceNotFoundException{
			Message: &msg,
		}
		return nil, err
	}
}

func (svc *Service) GetItem(ctx context.Context, input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	svc.tableLock.RLock()
	defer svc.tableLock.RUnlock()

	tableName := *input.TableName
	if _, ok := svc.tableMetadatas[tableName]; ok {

		consistentRead := false
		if input.ConsistentRead != nil {
			consistentRead = *input.ConsistentRead
		}

		req := &GetRequest{
			Entry:          NewEntryFromItem(input.Key),
			ConsistentRead: consistentRead,
			TableName:      tableName,
		}

		entry, err := svc.storage.Get(req)

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
	} else {
		msg := "Cannot do operations on a non-existent table"
		err := &types.ResourceNotFoundException{
			Message: &msg,
		}
		return nil, err
	}
}

type ValidationException struct {
	Message string
}

func (e *ValidationException) Error() string {
	return e.Message
}

func (svc *Service) Query(ctx context.Context, input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	svc.tableLock.RLock()
	defer svc.tableLock.RUnlock()

	tableName := *input.TableName
	tableMetadata, ok := svc.tableMetadatas[tableName]
	if !ok {
		msg := "Cannot do operations on a non-existent table"
		err := &types.ResourceNotFoundException{
			Message: &msg,
		}
		return nil, err
	}
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
		TableMetadata:             tableMetadata,
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
	query.TableName = tableName

	res, err := svc.storage.Query(query)
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
		partitionKeyName := tableMetadata.PartitionKeySchema.AttributeName
		pk, ok := lastEntry.Body[partitionKeyName]
		if !ok {
			return nil, fmt.Errorf("can't found partition key in last entry")
		}
		lastEvaluatedKey[partitionKeyName] = pk.ToDdbAttributeValue()
		if tableMetadata.SortKeySchema != nil {
			sortKeyName := tableMetadata.SortKeySchema.AttributeName
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

func (svc *Service) DeleteTable(ctx context.Context, input *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error) {
	svc.tableLock.Lock()
	defer svc.tableLock.Unlock()

	tableName := *input.TableName
	if _, ok := svc.tableMetadatas[tableName]; ok {
		table := svc.tableMetadatas[tableName]
		tableDescription := table.Description()
		delete(svc.tableMetadatas, tableName)

		// TODO: delete from storage
		output := &dynamodb.DeleteTableOutput{
			TableDescription: tableDescription,
		}

		return output, nil
	} else {
		msg := "Cannot do operations on a non-existent table"
		err := &types.ResourceNotFoundException{
			Message: &msg,
		}
		return nil, err
	}
}

func (svc *Service) DescribeTable(ctx context.Context, input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	svc.tableLock.RLock()
	defer svc.tableLock.RUnlock()

	tableName := *input.TableName
	if _, ok := svc.tableMetadatas[tableName]; ok {
		table := svc.tableMetadatas[tableName]
		tableDescription := table.Description()

		output := &dynamodb.DescribeTableOutput{
			Table: tableDescription,
		}

		return output, nil
	} else {
		msg := "Cannot do operations on a non-existent table"
		err := &types.ResourceNotFoundException{
			Message: &msg,
		}
		return nil, err
	}
}

func (svc *Service) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
	// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
	svc.tableLock.RLock()
	defer svc.tableLock.RUnlock()

	txn, err := svc.storage.BeginTxn(false)
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	for _, writeItem := range input.TransactItems {
		if writeItem.Put != nil {
			// TODO: handle condition

			put := writeItem.Put
			tableName := *put.TableName
			if _, ok := svc.tableMetadatas[tableName]; !ok {
				msg := "Cannot do operations on a non-existent table"
				err = &types.ResourceNotFoundException{
					Message: &msg,
				}
				return nil, err
			}

			var condition *Condition
			if put.ConditionExpression != nil {
				condition, err = BuildCondition(
					*put.ConditionExpression,
					put.ExpressionAttributeNames,
					NewEntryFromItem(put.ExpressionAttributeValues).Body,
				)
				if err != nil {
					return nil, &ValidationException{
						Message: err.Error(),
					}
				}
			}

			entry := NewEntryFromItem(put.Item)
			req := &PutRequest{
				Entry:     entry,
				TableName: tableName,
				Condition: condition,
			}
			err = svc.storage.PutWithTransaction(req, txn)
			if err != nil {
				return nil, err
			}
		}
		if writeItem.Delete != nil {
			deleteReq := writeItem.Delete
			tableName := *deleteReq.TableName
			if _, ok := svc.tableMetadatas[tableName]; !ok {
				msg := "Cannot do operations on a non-existent table"
				err = &types.ResourceNotFoundException{
					Message: &msg,
				}
				return nil, err
			}

			var condition *Condition
			if deleteReq.ConditionExpression != nil {
				condition, err = BuildCondition(
					*deleteReq.ConditionExpression,
					deleteReq.ExpressionAttributeNames,
					NewEntryFromItem(deleteReq.ExpressionAttributeValues).Body,
				)
				if err != nil {
					return nil, &ValidationException{
						Message: err.Error(),
					}
				}
			}

			entry := NewEntryFromItem(deleteReq.Key)
			req := &DeleteRequest{
				Entry:     entry,
				TableName: tableName,
				Condition: condition,
			}
			err = svc.storage.DeleteWithTransaction(req, txn)
			if err != nil {
				return nil, err
			}

		}

	}
	//table
	//svc.tableLock
	return nil, fmt.Errorf("unimplemented")

}

package ddb

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"sync"
	"time"
)

type Service struct {
	tables    map[string]*Table
	tableLock sync.RWMutex
}

func NewDdbService() *Service {
	return &Service{
		tables: make(map[string]*Table),
	}
}

func (svc *Service) ListTables(ctx context.Context, input *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
	svc.tableLock.RLock()
	defer svc.tableLock.RUnlock()

	// TODO: implement paging
	tableNames := make([]string, 0)
	for tableName, _ := range svc.tables {
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
	if _, ok := svc.tables[tableName]; ok {
		msg := "Cannot create preexisting table"
		err := &types.ResourceInUseException{
			Message: &msg,
		}
		return nil, err
	}

	now := time.Now()
	var partitionKeySchema *types.KeySchemaElement
	var sortKeySchema *types.KeySchemaElement
	for _, keySchema := range input.KeySchema {
		if keySchema.KeyType == types.KeyTypeHash {
			partitionKeySchema = &keySchema
		} else {
			sortKeySchema = &keySchema
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

	meta := TableMetaData{
		AttributeDefinitions:         input.AttributeDefinitions,
		GlobalSecondaryIndexSettings: gsiSettings,
		LocalSecondaryIndexes:        input.LocalSecondaryIndexes,
		ProvisionedThroughput:        input.ProvisionedThroughput,
		CreationDateTime:             &now,
		partitionKeySchema:           partitionKeySchema,
		sortKeySchema:                sortKeySchema,
		Name:                         tableName,
	}
	table := NewTable(&meta)
	svc.tables[tableName] = table

	output := dynamodb.CreateTableOutput{
		TableDescription: table.Description(),
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
		table, ok := svc.tables[tableName]
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
			item, err := table.Get(getItemInput)
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
		table, ok := svc.tables[tableName]
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
				_, err := table.Put(putItemInput)
				if err != nil {
					return nil, err
				}
			} else if request.DeleteRequest != nil {
				deleteItemInput := &dynamodb.DeleteItemInput{
					Key:       request.DeleteRequest.Key,
					TableName: &tableName,
				}
				_, err := table.Delete(deleteItemInput)
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
	if table, ok := svc.tables[tableName]; ok {
		fmt.Println("table found")
		return table.Put(input)
	} else {
		fmt.Println("table not found")
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
	if table, ok := svc.tables[tableName]; ok {
		fmt.Println("table found")
		return table.Delete(input)
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
	if table, ok := svc.tables[tableName]; ok {
		return table.Get(input)
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
	if table, ok := svc.tables[tableName]; ok {
		return table.Query(ctx, input)
	} else {
		msg := "Cannot do operations on a non-existent table"
		err := &types.ResourceNotFoundException{
			Message: &msg,
		}
		return nil, err
	}
}

func (svc *Service) DeleteTable(ctx context.Context, input *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error) {
	svc.tableLock.Lock()
	defer svc.tableLock.Unlock()

	tableName := *input.TableName
	if _, ok := svc.tables[tableName]; ok {
		table := svc.tables[tableName]
		tableDescription := table.Description()
		delete(svc.tables, tableName)

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
	if _, ok := svc.tables[tableName]; ok {
		table := svc.tables[tableName]
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

//func (svc *Service) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
//	// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
//	svc.tableLock.RLock()
//	defer svc.tableLock.RUnlock()
//
//	for _, writeItem := range input.TransactItems {
//		//writeItem.
//		// maybe
//
//	}
//	//table
//	//svc.tableLock
//
//}

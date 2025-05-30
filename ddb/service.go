package ddb

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	condition2 "github.com/ocowchun/baddb/ddb/condition"
	"github.com/ocowchun/baddb/ddb/core"
	"github.com/ocowchun/baddb/ddb/inner_storage"
	query2 "github.com/ocowchun/baddb/ddb/query"
	"github.com/ocowchun/baddb/ddb/update"
	"github.com/ocowchun/baddb/expression"
	"sync"
	"time"
)

type Service struct {
	tableLock      sync.RWMutex
	tableMetadatas map[string]*core.TableMetaData
	storage        *inner_storage.InnerStorage
}

func NewDdbService() *Service {
	innerStorage := inner_storage.NewInnerStorage()
	return &Service{
		tableMetadatas: make(map[string]*core.TableMetaData),
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
	var partitionKeySchema *core.KeySchema
	var sortKeySchema *core.KeySchema
	for _, keySchema := range input.KeySchema {
		if keySchema.KeyType == types.KeyTypeHash {
			partitionKeySchema = &core.KeySchema{
				AttributeName: *keySchema.AttributeName,
			}
		} else {
			sortKeySchema = &core.KeySchema{
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

	gsiSettings := make([]core.GlobalSecondaryIndexSetting, len(input.GlobalSecondaryIndexes))
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
		var projectionType core.ProjectionType
		switch gsi.Projection.ProjectionType {
		case types.ProjectionTypeKeysOnly:
			projectionType = core.PROJECTION_TYPE_KEYS_ONLY
		case types.ProjectionTypeInclude:
			projectionType = core.PROJECTION_TYPE_INCLUDE
		case types.ProjectionTypeAll:
			projectionType = core.PROJECTION_TYPE_ALL
		}

		gsiSettings[i] = core.GlobalSecondaryIndexSetting{
			IndexName:        gsi.IndexName,
			PartitionKeyName: partitionKeyName,
			SortKeyName:      sortKeyName,
			NonKeyAttributes: nonKeyAttributes,
			ProjectionType:   projectionType,
		}
	}
	// api error ValidationException:
	billingMode := core.BILLING_MODE_PAY_PER_REQUEST
	if input.BillingMode == types.BillingModeProvisioned {
		billingMode = core.BILLING_MODE_PROVISIONED
		if input.ProvisionedThroughput == nil {
			msg := "No provisioned throughput specified for the table"
			err := &ValidationException{
				Message: msg,
			}
			return nil, err
		}
		if input.ProvisionedThroughput.ReadCapacityUnits == nil || input.ProvisionedThroughput.WriteCapacityUnits == nil {
			msg := "readCapacityUnits and writeCapacityUnits must be specified"
			err := &ValidationException{
				Message: msg,
			}
			return nil, err
		}
	}

	meta := &core.TableMetaData{
		AttributeDefinitions:         input.AttributeDefinitions,
		GlobalSecondaryIndexSettings: gsiSettings,
		LocalSecondaryIndexes:        input.LocalSecondaryIndexes,
		ProvisionedThroughput:        input.ProvisionedThroughput,
		CreationDateTime:             &now,
		PartitionKeySchema:           partitionKeySchema,
		SortKeySchema:                sortKeySchema,
		Name:                         tableName,
		BillingMode:                  billingMode,
	}
	err := svc.storage.CreateTable(meta)
	if err != nil {
		return nil, err
	}

	svc.tableMetadatas[tableName] = meta

	itemCount, err := svc.storage.QueryItemCount(tableName)
	if err != nil {
		return nil, err
	}

	output := dynamodb.CreateTableOutput{
		TableDescription: meta.Description(itemCount),
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
		entry := core.NewEntryFromItem(input.Item)

		var condition *condition2.Condition
		var err error
		if input.ConditionExpression != nil {
			condition, err = condition2.BuildCondition(
				*input.ConditionExpression,
				input.ExpressionAttributeNames,
				core.NewEntryFromItem(input.ExpressionAttributeValues).Body,
			)
			if err != nil {
				return nil, &ValidationException{
					Message: err.Error(),
				}
			}
		}

		req := &inner_storage.PutRequest{
			Entry:     entry,
			TableName: tableName,
			Condition: condition,
		}
		err = svc.storage.Put(req)
		if err != nil {
			if errors.Is(err, inner_storage.RateLimitReachedError) {
				return nil, ProvisionedThroughputExceededException

			}
			return nil, err
		}

		output := &dynamodb.PutItemOutput{}
		//TODO: add PutItemOutput
		return output, nil
	} else {
		msg := "Cannot do operations on a non-existent table"
		err := &types.ResourceNotFoundException{
			Message: &msg,
		}
		return nil, err
	}
}

func (svc *Service) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	svc.tableLock.RLock()
	defer svc.tableLock.RUnlock()

	tableName := *input.TableName
	if _, ok := svc.tableMetadatas[tableName]; ok {
		if input.UpdateExpression == nil {
			msg := "UpdateExpression must be provided"
			err := &ValidationException{
				Message: msg,
			}
			return nil, err
		}

		updateOperation, err := update.BuildUpdateOperation(
			*input.UpdateExpression,
			input.ExpressionAttributeNames,
			core.NewEntryFromItem(input.ExpressionAttributeValues).Body)
		if err != nil {
			return nil, &ValidationException{
				Message: err.Error(),
			}
		}

		var condition *condition2.Condition
		if input.ConditionExpression != nil {
			condition, err = condition2.BuildCondition(
				*input.ConditionExpression,
				input.ExpressionAttributeNames,
				core.NewEntryFromItem(input.ExpressionAttributeValues).Body,
			)
			if err != nil {
				return nil, &ValidationException{
					Message: err.Error(),
				}
			}
		}

		req := &inner_storage.UpdateRequest{
			Key:             core.NewEntryFromItem(input.Key),
			UpdateOperation: updateOperation,
			TableName:       tableName,
			Condition:       condition,
		}
		res, err := svc.storage.Update(req)
		if err != nil {
			return nil, err
		}

		// TODO: consider ReturnValues
		output := &dynamodb.UpdateItemOutput{
			Attributes: core.NewItemFromEntry(res.NewEntry.Body),
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

func (svc *Service) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	svc.tableLock.RLock()
	defer svc.tableLock.RUnlock()

	tableName := *input.TableName
	if _, ok := svc.tableMetadatas[tableName]; ok {
		var condition *condition2.Condition
		var err error
		if input.ConditionExpression != nil {
			condition, err = condition2.BuildCondition(
				*input.ConditionExpression,
				input.ExpressionAttributeNames,
				core.NewEntryFromItem(input.ExpressionAttributeValues).Body,
			)
			if err != nil {
				return nil, &ValidationException{
					Message: err.Error(),
				}
			}
		}

		entry := core.NewEntryFromItem(input.Key)
		req := &inner_storage.DeleteRequest{
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

		req := &inner_storage.GetRequest{
			Entry:          core.NewEntryFromItem(input.Key),
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

		item := core.NewItemFromEntry(entry.Body)
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

var (
	provisionedThroughputExceededExceptionMessage = "The level of configured provisioned throughput for the table was exceeded. Consider increasing your provisioning level with the UpdateTable API."
	ProvisionedThroughputExceededException        = &types.ProvisionedThroughputExceededException{
		Message: &provisionedThroughputExceededExceptionMessage,
	}
)

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
	expressionAttributeValues := make(map[string]core.AttributeValue)
	for k, v := range input.ExpressionAttributeValues {
		expressionAttributeValues[k] = core.TransformDdbAttributeValue(v)
	}

	builder := query2.QueryBuilder{
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
		items[i] = core.NewItemFromEntry(entry.Body)
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

		itemCount, err := svc.storage.QueryItemCount(tableName)
		if err != nil {
			return nil, err
		}
		tableDescription := table.Description(itemCount)
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
		itemCount, err := svc.storage.QueryItemCount(tableName)
		if err != nil {
			return nil, err
		}
		tableDescription := table.Description(itemCount)

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

const (
	MAX_ACTION_REQUEST = 100
)

func (svc *Service) validateTransactWriteItemsInput(input *dynamodb.TransactWriteItemsInput) error {
	if len(input.TransactItems) > MAX_ACTION_REQUEST {
		return &ValidationException{
			Message: fmt.Sprintf("Member must have length less than or equal to %d", MAX_ACTION_REQUEST),
		}
	}

	primaryKeys := make(map[string]map[string]bool)
	for _, writeItem := range input.TransactItems {
		var pk *inner_storage.PrimaryKey
		var tableName string
		var err error
		if writeItem.ConditionCheck != nil {
			conditionCheck := writeItem.ConditionCheck

			tableName = *conditionCheck.TableName
			tableMetadata, ok := svc.tableMetadatas[tableName]
			if !ok {
				msg := "Cannot do operations on a non-existent table"
				return &types.ResourceNotFoundException{
					Message: &msg,
				}
			}
			pk, err = svc.buildTablePrimaryKey(core.NewEntryFromItem(conditionCheck.Key), tableMetadata)
			if err != nil {
				return err
			}
		} else if writeItem.Put != nil {
			put := writeItem.Put

			tableName = *put.TableName
			tableMetadata, ok := svc.tableMetadatas[tableName]
			if !ok {
				msg := "Cannot do operations on a non-existent table"
				return &types.ResourceNotFoundException{
					Message: &msg,
				}
			}
			pk, err = svc.buildTablePrimaryKey(core.NewEntryFromItem(put.Item), tableMetadata)
			if err != nil {
				return err
			}
		} else if writeItem.Delete != nil {
			deleteReq := writeItem.Delete

			tableName = *deleteReq.TableName
			tableMetadata, ok := svc.tableMetadatas[tableName]
			if !ok {
				msg := "Cannot do operations on a non-existent table"
				return &types.ResourceNotFoundException{
					Message: &msg,
				}
			}
			pk, err = svc.buildTablePrimaryKey(core.NewEntryFromItem(deleteReq.Key), tableMetadata)
			if err != nil {
				return err
			}
		} else if writeItem.Update != nil {
			update := writeItem.Update

			tableName = *update.TableName
			tableMetadata, ok := svc.tableMetadatas[tableName]
			if !ok {
				msg := "Cannot do operations on a non-existent table"
				return &types.ResourceNotFoundException{
					Message: &msg,
				}
			}
			pk, err = svc.buildTablePrimaryKey(core.NewEntryFromItem(update.Key), tableMetadata)
			if err != nil {
				return err
			}

		}

		if _, ok := primaryKeys[tableName]; !ok {
			primaryKeys[tableName] = make(map[string]bool)
		}
		if _, ok := primaryKeys[tableName][pk.String()]; ok {
			msg := "Transaction request cannot include multiple operations on one item"
			return &ValidationException{
				Message: msg,
			}
		}
		primaryKeys[tableName][pk.String()] = true

	}

	return nil
}

// TODO: refactor it
func (svc *Service) buildTablePrimaryKey(entry *core.Entry, table *core.TableMetaData) (*inner_storage.PrimaryKey, error) {
	primaryKey := &inner_storage.PrimaryKey{
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

func (svc *Service) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
	// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
	svc.tableLock.RLock()
	defer svc.tableLock.RUnlock()

	err := svc.validateTransactWriteItemsInput(input)
	if err != nil {
		return nil, err
	}

	txn, err := svc.storage.BeginTxn(false)
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	for _, writeItem := range input.TransactItems {
		if writeItem.ConditionCheck != nil {
			conditionCheck := writeItem.ConditionCheck
			tableName := *conditionCheck.TableName
			if _, ok := svc.tableMetadatas[tableName]; !ok {
				msg := "Cannot do operations on a non-existent table"
				err = &types.ResourceNotFoundException{
					Message: &msg,
				}
				return nil, err
			}

			var condition *condition2.Condition
			if conditionCheck.ConditionExpression == nil || *conditionCheck.ConditionExpression == "" {
				return nil, &ValidationException{
					Message: "The expression can not be empty;",
				}
			}

			condition, err = condition2.BuildCondition(
				*conditionCheck.ConditionExpression,
				conditionCheck.ExpressionAttributeNames,
				core.NewEntryFromItem(conditionCheck.ExpressionAttributeValues).Body,
			)
			if err != nil {
				return nil, &ValidationException{
					Message: err.Error(),
				}
			}

			key := core.NewEntryFromItem(conditionCheck.Key)
			req := &inner_storage.GetRequest{
				Entry:          key,
				TableName:      tableName,
				ConsistentRead: true,
			}
			entry, err := svc.storage.GetWithTransaction(req, txn)
			if err != nil {
				return nil, err
			}

			if entry == nil {
				entry = &core.Entry{
					Body: make(map[string]core.AttributeValue),
				}
			}
			matched, err := condition.Check(entry)

			if err != nil {
				return nil, err
			} else if matched {
				continue
			} else {
				msg := "The conditional request failed"
				err = &types.ConditionalCheckFailedException{
					Message: &msg,
				}
				return nil, err
			}

		} else if writeItem.Put != nil {
			put := writeItem.Put
			tableName := *put.TableName
			if _, ok := svc.tableMetadatas[tableName]; !ok {
				msg := "Cannot do operations on a non-existent table"
				err = &types.ResourceNotFoundException{
					Message: &msg,
				}
				return nil, err
			}

			var condition *condition2.Condition
			if put.ConditionExpression != nil {
				condition, err = condition2.BuildCondition(
					*put.ConditionExpression,
					put.ExpressionAttributeNames,
					core.NewEntryFromItem(put.ExpressionAttributeValues).Body,
				)
				if err != nil {
					return nil, &ValidationException{
						Message: err.Error(),
					}
				}
			}

			entry := core.NewEntryFromItem(put.Item)
			req := &inner_storage.PutRequest{
				Entry:     entry,
				TableName: tableName,
				Condition: condition,
			}
			err = svc.storage.PutWithTransaction(req, txn)
			if err != nil {
				return nil, err
			}
		} else if writeItem.Delete != nil {
			deleteReq := writeItem.Delete
			tableName := *deleteReq.TableName
			if _, ok := svc.tableMetadatas[tableName]; !ok {
				msg := "Cannot do operations on a non-existent table"
				err = &types.ResourceNotFoundException{
					Message: &msg,
				}
				return nil, err
			}

			var condition *condition2.Condition
			if deleteReq.ConditionExpression != nil {
				condition, err = condition2.BuildCondition(
					*deleteReq.ConditionExpression,
					deleteReq.ExpressionAttributeNames,
					core.NewEntryFromItem(deleteReq.ExpressionAttributeValues).Body,
				)
				if err != nil {
					return nil, &ValidationException{
						Message: err.Error(),
					}
				}
			}

			entry := core.NewEntryFromItem(deleteReq.Key)
			req := &inner_storage.DeleteRequest{
				Entry:     entry,
				TableName: tableName,
				Condition: condition,
			}
			err = svc.storage.DeleteWithTransaction(req, txn)
			if err != nil {
				return nil, err
			}
		} else if writeItem.Update != nil {
			updateReq := writeItem.Update
			tableName := *updateReq.TableName
			if _, ok := svc.tableMetadatas[tableName]; !ok {
				msg := "Cannot do operations on a non-existent table"
				err = &types.ResourceNotFoundException{
					Message: &msg,
				}
				return nil, err
			}

			if updateReq.UpdateExpression == nil {
				msg := "UpdateExpression must be provided"
				err := &ValidationException{
					Message: msg,
				}
				return nil, err
			}

			updateOperation, err := update.BuildUpdateOperation(
				*updateReq.UpdateExpression,
				updateReq.ExpressionAttributeNames,
				core.NewEntryFromItem(updateReq.ExpressionAttributeValues).Body)
			if err != nil {
				return nil, &ValidationException{
					Message: err.Error(),
				}
			}

			var condition *condition2.Condition
			if updateReq.ConditionExpression != nil {
				condition, err = condition2.BuildCondition(
					*updateReq.ConditionExpression,
					updateReq.ExpressionAttributeNames,
					core.NewEntryFromItem(updateReq.ExpressionAttributeValues).Body,
				)
				if err != nil {
					return nil, &ValidationException{
						Message: err.Error(),
					}
				}
			}

			req := &inner_storage.UpdateRequest{
				Key:             core.NewEntryFromItem(updateReq.Key),
				UpdateOperation: updateOperation,
				TableName:       tableName,
				Condition:       condition,
			}

			_, err = svc.storage.UpdateWithTransaction(req, txn)
			if err != nil {
				return nil, err
			}
		}

	}
	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	output := &dynamodb.TransactWriteItemsOutput{}

	return output, nil
}

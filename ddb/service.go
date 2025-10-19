package ddb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ocowchun/baddb/ddb/condition"
	"github.com/ocowchun/baddb/ddb/core"
	"github.com/ocowchun/baddb/ddb/expression"
	"github.com/ocowchun/baddb/ddb/query"
	"github.com/ocowchun/baddb/ddb/request"
	"github.com/ocowchun/baddb/ddb/scan"
	"github.com/ocowchun/baddb/ddb/storage"
)

type Service struct {
	tableLock          sync.RWMutex
	tableMetadataStore map[string]*core.TableMetaData
	storage            *storage.InnerStorage
}

func NewDdbService() *Service {
	innerStorage := storage.NewInnerStorage()
	tableMetadatas := make(map[string]*core.TableMetaData)
	tableMetadatas[storage.METADATA_TABLE_NAME] = &core.TableMetaData{}

	return &Service{
		tableMetadataStore: tableMetadatas,
		storage:            innerStorage,
	}
}

func (svc *Service) ListTables(ctx context.Context, input *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
	svc.tableLock.RLock()
	defer svc.tableLock.RUnlock()

	// TODO: implement paging
	tableNames := make([]string, 0)
	for tableName, _ := range svc.tableMetadataStore {
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
	if _, ok := svc.tableMetadataStore[tableName]; ok {
		msg := "Cannot create preexisting table"
		err := &types.ResourceInUseException{
			Message: &msg,
		}
		return nil, err
	}

	if err := core.ValidateTableName(tableName); err != nil {
		return nil, &ValidationException{
			Message: err.Error(),
		}
	}

	now := time.Now()
	var partitionKeySchema *core.KeySchema
	var sortKeySchema *core.KeySchema

	var attributeDefinitionMap = make(map[string]types.AttributeDefinition)
	for _, attrDef := range input.AttributeDefinitions {
		attributeDefinitionMap[*attrDef.AttributeName] = attrDef
	}

	for _, keySchema := range input.KeySchema {
		def, ok := attributeDefinitionMap[*keySchema.AttributeName]
		if !ok {
			msg := fmt.Sprintf("%s not found in attribute definitions", *keySchema.AttributeName)
			err := &ValidationException{
				Message: msg,
			}
			return nil, err
		}
		attrType, err := core.GetScalarAttributeType(def)
		if err != nil {
			err := &ValidationException{
				Message: err.Error(),
			}
			return nil, err
		}

		if keySchema.KeyType == types.KeyTypeHash {
			partitionKeySchema = &core.KeySchema{
				AttributeName: *keySchema.AttributeName,
				AttributeType: attrType,
			}
		} else if keySchema.KeyType == types.KeyTypeRange {
			sortKeySchema = &core.KeySchema{
				AttributeName: *keySchema.AttributeName,
				AttributeType: attrType,
			}
		} else {
			msg := fmt.Sprintf("Unknown key type %s for attribute %s", keySchema.KeyType, *keySchema.AttributeName)
			err := &ValidationException{
				Message: msg,
			}
			return nil, err
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
		if err := core.ValidateTableName(*gsi.IndexName); err != nil {
			return nil, &ValidationException{
				Message: err.Error(),
			}
		}

		var partitionKey *core.KeySchema
		var sortKey *core.KeySchema
		for _, key := range gsi.KeySchema {
			def, ok := attributeDefinitionMap[*key.AttributeName]
			if !ok {
				msg := fmt.Sprintf("%s not found in attribute definitions", *key.AttributeName)
				err := &ValidationException{
					Message: msg,
				}
				return nil, err
			}
			attrType, err := core.GetScalarAttributeType(def)
			if err != nil {
				err := &ValidationException{
					Message: err.Error(),
				}
				return nil, err
			}

			if key.KeyType == types.KeyTypeHash {
				partitionKey = &core.KeySchema{
					AttributeName: *key.AttributeName,
					AttributeType: attrType,
				}
			} else if key.KeyType == types.KeyTypeRange {
				sortKey = &core.KeySchema{
					AttributeName: *key.AttributeName,
					AttributeType: attrType,
				}
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
			IndexName:          gsi.IndexName,
			PartitionKeySchema: partitionKey,
			SortKeySchema:      sortKey,
			NonKeyAttributes:   nonKeyAttributes,
			ProjectionType:     projectionType,
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

	provisionedThroughput, err := core.BuildProvisionedThroughput(input.ProvisionedThroughput)
	if err != nil {
		return nil, &ValidationException{Message: err.Error()}
	}

	meta := &core.TableMetaData{
		AttributeDefinitions:         input.AttributeDefinitions,
		GlobalSecondaryIndexSettings: gsiSettings,
		LocalSecondaryIndexes:        input.LocalSecondaryIndexes,
		ProvisionedThroughput:        provisionedThroughput,
		CreationDateTime:             &now,
		PartitionKeySchema:           partitionKeySchema,
		SortKeySchema:                sortKeySchema,
		Name:                         tableName,
		BillingMode:                  billingMode,
	}
	err = svc.storage.CreateTable(meta)
	if err != nil {
		return nil, err
	}

	svc.tableMetadataStore[tableName] = meta

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

	if reqKeysCount > 100 {
		msg := "Too many items requested for the BatchGetItem call"
		err := &ValidationException{
			Message: msg,
		}
		return nil, err
	}

	responses := make(map[string][]map[string]types.AttributeValue)
	unprocessedKeys := make(map[string]types.KeysAndAttributes)

	for tableName, r := range input.RequestItems {
		_, ok := svc.tableMetadataStore[tableName]
		if !ok {
			msg := "Cannot do operations on a non-existent table"
			err := &types.ResourceNotFoundException{
				Message: &msg,
			}
			return nil, err
		}

		for _, key := range r.Keys {
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
				if errors.Is(err, storage.ErrUnprocessed) {
					unprocessedSummary, ok := unprocessedKeys[tableName]
					if !ok {
						unprocessedSummary = types.KeysAndAttributes{}
					}
					unprocessedSummary.Keys = append(unprocessedSummary.Keys, key)
					unprocessedKeys[tableName] = unprocessedSummary
					continue
				}

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

	//if len(input.RequestItems) == 0 {
	//	msg := "The batch write request list for a table cannot be null or empty"
	//	err := &ValidationException{
	//		Message: msg,
	//	}
	//	return nil, err
	//}
	for tableName, requests := range input.RequestItems {
		if len(requests) == 0 {
			msg := fmt.Sprintf("The batch write request list for a table cannot be null or empty: %s", tableName)
			err := &ValidationException{
				Message: msg,
			}
			return nil, err
		}
	}

	reqCount := 0
	for _, r := range input.RequestItems {
		reqCount += len(r)
	}

	if reqCount > 25 {
		msg := "Too many items requested for the BatchWriteItem call"
		err := &ValidationException{
			Message: msg,
		}
		return nil, err
	}

	unprocessedItems := make(map[string][]types.WriteRequest)
	for tableName, requests := range input.RequestItems {
		_, ok := svc.tableMetadataStore[tableName]
		if !ok {
			msg := "Cannot do operations on a non-existent table"
			err := &types.ResourceNotFoundException{
				Message: &msg,
			}
			return nil, err
		}

		for _, request := range requests {
			var err error
			if request.PutRequest != nil {
				putItemInput := &dynamodb.PutItemInput{
					Item:      request.PutRequest.Item,
					TableName: &tableName,
				}
				_, err = svc.PutItem(ctx, putItemInput)
			} else if request.DeleteRequest != nil {
				deleteItemInput := &dynamodb.DeleteItemInput{
					Key:       request.DeleteRequest.Key,
					TableName: &tableName,
				}
				_, err = svc.DeleteItem(ctx, deleteItemInput)
			} else {
				msg := "Invalid request"
				err = &ValidationException{
					Message: msg,
				}
			}

			if err != nil {
				if errors.Is(err, storage.ErrUnprocessed) {
					unprocessedSummary, ok := unprocessedItems[tableName]
					if !ok {
						unprocessedSummary = make([]types.WriteRequest, 0)
					}
					unprocessedItems[tableName] = append(unprocessedSummary, request)
					continue
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
	if _, ok := svc.tableMetadataStore[tableName]; ok {
		builder := &request.PutRequestBuilder{
			ConditionExpression:       input.ConditionExpression,
			ExpressionAttributeNames:  input.ExpressionAttributeNames,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
			Item:                      input.Item,
			TableName:                 input.TableName,
		}
		req, err := builder.Build()
		if err != nil {
			return nil, &ValidationException{
				Message: err.Error(),
			}
		}
		err = svc.storage.Put(req)
		if err != nil {
			return nil, wrapError(err)
		}

		//TODO: configure PutItemOutput
		output := &dynamodb.PutItemOutput{}
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
	if _, ok := svc.tableMetadataStore[tableName]; ok {
		builder := &request.UpdateRequestBuilder{
			TableName:                 input.TableName,
			UpdateExpression:          input.UpdateExpression,
			ExpressionAttributeNames:  input.ExpressionAttributeNames,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
			ConditionExpression:       input.ConditionExpression,
			Key:                       input.Key,
		}
		req, err := builder.Build()
		if err != nil {
			return nil, &ValidationException{
				Message: err.Error(),
			}
		}

		res, err := svc.storage.Update(req)
		if err != nil {
			return nil, wrapError(err)
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
	if _, ok := svc.tableMetadataStore[tableName]; ok {
		builder := &request.DeleteRequestBuilder{
			TableName:                 input.TableName,
			ConditionExpression:       input.ConditionExpression,
			ExpressionAttributeNames:  input.ExpressionAttributeNames,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
			Key:                       input.Key,
		}
		req, err := builder.Build()
		if err != nil {
			return nil, &ValidationException{
				Message: err.Error(),
			}
		}

		err = svc.storage.Delete(req)
		if err != nil {
			return nil, wrapError(err)
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
	if _, ok := svc.tableMetadataStore[tableName]; ok {
		builder := request.GetRequestBuilder{
			Input:         input,
			TableMetaData: svc.tableMetadataStore[tableName],
		}
		req, err := builder.Build()
		if err != nil {
			return nil, &ValidationException{
				Message: err.Error(),
			}
		}

		entry, err := svc.storage.Get(req)

		if err != nil {
			return nil, wrapError(err)
		}
		if entry == nil {
			output := dynamodb.GetItemOutput{
				Item: nil,
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
	tableMetadata, ok := svc.tableMetadataStore[tableName]
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
			Message: err.Error(),
		}
		return nil, err
	}

	expressionAttributeValues, err := core.TransformAttributeValueMap(input.ExpressionAttributeValues)
	if err != nil {
		message := fmt.Sprintf("ExpressionAttributeValues contains invalid value: %s", err.Error())
		return nil, &ValidationException{Message: message}
	}

	builder := query.QueryBuilder{
		KeyConditionExpression:    keyConditionExpression,
		ExpressionAttributeValues: expressionAttributeValues,
		ExpressionAttributeNames:  input.ExpressionAttributeNames,
		FilterExpressionStr:       input.FilterExpression,
		TableMetadata:             tableMetadata,
		ExclusiveStartKey:         input.ExclusiveStartKey,
		ConsistentRead:            input.ConsistentRead,
		Limit:                     input.Limit,
		IndexName:                 input.IndexName,
		ScanIndexForward:          input.ScanIndexForward,
	}

	queryReq, err := builder.BuildQuery()
	if err != nil {
		err = &ValidationException{
			Message: err.Error(),
		}
		return nil, err
	}
	queryReq.TableName = tableName

	res, err := svc.storage.Query(queryReq)
	if err != nil {
		return nil, wrapError(err)
	}
	entries := res.Entries
	items := make([]map[string]types.AttributeValue, len(entries))
	for i, entry := range entries {
		items[i] = core.NewItemFromEntry(entry.Body)
	}

	lastEvaluatedKey := make(map[string]types.AttributeValue)
	if len(entries) > 0 {
		// include hashKey, rangeKey, and GSI keys(if query is GSI)
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

		if input.IndexName != nil {
			gsiSetting, ok := tableMetadata.GetGlobalSecondaryIndexSetting(*input.IndexName)
			if !ok {
				return nil, fmt.Errorf("GSI %s not found in table %s", *input.IndexName, tableName)
			}
			gsiPkName := gsiSetting.PartitionKeySchema.AttributeName
			if _, ok := lastEvaluatedKey[gsiPkName]; !ok {
				gsiPk, ok := lastEntry.Body[gsiPkName]
				if !ok {
					return nil, fmt.Errorf("can't found GSI partition key in last entry")
				}
				lastEvaluatedKey[gsiPkName] = gsiPk.ToDdbAttributeValue()
			}
			if gsiSetting.SortKeySchema != nil {
				gsiSkName := gsiSetting.SortKeySchema.AttributeName
				if _, ok := lastEvaluatedKey[gsiSkName]; !ok {
					gsiSk, ok := lastEntry.Body[gsiSkName]
					if !ok {
						return nil, fmt.Errorf("can't found GSI sort key in last entry")
					}
					lastEvaluatedKey[gsiSkName] = gsiSk.ToDdbAttributeValue()
				}
			}
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

func (svc *Service) UpdateTable(ctx context.Context, input *dynamodb.UpdateTableInput) (*dynamodb.UpdateTableOutput, error) {
	svc.tableLock.Lock()
	defer svc.tableLock.Unlock()

	tableName := *input.TableName
	table, ok := svc.tableMetadataStore[tableName]
	if !ok {
		msg := "Cannot do operations on a non-existent table"
		err := &types.ResourceNotFoundException{
			Message: &msg,
		}
		return nil, err
	}

	originalTable := table.Clone()

	if input.BillingMode != "" {
		switch input.BillingMode {
		case types.BillingModeProvisioned:
			table.BillingMode = core.BILLING_MODE_PROVISIONED
			if input.ProvisionedThroughput == nil && table.ProvisionedThroughput == nil {
				svc.tableMetadataStore[tableName] = originalTable
				msg := "ProvisionedThroughput must be specified when BillingMode is PROVISIONED"
				err := &ValidationException{
					Message: msg,
				}
				return nil, err
			}
		case types.BillingModePayPerRequest:
			table.BillingMode = core.BILLING_MODE_PAY_PER_REQUEST
			if input.ProvisionedThroughput != nil {
				svc.tableMetadataStore[tableName] = originalTable
				msg := "Cannot specify ProvisionedThroughput when BillingMode is PAY_PER_REQUEST"
				err := &ValidationException{
					Message: msg,
				}
				return nil, err
			}
			table.ProvisionedThroughput = nil
		default:
			svc.tableMetadataStore[tableName] = originalTable
			msg := "Invalid billing mode"
			err := &ValidationException{
				Message: msg,
			}
			return nil, err
		}
	}

	if input.ProvisionedThroughput != nil {
		if table.BillingMode == core.BILLING_MODE_PAY_PER_REQUEST {
			svc.tableMetadataStore[tableName] = originalTable
			msg := "Cannot specify ProvisionedThroughput when table BillingMode is PAY_PER_REQUEST"
			err := &ValidationException{
				Message: msg,
			}
			return nil, err
		}

		provisionedThroughput, err := core.BuildProvisionedThroughput(input.ProvisionedThroughput)
		if err != nil {
			return nil, &ValidationException{Message: err.Error()}
		}

		table.ProvisionedThroughput = provisionedThroughput
	}

	if len(input.GlobalSecondaryIndexUpdates) > 0 {
		err := svc.processGSIUpdates(table, input.GlobalSecondaryIndexUpdates)
		if err != nil {
			svc.tableMetadataStore[tableName] = originalTable
			return nil, err
		}
	}

	err := svc.updateInnerStorage(tableName, input)
	if err != nil {
		svc.tableMetadataStore[tableName] = originalTable
		return nil, err
	}

	itemCount, err := svc.storage.QueryItemCount(tableName)
	if err != nil {
		svc.tableMetadataStore[tableName] = originalTable
		return nil, err
	}

	tableDescription := table.Description(itemCount)
	output := &dynamodb.UpdateTableOutput{
		TableDescription: tableDescription,
	}

	return output, nil
}

func (svc *Service) updateInnerStorage(tableName string, input *dynamodb.UpdateTableInput) error {
	updates := input.GlobalSecondaryIndexUpdates
	storageOperations := make([]storage.GSIOperation, 0, len(updates))

	for _, update := range updates {
		if update.Create != nil {
			op := storage.GSIOperation{
				Type:         "CREATE",
				GSIName:      *update.Create.IndexName,
				CreateAction: svc.convertToStorageCreateAction(update.Create),
			}
			storageOperations = append(storageOperations, op)
		}
		if update.Update != nil {
			var provisionedThroughput *core.ProvisionedThroughput
			if update.Update.ProvisionedThroughput != nil {
				pt, err := core.BuildProvisionedThroughput(update.Update.ProvisionedThroughput)
				if err != nil {
					return err
				}
				provisionedThroughput = pt
			}

			op := storage.GSIOperation{
				Type:    "UPDATE",
				GSIName: *update.Update.IndexName,
				UpdateAction: &storage.UpdateGSIAction{
					ProvisionedThroughput: provisionedThroughput,
				},
			}
			storageOperations = append(storageOperations, op)
		}
		if update.Delete != nil {
			op := storage.GSIOperation{
				Type:         "DELETE",
				GSIName:      *update.Delete.IndexName,
				DeleteAction: &storage.DeleteGSIAction{},
			}
			storageOperations = append(storageOperations, op)
		}
	}

	newBillingMode := core.BILLING_MODE_PAY_PER_REQUEST
	if input.BillingMode == types.BillingModeProvisioned {
		newBillingMode = core.BILLING_MODE_PROVISIONED
	}
	readCapacity := 0
	writeCapacity := 0
	if input.ProvisionedThroughput != nil {
		readCapacity = int(*input.ProvisionedThroughput.ReadCapacityUnits)
		writeCapacity = int(*input.ProvisionedThroughput.WriteCapacityUnits)
	}

	if err := svc.storage.UpdateTable(tableName, readCapacity, writeCapacity, newBillingMode, storageOperations); err != nil {
		return err
	}

	return nil
}

func (svc *Service) processGSIUpdates(table *core.TableMetaData, updates []types.GlobalSecondaryIndexUpdate) error {
	// Phase 1: Validate ALL operations first (fail fast)
	for _, update := range updates {
		if update.Create != nil {
			if err := svc.validateGSICreate(table, update.Create); err != nil {
				return err
			}
		}
		if update.Update != nil {
			if err := svc.validateGSIUpdate(table, update.Update); err != nil {
				return err
			}
		}
		if update.Delete != nil {
			if err := svc.validateGSIDelete(table, update.Delete); err != nil {
				return err
			}
		}
	}

	// Phase 2: Update metadata first (fail fast - cheaper operation)
	for _, update := range updates {
		if update.Create != nil {
			if err := svc.addGSIToTableMetadata(table, update.Create); err != nil {
				return err
			}
		}
		if update.Update != nil {
			if err := svc.updateGSIInTableMetadata(table, update.Update); err != nil {
				return err
			}
		}
		if update.Delete != nil {
			if err := svc.removeGSIFromTableMetadata(table, update.Delete); err != nil {
				return err
			}
		}
	}

	return nil
}

func (svc *Service) validateGSICreate(table *core.TableMetaData, create *types.CreateGlobalSecondaryIndexAction) error {
	if create.IndexName == nil || *create.IndexName == "" {
		return &ValidationException{Message: "Index name is required"}
	}

	if err := core.ValidateTableName(*create.IndexName); err != nil {
		return &ValidationException{
			Message: err.Error(),
		}
	}

	// For CREATE: GSI must NOT exist
	if svc.gsiExists(table, create.IndexName) {
		return &ValidationException{Message: "Global Secondary Index already exists"}
	}

	if err := svc.validateGSIKeySchema(table, create.KeySchema); err != nil {
		return err
	}

	if err := svc.validateGSIProjection(table, create.Projection); err != nil {
		return err
	}
	if table.BillingMode == core.BILLING_MODE_PROVISIONED && create.ProvisionedThroughput == nil {
		return &ValidationException{Message: "ProvisionedThroughput is required when BillingMode is PROVISIONED"}
	}

	return nil
}

func (svc *Service) validateGSIUpdate(table *core.TableMetaData, update *types.UpdateGlobalSecondaryIndexAction) error {
	if update.IndexName == nil || *update.IndexName == "" {
		return &ValidationException{Message: "Index name is required"}
	}

	// For UPDATE: GSI must exist
	if !svc.gsiExists(table, update.IndexName) {
		return &ValidationException{Message: "Global Secondary Index not found"}
	}

	// GSI updates are limited to throughput settings only
	if table.BillingMode == core.BILLING_MODE_PROVISIONED && update.ProvisionedThroughput == nil {
		return &ValidationException{Message: "ProvisionedThroughput is required when BillingMode is PROVISIONED"}
	}
	if update.ProvisionedThroughput != nil {
		if err := svc.validateProvisionedThroughput(update.ProvisionedThroughput); err != nil {
			return err
		}
	}

	return nil
}

func (svc *Service) validateGSIDelete(table *core.TableMetaData, delete *types.DeleteGlobalSecondaryIndexAction) error {
	if delete.IndexName == nil || *delete.IndexName == "" {
		return &ValidationException{Message: "Index name is required"}
	}

	// For DELETE: GSI must exist
	if !svc.gsiExists(table, delete.IndexName) {
		return &ValidationException{Message: "Global Secondary Index not found"}
	}

	return nil
}

func (svc *Service) gsiExists(table *core.TableMetaData, indexName *string) bool {
	if indexName == nil {
		return false
	}

	for _, existingGSI := range table.GlobalSecondaryIndexSettings {
		if existingGSI.IndexName != nil && *existingGSI.IndexName == *indexName {
			return true
		}
	}

	return false
}

func (svc *Service) validateGSIKeySchema(table *core.TableMetaData, keySchema []types.KeySchemaElement) error {
	if len(keySchema) == 0 {
		return &ValidationException{Message: "KeySchema is required for Global Secondary Index"}
	}

	var hasPartitionKey bool
	var hasSortKey bool
	attributeDefinitionMap := make(map[string]types.AttributeDefinition)

	// Build attribute definition map for lookup
	for _, attrDef := range table.AttributeDefinitions {
		attributeDefinitionMap[*attrDef.AttributeName] = attrDef
	}

	for _, element := range keySchema {
		if element.AttributeName == nil {
			return &ValidationException{Message: "Attribute name is required in KeySchema"}
		}

		// Check attribute exists in table definition
		if _, exists := attributeDefinitionMap[*element.AttributeName]; !exists {
			return &ValidationException{Message: "Attribute not found in table attribute definitions"}
		}

		// Validate key types
		switch element.KeyType {
		case types.KeyTypeHash:
			if hasPartitionKey {
				return &ValidationException{Message: "Multiple partition keys not allowed"}
			}
			hasPartitionKey = true
		case types.KeyTypeRange:
			if hasSortKey {
				return &ValidationException{Message: "Multiple sort keys not allowed"}
			}
			hasSortKey = true
		default:
			return &ValidationException{Message: "Invalid key type"}
		}
	}

	if !hasPartitionKey {
		return &ValidationException{Message: "Partition key is required for Global Secondary Index"}
	}

	return nil
}

func (svc *Service) validateGSIProjection(table *core.TableMetaData, projection *types.Projection) error {
	if projection == nil {
		return nil // Projection is optional, defaults to ALL
	}

	switch projection.ProjectionType {
	case types.ProjectionTypeAll:
		// No additional validation needed
	case types.ProjectionTypeKeysOnly:
		if len(projection.NonKeyAttributes) > 0 {
			return &ValidationException{Message: "NonKeyAttributes not allowed with KEYS_ONLY projection"}
		}
	case types.ProjectionTypeInclude:
		if len(projection.NonKeyAttributes) == 0 {
			return &ValidationException{Message: "NonKeyAttributes required with INCLUDE projection"}
		}

		// Validate that NonKeyAttributes exist in table
		attributeDefinitionMap := make(map[string]bool)
		for _, attrDef := range table.AttributeDefinitions {
			attributeDefinitionMap[*attrDef.AttributeName] = true
		}

		for _, attr := range projection.NonKeyAttributes {
			if !attributeDefinitionMap[attr] {
				return &ValidationException{Message: "NonKeyAttribute not found in table attribute definitions"}
			}
		}
	default:
		return &ValidationException{Message: "Invalid projection type"}
	}

	return nil
}

func (svc *Service) validateProvisionedThroughput(throughput *types.ProvisionedThroughput) error {
	if throughput.ReadCapacityUnits != nil && *throughput.ReadCapacityUnits < 1 {
		return &ValidationException{Message: "Read capacity units must be greater than 0"}
	}
	if throughput.WriteCapacityUnits != nil && *throughput.WriteCapacityUnits < 1 {
		return &ValidationException{Message: "Write capacity units must be greater than 0"}
	}
	return nil
}

func (svc *Service) addGSIToTableMetadata(table *core.TableMetaData, create *types.CreateGlobalSecondaryIndexAction) error {
	var attributeDefinitionMap = make(map[string]types.AttributeDefinition)
	for _, attrDef := range table.AttributeDefinitions {
		attributeDefinitionMap[*attrDef.AttributeName] = attrDef
	}

	var partitionKeySchema *core.KeySchema
	var sortKeySchema *core.KeySchema

	for _, keySchema := range create.KeySchema {
		def, ok := attributeDefinitionMap[*keySchema.AttributeName]
		if !ok {
			return &ValidationException{Message: "Attribute not found in table attribute definitions"}
		}
		attrType, err := core.GetScalarAttributeType(def)
		if err != nil {
			return &ValidationException{Message: err.Error()}
		}

		if keySchema.KeyType == types.KeyTypeHash {
			partitionKeySchema = &core.KeySchema{
				AttributeName: *keySchema.AttributeName,
				AttributeType: attrType,
			}
		} else if keySchema.KeyType == types.KeyTypeRange {
			sortKeySchema = &core.KeySchema{
				AttributeName: *keySchema.AttributeName,
				AttributeType: attrType,
			}
		}
	}

	projectionType := core.PROJECTION_TYPE_ALL
	if create.Projection != nil {
		switch create.Projection.ProjectionType {
		case types.ProjectionTypeKeysOnly:
			projectionType = core.PROJECTION_TYPE_KEYS_ONLY
		case types.ProjectionTypeInclude:
			projectionType = core.PROJECTION_TYPE_INCLUDE
		case types.ProjectionTypeAll:
			projectionType = core.PROJECTION_TYPE_ALL
		}
	}

	gsiSetting := core.GlobalSecondaryIndexSetting{
		IndexName:          create.IndexName,
		PartitionKeySchema: partitionKeySchema,
		SortKeySchema:      sortKeySchema,
		ProjectionType:     projectionType,
	}

	if create.Projection != nil && len(create.Projection.NonKeyAttributes) > 0 {
		gsiSetting.NonKeyAttributes = create.Projection.NonKeyAttributes
	}

	table.GlobalSecondaryIndexSettings = append(table.GlobalSecondaryIndexSettings, gsiSetting)
	return nil
}

func (svc *Service) updateGSIInTableMetadata(table *core.TableMetaData, update *types.UpdateGlobalSecondaryIndexAction) error {
	for i, gsi := range table.GlobalSecondaryIndexSettings {
		if gsi.IndexName != nil && *gsi.IndexName == *update.IndexName {
			if update.ProvisionedThroughput != nil {
				pt, err := core.BuildProvisionedThroughput(update.ProvisionedThroughput)
				if err != nil {
					return &ValidationException{Message: err.Error()}
				}
				table.GlobalSecondaryIndexSettings[i].ProvisionedThroughput = pt
			} else {
				table.GlobalSecondaryIndexSettings[i].ProvisionedThroughput = nil
			}
			return nil
		}
	}
	return &ValidationException{Message: "Global Secondary Index not found"}
}

func (svc *Service) removeGSIFromTableMetadata(table *core.TableMetaData, delete *types.DeleteGlobalSecondaryIndexAction) error {
	for i, gsi := range table.GlobalSecondaryIndexSettings {
		if gsi.IndexName != nil && *gsi.IndexName == *delete.IndexName {
			table.GlobalSecondaryIndexSettings = append(
				table.GlobalSecondaryIndexSettings[:i],
				table.GlobalSecondaryIndexSettings[i+1:]...,
			)
			return nil
		}
	}
	return &ValidationException{Message: "Global Secondary Index not found"}
}

func (svc *Service) convertToStorageCreateAction(create *types.CreateGlobalSecondaryIndexAction) *storage.CreateGSIAction {
	action := &storage.CreateGSIAction{
		IndexName: create.IndexName,
	}

	// Convert KeySchema
	for _, keySchema := range create.KeySchema {
		if keySchema.KeyType == types.KeyTypeHash {
			action.PartitionKeyName = keySchema.AttributeName
		} else if keySchema.KeyType == types.KeyTypeRange {
			action.SortKeyName = keySchema.AttributeName
		}
	}

	// Convert Projection
	if create.Projection != nil {
		switch create.Projection.ProjectionType {
		case types.ProjectionTypeKeysOnly:
			action.ProjectionType = core.PROJECTION_TYPE_KEYS_ONLY
		case types.ProjectionTypeInclude:
			action.ProjectionType = core.PROJECTION_TYPE_INCLUDE
			action.NonKeyAttributes = create.Projection.NonKeyAttributes
		case types.ProjectionTypeAll:
			action.ProjectionType = core.PROJECTION_TYPE_ALL
		}
	} else {
		action.ProjectionType = core.PROJECTION_TYPE_ALL
	}

	return action
}

func (svc *Service) DeleteTable(ctx context.Context, input *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error) {
	svc.tableLock.Lock()
	defer svc.tableLock.Unlock()

	tableName := *input.TableName
	if _, ok := svc.tableMetadataStore[tableName]; ok {
		table := svc.tableMetadataStore[tableName]

		itemCount, err := svc.storage.QueryItemCount(tableName)
		if err != nil {
			return nil, err
		}
		tableDescription := table.Description(itemCount)
		delete(svc.tableMetadataStore, tableName)

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
	if _, ok := svc.tableMetadataStore[tableName]; ok {
		table := svc.tableMetadataStore[tableName]
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
		var pk *storage.PrimaryKey
		var tableName string
		if writeItem.ConditionCheck != nil {
			conditionCheck := writeItem.ConditionCheck

			tableName = *conditionCheck.TableName
			tableMetadata, ok := svc.tableMetadataStore[tableName]
			if !ok {
				msg := "Cannot do operations on a non-existent table"
				return &types.ResourceNotFoundException{
					Message: &msg,
				}
			}
			entry, err := core.NewEntryFromItem(conditionCheck.Key)
			if err != nil {
				return err
			}

			pk, err = svc.buildTablePrimaryKey(entry, tableMetadata)
			if err != nil {
				return err
			}
		} else if writeItem.Put != nil {
			put := writeItem.Put

			tableName = *put.TableName
			tableMetadata, ok := svc.tableMetadataStore[tableName]
			if !ok {
				msg := "Cannot do operations on a non-existent table"
				return &types.ResourceNotFoundException{
					Message: &msg,
				}
			}

			entry, err := core.NewEntryFromItem(put.Item)
			if err != nil {
				return err
			}
			pk, err = svc.buildTablePrimaryKey(entry, tableMetadata)
			if err != nil {
				return err
			}
		} else if writeItem.Delete != nil {
			deleteReq := writeItem.Delete

			tableName = *deleteReq.TableName
			tableMetadata, ok := svc.tableMetadataStore[tableName]
			if !ok {
				msg := "Cannot do operations on a non-existent table"
				return &types.ResourceNotFoundException{
					Message: &msg,
				}
			}

			entry, err := core.NewEntryFromItem(deleteReq.Key)
			if err != nil {
				return err
			}
			pk, err = svc.buildTablePrimaryKey(entry, tableMetadata)
			if err != nil {
				return err
			}
		} else if writeItem.Update != nil {
			update := writeItem.Update

			tableName = *update.TableName
			tableMetadata, ok := svc.tableMetadataStore[tableName]
			if !ok {
				msg := "Cannot do operations on a non-existent table"
				return &types.ResourceNotFoundException{
					Message: &msg,
				}
			}
			entry, err := core.NewEntryFromItem(update.Key)
			if err != nil {
				return err
			}
			pk, err = svc.buildTablePrimaryKey(entry, tableMetadata)
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
func (svc *Service) buildTablePrimaryKey(entry *core.Entry, table *core.TableMetaData) (*storage.PrimaryKey, error) {
	primaryKey := &storage.PrimaryKey{
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

type TransactionCanceledException struct {
	rawError            error
	CancellationReasons []CancellationReason
}

type CancellationReason struct {
	Code    string
	Message string
}

func (e *TransactionCanceledException) Error() string {
	// TODO: improve error message
	var reason = "ConditionalCheckFailed"
	return fmt.Sprintf("Transaction cancelled, please refer cancellation reasons for specific reasons [%s]", reason)
}

func (svc *Service) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
	// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
	svc.tableLock.RLock()
	defer svc.tableLock.RUnlock()

	err := svc.validateTransactWriteItemsInput(input)
	if err != nil {
		return nil, err
	}

	txn, err := svc.storage.BeginTxn()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	for _, writeItem := range input.TransactItems {
		if writeItem.ConditionCheck != nil {
			conditionCheck := writeItem.ConditionCheck
			tableName := *conditionCheck.TableName
			if _, ok := svc.tableMetadataStore[tableName]; !ok {
				msg := "Cannot do operations on a non-existent table"
				err = &types.ResourceNotFoundException{
					Message: &msg,
				}
				return nil, err
			}

			var cond *condition.Condition
			if conditionCheck.ConditionExpression == nil || *conditionCheck.ConditionExpression == "" {
				return nil, &ValidationException{
					Message: "The expression can not be empty;",
				}
			}

			expressionAttributeValues, err := core.TransformAttributeValueMap(conditionCheck.ExpressionAttributeValues)
			if err != nil {
				return nil, &ValidationException{
					Message: err.Error(),
				}
			}
			cond, err = condition.BuildCondition(
				*conditionCheck.ConditionExpression,
				conditionCheck.ExpressionAttributeNames,
				expressionAttributeValues,
			)
			if err != nil {
				return nil, &ValidationException{
					Message: err.Error(),
				}
			}

			key, err := core.NewEntryFromItem(conditionCheck.Key)
			if err != nil {
				return nil, &ValidationException{
					Message: err.Error(),
				}
			}

			req := &storage.GetRequest{
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
			matched, err := cond.Check(entry)

			if err != nil {
				return nil, err
			} else if matched {
				continue
			} else {
				return nil, wrapTransactionError(err)
			}

		} else if writeItem.Put != nil {
			put := writeItem.Put
			tableName := *put.TableName
			if _, ok := svc.tableMetadataStore[tableName]; !ok {
				msg := "Cannot do operations on a non-existent table"
				err = &types.ResourceNotFoundException{
					Message: &msg,
				}
				return nil, err
			}
			builder := &request.PutRequestBuilder{
				ConditionExpression:       put.ConditionExpression,
				ExpressionAttributeNames:  put.ExpressionAttributeNames,
				ExpressionAttributeValues: put.ExpressionAttributeValues,
				Item:                      put.Item,
				TableName:                 put.TableName,
			}
			req, err := builder.Build()
			if err != nil {
				return nil, &ValidationException{
					Message: err.Error(),
				}
			}
			err = svc.storage.PutWithTransaction(req, txn)
			if err != nil {
				return nil, wrapTransactionError(err)
			}
		} else if writeItem.Delete != nil {
			deleteReq := writeItem.Delete
			tableName := *deleteReq.TableName
			if _, ok := svc.tableMetadataStore[tableName]; !ok {
				msg := "Cannot do operations on a non-existent table"
				err = &types.ResourceNotFoundException{
					Message: &msg,
				}
				return nil, err
			}

			builder := &request.DeleteRequestBuilder{
				TableName:                 deleteReq.TableName,
				ConditionExpression:       deleteReq.ConditionExpression,
				ExpressionAttributeNames:  deleteReq.ExpressionAttributeNames,
				ExpressionAttributeValues: deleteReq.ExpressionAttributeValues,
				Key:                       deleteReq.Key,
			}
			req, err := builder.Build()
			if err != nil {
				return nil, &ValidationException{
					Message: err.Error(),
				}
			}

			err = svc.storage.DeleteWithTransaction(req, txn)
			if err != nil {
				return nil, err
			}
		} else if writeItem.Update != nil {
			updateReq := writeItem.Update
			tableName := *updateReq.TableName
			if _, ok := svc.tableMetadataStore[tableName]; !ok {
				msg := "Cannot do operations on a non-existent table"
				err = &types.ResourceNotFoundException{
					Message: &msg,
				}
				return nil, err
			}

			builder := &request.UpdateRequestBuilder{
				TableName:                 updateReq.TableName,
				UpdateExpression:          updateReq.UpdateExpression,
				ExpressionAttributeNames:  updateReq.ExpressionAttributeNames,
				ExpressionAttributeValues: updateReq.ExpressionAttributeValues,
				ConditionExpression:       updateReq.ConditionExpression,
				Key:                       updateReq.Key,
			}
			req, err := builder.Build()
			if err != nil {
				return nil, &ValidationException{
					Message: err.Error(),
				}
			}

			_, err = svc.storage.UpdateWithTransaction(req, txn)
			if err != nil {
				return nil, wrapTransactionError(err)
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

func wrapTransactionError(err error) error {
	var conditionalCheckFailedException *storage.ConditionalCheckFailedException
	if errors.As(err, &conditionalCheckFailedException) {
		return &TransactionCanceledException{
			rawError: err,
			CancellationReasons: []CancellationReason{
				{
					Code:    "ConditionalCheckFailed",
					Message: "The conditional request failed",
				},
			},
		}
	} else if errors.Is(err, storage.RateLimitReachedError) {
		return ProvisionedThroughputExceededException
	} else {
		return err
	}
}

func wrapError(err error) error {
	if errors.Is(err, storage.RateLimitReachedError) {
		return ProvisionedThroughputExceededException
	} else {
		return err
	}
}

func (svc *Service) Scan(ctx context.Context, input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html
	svc.tableLock.RLock()
	defer svc.tableLock.RUnlock()

	tableName := *input.TableName
	tableMetadata, ok := svc.tableMetadataStore[tableName]
	if !ok {
		msg := "Cannot do operations on a non-existent table"
		err := &types.ResourceNotFoundException{
			Message: &msg,
		}
		return nil, err
	}

	expressionAttributeValues, err := core.TransformAttributeValueMap(input.ExpressionAttributeValues)
	if err != nil {
		return nil, &ValidationException{
			Message: err.Error(),
		}
	}

	scanReqBuilder := &scan.RequestBuilder{
		FilterExpressionStr:       input.FilterExpression,
		ExpressionAttributeNames:  input.ExpressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
		TableMetadata:             tableMetadata,
		ExclusiveStartKey:         input.ExclusiveStartKey,
		ConsistentRead:            input.ConsistentRead,
		Limit:                     input.Limit,
		IndexName:                 input.IndexName,
		Segment:                   input.Segment,
		TotalSegments:             input.TotalSegments,
	}
	scanReq, err := scanReqBuilder.Build()
	if err != nil {
		return nil, &ValidationException{
			Message: err.Error(),
		}
	}

	res, err := svc.storage.Scan(scanReq)
	if err != nil {
		return nil, wrapError(err)
	}

	entries := res.Entries
	items := make([]map[string]types.AttributeValue, len(entries))
	for i, entry := range entries {
		items[i] = core.NewItemFromEntry(entry.Body)
	}
	lastEvaluatedKey, err := buildLastEvaluatedKey(entries, tableMetadata)

	output := &dynamodb.ScanOutput{
		Count:            int32(len(res.Entries)),
		ScannedCount:     res.ScannedCount,
		LastEvaluatedKey: lastEvaluatedKey,
		Items:            items,
	}

	// TODO: handle select,ProjectionExpression

	return output, nil
}

func buildLastEvaluatedKey(entries []*core.Entry, tableMetadata *core.TableMetaData) (map[string]types.AttributeValue, error) {
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

	return lastEvaluatedKey, nil
}

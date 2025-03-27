package encoding

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/ocowchun/baddb/ddb"
	"io"
	"log"
	"strconv"
	"time"
)

type timestamp time.Time

func newTimestamp(t *time.Time) *timestamp {
	ts := timestamp(*t)
	return &ts
}

func (t timestamp) MarshalJSON() ([]byte, error) {
	ts := time.Time(t)
	return []byte(strconv.FormatInt(ts.Unix(), 10)), nil
}

type tableDescription struct {

	// Contains information about the table archive.
	ArchivalSummary *types.ArchivalSummary

	AttributeDefinitions []types.AttributeDefinition

	BillingModeSummary *types.BillingModeSummary

	CreationDateTime *timestamp

	DeletionProtectionEnabled *bool

	GlobalSecondaryIndexes []types.GlobalSecondaryIndexDescription

	GlobalTableVersion *string

	ItemCount int64

	KeySchema []types.KeySchemaElement

	LatestStreamArn *string

	LatestStreamLabel     *string
	LocalSecondaryIndexes []types.LocalSecondaryIndexDescription

	MultiRegionConsistency types.MultiRegionConsistency

	OnDemandThroughput *types.OnDemandThroughput

	ProvisionedThroughput *types.ProvisionedThroughputDescription

	Replicas []types.ReplicaDescription

	RestoreSummary *types.RestoreSummary

	SSEDescription *types.SSEDescription

	StreamSpecification *types.StreamSpecification

	TableArn *string

	TableClassSummary *types.TableClassSummary

	TableId *string

	TableName *string

	TableSizeBytes int64

	TableStatus types.TableStatus

	WarmThroughput *types.TableWarmThroughputDescription
}

func newTableDescription(description *types.TableDescription) *tableDescription {
	return &tableDescription{
		ArchivalSummary:           description.ArchivalSummary,
		AttributeDefinitions:      description.AttributeDefinitions,
		BillingModeSummary:        description.BillingModeSummary,
		CreationDateTime:          newTimestamp(description.CreationDateTime),
		DeletionProtectionEnabled: description.DeletionProtectionEnabled,
		GlobalSecondaryIndexes:    description.GlobalSecondaryIndexes,
		GlobalTableVersion:        description.GlobalTableVersion,
		ItemCount:                 *description.ItemCount,
		KeySchema:                 description.KeySchema,
		LatestStreamArn:           description.LatestStreamArn,
		LatestStreamLabel:         description.LatestStreamLabel,
		LocalSecondaryIndexes:     description.LocalSecondaryIndexes,
		MultiRegionConsistency:    description.MultiRegionConsistency,
		OnDemandThroughput:        description.OnDemandThroughput,
		ProvisionedThroughput:     description.ProvisionedThroughput,
		Replicas:                  description.Replicas,
		RestoreSummary:            description.RestoreSummary,
		SSEDescription:            description.SSEDescription,
		StreamSpecification:       description.StreamSpecification,
		TableArn:                  description.TableArn,
		TableClassSummary:         description.TableClassSummary,
		TableId:                   description.TableId,
		TableName:                 description.TableName,
		TableSizeBytes:            *description.TableSizeBytes,
		TableStatus:               description.TableStatus,
		WarmThroughput:            description.WarmThroughput,
	}

}

type KeysAndAttributes struct {
	Keys                     []map[string]ddb.AttributeValue
	AttributesToGet          []string
	ConsistentRead           *bool
	ExpressionAttributeNames map[string]string
	ProjectionExpression     *string
}

type batchGetItemInput struct {
	RequestItems map[string]KeysAndAttributes
}

func DecodeBatchGetItemInput(reader io.ReadCloser) (*dynamodb.BatchGetItemInput, error) {
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing request body: %v", err)
		}
	}()

	var input2 batchGetItemInput
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &input2)

	requestItems := map[string]types.KeysAndAttributes{}
	for tableName, keysAndAttributes := range input2.RequestItems {
		keys := make([]map[string]types.AttributeValue, len(keysAndAttributes.Keys))
		for i, key := range keysAndAttributes.Keys {
			keys[i] = transformToDdbMap(key)
		}
		requestItems[tableName] = types.KeysAndAttributes{
			Keys:                     keys,
			AttributesToGet:          keysAndAttributes.AttributesToGet,
			ConsistentRead:           keysAndAttributes.ConsistentRead,
			ExpressionAttributeNames: keysAndAttributes.ExpressionAttributeNames,
			ProjectionExpression:     keysAndAttributes.ProjectionExpression,
		}
	}
	input := &dynamodb.BatchGetItemInput{
		RequestItems: requestItems,
	}

	return input, err
}

type batchGetItemOutput struct {
	Responses       map[string][]map[string]ddb.AttributeValue
	UnprocessedKeys map[string]KeysAndAttributes
}

func EncodeBatchGetItemOutput(output *dynamodb.BatchGetItemOutput) ([]byte, error) {
	responses := make(map[string][]map[string]ddb.AttributeValue, len(output.Responses))
	for tableName, items := range output.Responses {
		items2 := make([]map[string]ddb.AttributeValue, len(items))
		for i, item := range items {
			items2[i] = ddb.NewEntryFromItem(item).Body
		}
		responses[tableName] = items2
	}

	unprocessedKeys := make(map[string]KeysAndAttributes, len(output.UnprocessedKeys))
	for tableName, keysAndAttributes := range output.UnprocessedKeys {
		keys := make([]map[string]ddb.AttributeValue, len(keysAndAttributes.Keys))
		for i, key := range keysAndAttributes.Keys {
			keys[i] = ddb.NewEntryFromItem(key).Body
		}
		unprocessedKeys[tableName] = KeysAndAttributes{
			Keys:                     keys,
			AttributesToGet:          keysAndAttributes.AttributesToGet,
			ConsistentRead:           keysAndAttributes.ConsistentRead,
			ExpressionAttributeNames: keysAndAttributes.ExpressionAttributeNames,
			ProjectionExpression:     keysAndAttributes.ProjectionExpression,
		}
	}

	output2 := batchGetItemOutput{
		Responses:       responses,
		UnprocessedKeys: unprocessedKeys,
	}

	bs, err := json.Marshal(output2)
	return bs, err
}

type DeleteRequest struct {
	Key map[string]ddb.AttributeValue
}
type PutRequest struct {
	Item map[string]ddb.AttributeValue
}

type WriteRequest struct {
	DeleteRequest *DeleteRequest
	PutRequest    *PutRequest
}

type batchWriteItemInput struct {
	RequestItems map[string][]WriteRequest
}

func DecodeBatchWriteItemInput(reader io.ReadCloser) (*dynamodb.BatchWriteItemInput, error) {
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing request body: %v", err)
		}
	}()

	var input2 batchWriteItemInput
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &input2)
	if err != nil {
		return nil, err
	}

	requestItems := make(map[string][]types.WriteRequest)
	for tableName, writeRequests := range input2.RequestItems {
		requests := make([]types.WriteRequest, len(writeRequests))
		for i, writeRequest := range writeRequests {
			if writeRequest.DeleteRequest != nil {
				requests[i] = types.WriteRequest{
					DeleteRequest: &types.DeleteRequest{
						Key: transformToDdbMap(writeRequest.DeleteRequest.Key),
					},
				}
			} else {
				requests[i] = types.WriteRequest{
					PutRequest: &types.PutRequest{
						Item: transformToDdbMap(writeRequest.PutRequest.Item),
					},
				}
			}
		}
		requestItems[tableName] = requests
	}
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: requestItems,
	}
	return input, nil
}

type batchWriteItemOutput struct {
	UnprocessedItems map[string][]WriteRequest
}

func EncodeBatchWriteItemOutput(output *dynamodb.BatchWriteItemOutput) ([]byte, error) {
	unprocessedItems := make(map[string][]WriteRequest, len(output.UnprocessedItems))
	for tableName, writeRequests := range output.UnprocessedItems {
		requests := make([]WriteRequest, len(writeRequests))
		for i, writeRequest := range writeRequests {
			if writeRequest.DeleteRequest != nil {
				requests[i] = WriteRequest{
					DeleteRequest: &DeleteRequest{
						Key: ddb.NewEntryFromItem(writeRequest.DeleteRequest.Key).Body,
					},
				}
			} else {
				requests[i] = WriteRequest{
					PutRequest: &PutRequest{
						Item: ddb.NewEntryFromItem(writeRequest.PutRequest.Item).Body,
					},
				}
			}
		}
		unprocessedItems[tableName] = requests
	}

	output2 := batchWriteItemOutput{
		UnprocessedItems: unprocessedItems,
	}

	bs, err := json.Marshal(output2)
	return bs, err
}

func DecodeListTablesInput(reader io.ReadCloser) (*dynamodb.ListTablesInput, error) {
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing request body: %v", err)
		}
	}()

	var input dynamodb.ListTablesInput
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &input)

	return &input, err
}

func EncodeListTablesOutput(output *dynamodb.ListTablesOutput) ([]byte, error) {
	bs, err := json.Marshal(output)
	return bs, err
}

func DecodeCreateTableInput(reader io.ReadCloser) (*dynamodb.CreateTableInput, error) {
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing request body: %v", err)
		}
	}()

	var input dynamodb.CreateTableInput
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &input)

	return &input, err
}

type createTableOutput struct {
	TableDescription *tableDescription

	// Metadata pertaining to the operation's result.
	ResultMetadata middleware.Metadata
}

func EncodeCreateTableOutput(output *dynamodb.CreateTableOutput) ([]byte, error) {
	//return &dynamodb.CreateTableOutput{}
	output2 := createTableOutput{
		TableDescription: newTableDescription(output.TableDescription),
		ResultMetadata:   output.ResultMetadata,
	}

	bs, err := json.Marshal(output2)
	log.Println("createTableOutput ->", string(bs))
	return bs, err
}

type putItemInput struct {
	Item                                map[string]ddb.AttributeValue
	TableName                           *string
	ConditionExpression                 *string
	ConditionalOperator                 types.ConditionalOperator
	Expected                            map[string]types.ExpectedAttributeValue
	ExpressionAttributeNames            map[string]string
	ExpressionAttributeValues           map[string]ddb.AttributeValue
	ReturnConsumedCapacity              types.ReturnConsumedCapacity
	ReturnItemCollectionMetrics         types.ReturnItemCollectionMetrics
	ReturnValues                        types.ReturnValue
	ReturnValuesOnConditionCheckFailure types.ReturnValuesOnConditionCheckFailure
}

func DecodePutItemInput(reader io.ReadCloser) (*dynamodb.PutItemInput, error) {
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing request body: %v", err)
		}
	}()

	body, err := io.ReadAll(reader)

	if err != nil {
		return nil, err
	}

	var input2 putItemInput
	err = json.Unmarshal(body, &input2)

	input := dynamodb.PutItemInput{
		TableName:                           input2.TableName,
		Item:                                transformToDdbMap(input2.Item),
		ConditionExpression:                 input2.ConditionExpression,
		ConditionalOperator:                 input2.ConditionalOperator,
		Expected:                            input2.Expected,
		ExpressionAttributeNames:            input2.ExpressionAttributeNames,
		ExpressionAttributeValues:           transformToDdbMap(input2.ExpressionAttributeValues),
		ReturnConsumedCapacity:              input2.ReturnConsumedCapacity,
		ReturnItemCollectionMetrics:         input2.ReturnItemCollectionMetrics,
		ReturnValues:                        input2.ReturnValues,
		ReturnValuesOnConditionCheckFailure: input2.ReturnValuesOnConditionCheckFailure,
	}

	return &input, nil
}

func EncodePutItemOutput(output *dynamodb.PutItemOutput) ([]byte, error) {
	bs, err := json.Marshal(output)
	return bs, err
}

type updateItemInput struct {
	Key                                 map[string]ddb.AttributeValue
	TableName                           *string
	ConditionExpression                 *string
	ConditionalOperator                 types.ConditionalOperator
	Expected                            map[string]types.ExpectedAttributeValue
	ExpressionAttributeNames            map[string]string
	ExpressionAttributeValues           map[string]ddb.AttributeValue
	ReturnConsumedCapacity              types.ReturnConsumedCapacity
	ReturnItemCollectionMetrics         types.ReturnItemCollectionMetrics
	ReturnValues                        types.ReturnValue
	ReturnValuesOnConditionCheckFailure types.ReturnValuesOnConditionCheckFailure
	UpdateExpression                    *string
}

func DecodeUpdateItemInput(reader io.ReadCloser) (*dynamodb.UpdateItemInput, error) {
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing request body: %v", err)
		}
	}()

	body, err := io.ReadAll(reader)

	if err != nil {
		return nil, err
	}

	var input2 updateItemInput
	err = json.Unmarshal(body, &input2)

	input := dynamodb.UpdateItemInput{
		TableName:                           input2.TableName,
		Key:                                 transformToDdbMap(input2.Key),
		ConditionExpression:                 input2.ConditionExpression,
		ConditionalOperator:                 input2.ConditionalOperator,
		Expected:                            input2.Expected,
		ExpressionAttributeNames:            input2.ExpressionAttributeNames,
		ExpressionAttributeValues:           transformToDdbMap(input2.ExpressionAttributeValues),
		ReturnConsumedCapacity:              input2.ReturnConsumedCapacity,
		ReturnItemCollectionMetrics:         input2.ReturnItemCollectionMetrics,
		ReturnValues:                        input2.ReturnValues,
		ReturnValuesOnConditionCheckFailure: input2.ReturnValuesOnConditionCheckFailure,
		UpdateExpression:                    input2.UpdateExpression,
	}

	return &input, nil
}

type updateItemOutput struct {
	ConsumedCapacity *types.ConsumedCapacity

	Attributes map[string]ddb.AttributeValue

	ResultMetadata middleware.Metadata
}

func EncodeUpdateItemOutput(output *dynamodb.UpdateItemOutput) ([]byte, error) {
	output2 := updateItemOutput{
		ConsumedCapacity: output.ConsumedCapacity,
		Attributes:       ddb.NewEntryFromItem(output.Attributes).Body,
		ResultMetadata:   output.ResultMetadata,
	}

	bs, err := json.Marshal(output2)
	return bs, err
}

type getItemInput struct {
	Key                      map[string]ddb.AttributeValue
	TableName                *string
	AttributesToGet          []string
	ConsistentRead           *bool
	ExpressionAttributeNames map[string]string
	ProjectionExpression     *string
	ReturnConsumedCapacity   types.ReturnConsumedCapacity
}

func DecodeGetItemInput(reader io.ReadCloser) (*dynamodb.GetItemInput, error) {
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing request body: %v", err)
		}
	}()

	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	var input2 getItemInput
	err = json.Unmarshal(body, &input2)

	input := dynamodb.GetItemInput{
		TableName:                input2.TableName,
		Key:                      transformToDdbMap(input2.Key),
		AttributesToGet:          input2.AttributesToGet,
		ConsistentRead:           input2.ConsistentRead,
		ExpressionAttributeNames: input2.ExpressionAttributeNames,
		ProjectionExpression:     input2.ProjectionExpression,
		ReturnConsumedCapacity:   input2.ReturnConsumedCapacity,
	}

	return &input, nil
}

type getItemOutput struct {
	ConsumedCapacity *types.ConsumedCapacity

	Item map[string]ddb.AttributeValue

	ResultMetadata middleware.Metadata
}

func EncodeGetItemOutput(output *dynamodb.GetItemOutput) ([]byte, error) {
	output2 := getItemOutput{
		ConsumedCapacity: output.ConsumedCapacity,
		Item:             ddb.NewEntryFromItem(output.Item).Body,
		ResultMetadata:   output.ResultMetadata,
	}
	bs, err := json.Marshal(output2)
	return bs, err
}

type queryInput struct {
	TableName                 *string
	ConsistentRead            *bool
	ExclusiveStartKey         map[string]ddb.AttributeValue
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]ddb.AttributeValue
	FilterExpression          *string
	Limit                     *int32
	IndexName                 *string
	ScanIndexForward          *bool
	KeyConditionExpression    *string
}

func DecodeQueryInput(reader io.ReadCloser) (*dynamodb.QueryInput, error) {
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing request body: %v", err)
		}
	}()

	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	var input2 queryInput
	err = json.Unmarshal(body, &input2)

	input := dynamodb.QueryInput{
		TableName:                 input2.TableName,
		ConsistentRead:            input2.ConsistentRead,
		ExclusiveStartKey:         transformToDdbMap(input2.ExclusiveStartKey),
		ExpressionAttributeNames:  input2.ExpressionAttributeNames,
		ExpressionAttributeValues: transformToDdbMap(input2.ExpressionAttributeValues),
		FilterExpression:          input2.FilterExpression,
		Limit:                     input2.Limit,
		IndexName:                 input2.IndexName,
		ScanIndexForward:          input2.ScanIndexForward,
		KeyConditionExpression:    input2.KeyConditionExpression,
	}

	return &input, nil
}

type queryOutput struct {
	//ConsumedCapacity *types.ConsumedCapacity
	Count            int32
	Items            []map[string]ddb.AttributeValue
	LastEvaluatedKey map[string]ddb.AttributeValue
	ScannedCount     int32
	//ResultMetadata   middleware.Metadata
}

func EncodeQueryOutput(output *dynamodb.QueryOutput) ([]byte, error) {
	items := make([]map[string]ddb.AttributeValue, len(output.Items))
	for i, item := range output.Items {
		items[i] = ddb.NewEntryFromItem(item).Body
	}

	output2 := queryOutput{
		Count:            output.Count,
		Items:            items,
		LastEvaluatedKey: ddb.NewEntryFromItem(output.LastEvaluatedKey).Body,
	}
	bs, err := json.Marshal(output2)
	return bs, err
}

func transformToDdbMap(m map[string]ddb.AttributeValue) map[string]types.AttributeValue {
	result := map[string]types.AttributeValue{}
	for key, attribute := range m {
		result[key] = attribute.ToDdbAttributeValue()
	}
	return result
}

func DecodingDeleteTableInput(reader io.ReadCloser) (*dynamodb.DeleteTableInput, error) {
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing request body: %v", err)
		}
	}()

	var input dynamodb.DeleteTableInput
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &input)

	return &input, err
}

type deleteTableOutput struct {
	TableDescription *tableDescription

	// Metadata pertaining to the operation's result.
	ResultMetadata middleware.Metadata
}

func EncodeDeleteTableOutput(output *dynamodb.DeleteTableOutput) ([]byte, error) {
	output2 := deleteTableOutput{
		TableDescription: newTableDescription(output.TableDescription),
		ResultMetadata:   output.ResultMetadata,
	}

	bs, err := json.Marshal(output2)
	return bs, err
}

func DecodeDescribeTableInput(reader io.ReadCloser) (*dynamodb.DescribeTableInput, error) {
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing request body: %v", err)
		}
	}()

	var input dynamodb.DescribeTableInput
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &input)

	return &input, err
}

type describeTableOutput struct {
	Table *tableDescription

	// Metadata pertaining to the operation's result.
	ResultMetadata middleware.Metadata
}

func EncodeDescribeTableOutput(output *dynamodb.DescribeTableOutput) ([]byte, error) {
	output2 := describeTableOutput{
		Table:          newTableDescription(output.Table),
		ResultMetadata: output.ResultMetadata,
	}

	bs, err := json.Marshal(output2)
	return bs, err
}

type deleteItemInput struct {
	Key                       map[string]ddb.AttributeValue
	TableName                 *string
	ConditionExpression       *string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]ddb.AttributeValue
}

func DecodeDeleteItemInput(reader io.ReadCloser) (*dynamodb.DeleteItemInput, error) {
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing request body: %v", err)
		}
	}()

	var input2 deleteItemInput
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(body, &input2)

	input := &dynamodb.DeleteItemInput{
		TableName:                 input2.TableName,
		Key:                       transformToDdbMap(input2.Key),
		ConditionExpression:       input2.ConditionExpression,
		ExpressionAttributeNames:  input2.ExpressionAttributeNames,
		ExpressionAttributeValues: transformToDdbMap(input2.ExpressionAttributeValues),
	}

	return input, nil
}

type deleteItemOutput struct {
	Attributes map[string]ddb.AttributeValue
}

func EncodeDeleteItemOutput(output *dynamodb.DeleteItemOutput) ([]byte, error) {
	output2 := deleteItemOutput{
		Attributes: ddb.NewEntryFromItem(output.Attributes).Body,
	}

	bs, err := json.Marshal(output2)
	return bs, err
}

type ConditionCheck struct {
	ConditionExpression                 *string
	Key                                 map[string]ddb.AttributeValue
	TableName                           *string
	ExpressionAttributeNames            map[string]string
	ExpressionAttributeValues           map[string]ddb.AttributeValue
	ReturnValuesOnConditionCheckFailure types.ReturnValuesOnConditionCheckFailure
}
type Delete struct {
	Key                                 map[string]ddb.AttributeValue
	TableName                           *string
	ConditionExpression                 *string
	ExpressionAttributeNames            map[string]string
	ExpressionAttributeValues           map[string]ddb.AttributeValue
	ReturnValuesOnConditionCheckFailure types.ReturnValuesOnConditionCheckFailure
}
type Put struct {
	Item                                map[string]ddb.AttributeValue
	TableName                           *string
	ConditionExpression                 *string
	ExpressionAttributeNames            map[string]string
	ExpressionAttributeValues           map[string]ddb.AttributeValue
	ReturnValuesOnConditionCheckFailure types.ReturnValuesOnConditionCheckFailure
}

type Update struct {
	Key                                 map[string]ddb.AttributeValue
	TableName                           *string
	UpdateExpression                    *string
	ConditionExpression                 *string
	ExpressionAttributeNames            map[string]string
	ExpressionAttributeValues           map[string]ddb.AttributeValue
	ReturnValuesOnConditionCheckFailure types.ReturnValuesOnConditionCheckFailure
}
type TransactWriteItem struct {
	ConditionCheck *ConditionCheck
	Delete         *Delete
	Put            *Put
	Update         *Update
}

type transactWriteItemsInput struct {
	TransactItems []TransactWriteItem
}

func DecodeTransactWriteItemsInput(reader io.ReadCloser) (*dynamodb.TransactWriteItemsInput, error) {
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing request body: %v", err)
		}
	}()

	var input2 transactWriteItemsInput
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(body, &input2)

	transactItems := make([]types.TransactWriteItem, len(input2.TransactItems))
	for i, item := range input2.TransactItems {
		var transactItem types.TransactWriteItem
		if item.ConditionCheck != nil {
			transactItem.ConditionCheck = &types.ConditionCheck{
				ConditionExpression:                 item.ConditionCheck.ConditionExpression,
				Key:                                 transformToDdbMap(item.ConditionCheck.Key),
				TableName:                           item.ConditionCheck.TableName,
				ExpressionAttributeNames:            item.ConditionCheck.ExpressionAttributeNames,
				ExpressionAttributeValues:           transformToDdbMap(item.ConditionCheck.ExpressionAttributeValues),
				ReturnValuesOnConditionCheckFailure: item.ConditionCheck.ReturnValuesOnConditionCheckFailure,
			}
		}
		if item.Delete != nil {
			transactItem.Delete = &types.Delete{
				Key:                                 transformToDdbMap(item.Delete.Key),
				TableName:                           item.Delete.TableName,
				ConditionExpression:                 item.Delete.ConditionExpression,
				ExpressionAttributeNames:            item.Delete.ExpressionAttributeNames,
				ExpressionAttributeValues:           transformToDdbMap(item.Delete.ExpressionAttributeValues),
				ReturnValuesOnConditionCheckFailure: item.Delete.ReturnValuesOnConditionCheckFailure,
			}
		}
		if item.Put != nil {
			transactItem.Put = &types.Put{
				Item:                                transformToDdbMap(item.Put.Item),
				TableName:                           item.Put.TableName,
				ConditionExpression:                 item.Put.ConditionExpression,
				ExpressionAttributeNames:            item.Put.ExpressionAttributeNames,
				ExpressionAttributeValues:           transformToDdbMap(item.Put.ExpressionAttributeValues),
				ReturnValuesOnConditionCheckFailure: item.Put.ReturnValuesOnConditionCheckFailure,
			}
		}
		if item.Update != nil {
			transactItem.Update = &types.Update{
				Key:                                 transformToDdbMap(item.Update.Key),
				TableName:                           item.Update.TableName,
				UpdateExpression:                    item.Update.UpdateExpression,
				ConditionExpression:                 item.Update.ConditionExpression,
				ExpressionAttributeNames:            item.Update.ExpressionAttributeNames,
				ExpressionAttributeValues:           transformToDdbMap(item.Update.ExpressionAttributeValues),
				ReturnValuesOnConditionCheckFailure: item.Update.ReturnValuesOnConditionCheckFailure,
			}
		}
		transactItems[i] = transactItem
	}

	input := &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	}
	return input, nil
}

func EncodeTransactWriteItemsOutput(output *dynamodb.TransactWriteItemsOutput) ([]byte, error) {
	bs, err := json.Marshal(output)
	return bs, err
}

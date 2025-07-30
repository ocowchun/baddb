# baddb
A service that simulates DynamoDB with inconsistent reads, throughput limits, and partial batch get/write operations.

## Why
Based on the [AWS documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.UsageNotes.html#DynamoDBLocal.Differences), here are key limitations of DynamoDB Local that baddb can simulate for better testing:

####  Provisioned Throughput Testing
- DynamoDB Local: Ignores provisioned throughput settings, no throttling
- baddb: Simulates ProvisionedThroughputExceededException and throughput limits to verify system behavior under traffic that exceeds provisioned throughput.

#### Consistency Behavior
- DynamoDB Local: All reads are technically strongly consistent
- baddb: Configurable eventual consistency delays via metadata table  to verify system behavior when reading stale data.

#### Batch Operation Failures
- DynamoDB Local: Doesn't simulate partial batch failures
- baddb: Can simulate unprocessed items in batch operations to verify system behavior with partial results.


⚠️ Early Development Warning
baddb is currently in early development and should be used with caution.

While baddb is frequently tested against DynamoDB Local to ensure behavioral accuracy, it may still exhibit
differences from actual AWS DynamoDB behavior. Key considerations:

- Behavioral Differences: Despite extensive testing, some edge cases may behave differently than AWS DynamoDB
- Active Development: Behavior may change between versions
- Limited Coverage: Not all DynamoDB features are implemented (see below for current status)

Recommendation: Always validate your application against actual AWS DynamoDB before production deployment. Use
baddb primarily for controlled testing scenarios where you need to simulate specific failure conditions that
DynamoDB Local cannot provide.

Feedback Welcome: If you discover behavioral differences, please report them as issues to help improve accuracy.


## Install
```shell
go install github.com/ocowchun/baddb/cli/baddb
```

## Usage
```shell
# run baddb server on port 9527, if not specified, the default port is 9527
baddb --port 9527
```


### Configure Delay Time
```shell
aws dynamodb create-table \
    --table-name MusicCollection\
    --attribute-definitions AttributeName=Artist,AttributeType=S AttributeName=SongTitle,AttributeType=S \
    --key-schema AttributeName=Artist,KeyType=HASH AttributeName=SongTitle,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
    --endpoint-url http://localhost:9527


# configure delay time as 60 seconds
# tableDelaySeconds for get-item and query
# gsiDelaySeconds for query GSI
aws dynamodb put-item \
    --table-name baddb_table_metadata \
    --item '{"tableName": {"S": "MusicCollection"}, "tableDelaySeconds": {"N": "60"}, "gsiDelaySeconds": {"N": "60"}}' \
    --endpoint-url http://localhost:9527

aws dynamodb put-item \
    --table-name MusicCollection \
    --item '{"Artist": {"S": "the Jimi Hendrix Experience"}, "SongTitle": {"S": "Little Wing"}, "AlbumTitle": {"S": "Axis: Bold as Love"}}' \
    --endpoint-url http://localhost:9527

# see nothing if consistent-read is false and the command is called within 60 seconds after above command
aws dynamodb get-item \
    --table-name MusicCollection \
    --key '{"Artist": {"S": "the Jimi Hendrix Experience"}, "SongTitle": {"S": "Little Wing"}}' \
    --no-consistent-read \
    --endpoint-url http://localhost:9527

# see item immediately
aws dynamodb get-item \
    --table-name MusicCollection \
    --key '{"Artist": {"S": "No One You Know"}, "SongTitle": {"S": "Call Me Today"}}' \
    --consistent-read \
    --endpoint-url http://localhost:9527
```

### Configure unprocessed requests
```shell
aws dynamodb create-table \
    --table-name MusicCollection\
    --attribute-definitions AttributeName=Artist,AttributeType=S AttributeName=SongTitle,AttributeType=S \
    --key-schema AttributeName=Artist,KeyType=HASH AttributeName=SongTitle,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
    --endpoint-url http://localhost:9527


# the next 5 get/put/update/delete will failed
# if the batchGet/batchWrite has 6 requests, the first 5 request will return in unprocessed
# you also need to consider retry, do the math yourself
aws dynamodb put-item \
    --table-name baddb_table_metadata \
    --item '{"tableName": {"S": "MusicCollection"}, "unprocessedRequests": {"N": "5"}}' \
    --endpoint-url http://localhost:9527

#
aws dynamodb batch-write-item \
    --request-items '{ "MusicCollection": [{"PutRequest": {"Item": {"Artist": {"S": "the Jimi Hendrix Experience"}, "SongTitle": {"S": "Little Wing"}}}}] }' \
    --endpoint-url http://localhost:9527

```


## Not Supported
### Number type
baddb uses float64 to represent number, which is not compatible with DynamoDB's number type.
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypes.Number


## Support
### BatchGetItem
- [ ] AttributesToGet
- [x] ConsistentRead
- [ ] ProjectionExpression
- [ ] ReturnConsumedCapacity


### BatchWriteItem
- [x] DeleteRequest
- [x] PutRequest
- [ ] ReturnConsumedCapacity
- [ ] ReturnItemCollectionMetrics

### Create Table
- [x] AttributeDefinitions
- [x] BillingMode
- [ ] DeletionProtectionEnabled
- [x] GlobalSecondaryIndexes
- [x] KeySchema
- [ ] LocalSecondaryIndexes
- [ ] OnDemandThroughput
- [x] ProvisionedThroughput
- [ ] ResourcePolicy
- [ ] SSESpecification
- [ ] StreamSpecification
- [ ] TableClass
- [x] TableName
- [ ] Tags
- [ ] WarmThroughput

### DeleteItem
- [ ] ConditionalOperator
- [x] ConditionExpression
- [ ] Expected
- [x] ExpressionAttributeNames
- [x] ExpressionAttributeValues
- [x] Key
- [ ] ReturnConsumedCapacity
- [ ] ReturnItemCollectionMetrics
- [ ] ReturnValues
- [ ] ReturnValuesOnConditionCheckFailure
- [x] TableName

### DeleteTable
- [x] TableName

### DescribeTable
- [x] TableName

### GetItem
- [ ] AttributesToGet
- [x] ConsistentRead
- [ ] ExpressionAttributeNames
- [x] Keys
- [ ] ProjectionExpression
- [ ] ReturnConsumedCapacity
- [x] TableName

### ListTables
TBD

### PutItem
- [ ] ConditionalOperator
- [x] ConditionExpression
- [ ] Expected
- [x] ExpressionAttributeNames
- [x] ExpressionAttributeValues
- [x] Item
- [ ] ReturnConsumedCapacity
- [ ] ReturnItemCollectionMetrics
- [ ] ReturnValues
- [ ] ReturnValuesOnConditionCheckFailure
- [x] TableName

### Query
- [ ] AttributesToGet
- [ ] ConditionalOperator
- [x] ConsistentRead
- [x] ExclusiveStartKey
- [x] ExpressionAttributeNames
- [x] ExpressionAttributeValues
- [x] FilterExpression
- [x] IndexName
- [x] KeyConditionExpression
- [ ] KeyConditions
- [x] Limit
- [ ] ProjectionExpression
- [ ] QueryFilter
- [ ] ReturnConsumedCapacity
- [x] ScanIndexForward
- [ ] Select
- [x] TableName

### Scan
- [ ] AttributesToGet
- [ ] ConditionalOperator
- [x] ConsistentRead
- [x] ExclusiveStartKey
- [x] ExpressionAttributeNames
- [x] ExpressionAttributeValues
- [x] FilterExpression
- [x] IndexName
- [x] Limit
- [ ] ProjectionExpression
- [ ] ReturnConsumedCapacity
- [ ] ReturnConsumedCapacity
- [ ] ScanFilter
- [x] Segment
- [ ] Select
- [x] TableName
- [x] TotalSegments

### TransactGetItems
TBD


### TransactWriteItems
- [ ] ClientRequestToken
- [ ] ReturnConsumedCapacity
- [ ] ReturnItemCollectionMetrics
- [x] TransactItems

### UpdateTable
- [x] TableName
- [x] AttributeDefinitions
- [x] BillingMode
- [ ] DeletionProtectionEnabled
- [x] GlobalSecondaryIndexUpdates
- [ ] GlobalTableWitnessUpdates
- [ ] MultiRegionConsistency
- [ ] OnDemandThroughput
- [x] ProvisionedThroughput
- [ ] ReplicaUpdates
- [ ] SSESpecification
- [ ] StreamSpecification
- [ ] TableClass
- [ ] WarmThroughput

### UpdateTimeToLive
TBD

# This is a toy project and still in early development stage. Please do not use it in any serious stuff.

# baddb
A service that simulates DynamoDB with inconsistent reads, throughput limits, and partial batch get/write operations.

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

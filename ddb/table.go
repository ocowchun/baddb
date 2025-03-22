package ddb

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"time"
)

type KeySchema struct {
	AttributeName string
}

type TableMetaData struct {
	Name                         string
	AttributeDefinitions         []types.AttributeDefinition
	KeySchema                    []types.KeySchemaElement
	GlobalSecondaryIndexSettings []GlobalSecondaryIndexSetting
	LocalSecondaryIndexes        []types.LocalSecondaryIndex
	ProvisionedThroughput        *types.ProvisionedThroughput
	CreationDateTime             *time.Time
	PartitionKeySchema           *KeySchema
	SortKeySchema                *KeySchema
}

func (m *TableMetaData) Description() *types.TableDescription {
	tableDescription := &types.TableDescription{
		AttributeDefinitions: m.AttributeDefinitions,
		CreationDateTime:     m.CreationDateTime,

		TableName:   &m.Name,
		TableStatus: types.TableStatusActive,
	}

	return tableDescription
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

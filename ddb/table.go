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

func (m *TableMetaData) Description(itemCount int64) *types.TableDescription {
	tableSizeBytes := itemCount * 100
	keySchema := make([]types.KeySchemaElement, 0)

	keySchema = append(keySchema, types.KeySchemaElement{
		AttributeName: &m.PartitionKeySchema.AttributeName,
		KeyType:       types.KeyTypeHash,
	})

	if m.SortKeySchema != nil {
		keySchema = append(keySchema, types.KeySchemaElement{
			AttributeName: &m.SortKeySchema.AttributeName,
			KeyType:       types.KeyTypeRange,
		})
	}

	gsi := make([]types.GlobalSecondaryIndexDescription, 0)
	// TODO: implement GlobalSecondaryIndexDescription
	for _, setting := range m.GlobalSecondaryIndexSettings {
		keySchema := make([]types.KeySchemaElement, 0)
		keySchema = append(keySchema, types.KeySchemaElement{
			AttributeName: setting.PartitionKeyName,
			KeyType:       types.KeyTypeHash,
		})
		if setting.SortKeyName != nil {
			keySchema = append(keySchema, types.KeySchemaElement{
				AttributeName: setting.SortKeyName,
				KeyType:       types.KeyTypeRange,
			})
		}

		projectionType := types.ProjectionTypeAll
		switch setting.ProjectionType {
		case PROJECTION_TYPE_KEYS_ONLY:
			projectionType = types.ProjectionTypeKeysOnly
		case PROJECTION_TYPE_INCLUDE:
			projectionType = types.ProjectionTypeInclude
		case PROJECTION_TYPE_ALL:
			projectionType = types.ProjectionTypeAll
		}

		projection := types.Projection{
			NonKeyAttributes: setting.NonKeyAttributes,
			ProjectionType:   projectionType,
		}

		gsi = append(gsi, types.GlobalSecondaryIndexDescription{
			IndexName: setting.IndexName,
			KeySchema: keySchema,
			// TODO: fix it later, GSI item count might be different from the main table, but for now, we just use the same item count
			ItemCount:      &itemCount,
			IndexSizeBytes: &tableSizeBytes,
			Projection:     &projection,
		})
	}

	// TODO: adjust capacity units later
	readCapacityUnits := int64(0)
	writeCapacityUnits := int64(0)
	provisionedThroughput := &types.ProvisionedThroughputDescription{
		ReadCapacityUnits:  &readCapacityUnits,
		WriteCapacityUnits: &writeCapacityUnits,
	}

	tableDescription := &types.TableDescription{
		AttributeDefinitions:   m.AttributeDefinitions,
		CreationDateTime:       m.CreationDateTime,
		KeySchema:              keySchema,
		GlobalSecondaryIndexes: gsi,

		ProvisionedThroughput: provisionedThroughput,
		ItemCount:             &itemCount,
		TableName:             &m.Name,
		TableSizeBytes:        &tableSizeBytes,
		TableStatus:           types.TableStatusActive,
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

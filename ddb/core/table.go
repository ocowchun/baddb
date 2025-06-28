package core

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"time"
)

type KeySchema struct {
	AttributeName string
	AttributeType ScalarAttributeType
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
	BillingMode                  BillingMode
}

func (m *TableMetaData) FindKeySchema(attributeName string) *KeySchema {
	if m.PartitionKeySchema != nil && m.PartitionKeySchema.AttributeName == attributeName {
		return m.PartitionKeySchema
	}

	if m.SortKeySchema != nil && m.SortKeySchema.AttributeName == attributeName {
		return m.SortKeySchema
	}

	for _, index := range m.GlobalSecondaryIndexSettings {
		if index.PartitionKeySchema != nil && index.PartitionKeySchema.AttributeName == attributeName {
			return &KeySchema{
				AttributeName: index.PartitionKeySchema.AttributeName,
				AttributeType: index.PartitionKeySchema.AttributeType,
			}
		}
		if index.SortKeySchema != nil && index.SortKeySchema.AttributeName == attributeName {
			return &KeySchema{
				AttributeName: index.SortKeySchema.AttributeName,
				AttributeType: index.SortKeySchema.AttributeType,
			}
		}
	}
	return nil

}

func (m *TableMetaData) Clone() *TableMetaData {
	clone := &TableMetaData{
		Name:        m.Name,
		BillingMode: m.BillingMode,
	}

	if len(m.AttributeDefinitions) > 0 {
		clone.AttributeDefinitions = make([]types.AttributeDefinition, len(m.AttributeDefinitions))
		copy(clone.AttributeDefinitions, m.AttributeDefinitions)
	}

	if len(m.KeySchema) > 0 {
		clone.KeySchema = make([]types.KeySchemaElement, len(m.KeySchema))
		copy(clone.KeySchema, m.KeySchema)
	}

	if len(m.GlobalSecondaryIndexSettings) > 0 {
		clone.GlobalSecondaryIndexSettings = make([]GlobalSecondaryIndexSetting, len(m.GlobalSecondaryIndexSettings))
		for i, gsi := range m.GlobalSecondaryIndexSettings {
			clone.GlobalSecondaryIndexSettings[i] = GlobalSecondaryIndexSetting{
				ProjectionType: gsi.ProjectionType,
			}

			if gsi.IndexName != nil {
				indexName := *gsi.IndexName
				clone.GlobalSecondaryIndexSettings[i].IndexName = &indexName
			}

			if gsi.PartitionKeySchema != nil {
				clone.GlobalSecondaryIndexSettings[i].PartitionKeySchema = &KeySchema{
					AttributeName: gsi.PartitionKeySchema.AttributeName,
					AttributeType: gsi.PartitionKeySchema.AttributeType,
				}
			}

			if gsi.SortKeySchema != nil {
				clone.GlobalSecondaryIndexSettings[i].SortKeySchema = &KeySchema{
					AttributeName: gsi.SortKeySchema.AttributeName,
					AttributeType: gsi.SortKeySchema.AttributeType,
				}
			}

			if len(gsi.NonKeyAttributes) > 0 {
				clone.GlobalSecondaryIndexSettings[i].NonKeyAttributes = make([]string, len(gsi.NonKeyAttributes))
				copy(clone.GlobalSecondaryIndexSettings[i].NonKeyAttributes, gsi.NonKeyAttributes)
			}
		}
	}

	if len(m.LocalSecondaryIndexes) > 0 {
		clone.LocalSecondaryIndexes = make([]types.LocalSecondaryIndex, len(m.LocalSecondaryIndexes))
		copy(clone.LocalSecondaryIndexes, m.LocalSecondaryIndexes)
	}

	if m.ProvisionedThroughput != nil {
		clone.ProvisionedThroughput = &types.ProvisionedThroughput{
			ReadCapacityUnits:  m.ProvisionedThroughput.ReadCapacityUnits,
			WriteCapacityUnits: m.ProvisionedThroughput.WriteCapacityUnits,
		}
	}

	if m.CreationDateTime != nil {
		creationTime := *m.CreationDateTime
		clone.CreationDateTime = &creationTime
	}

	if m.PartitionKeySchema != nil {
		clone.PartitionKeySchema = &KeySchema{
			AttributeName: m.PartitionKeySchema.AttributeName,
			AttributeType: m.PartitionKeySchema.AttributeType,
		}
	}

	if m.SortKeySchema != nil {
		clone.SortKeySchema = &KeySchema{
			AttributeName: m.SortKeySchema.AttributeName,
			AttributeType: m.SortKeySchema.AttributeType,
		}
	}

	return clone
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
		gsiKeySchema := make([]types.KeySchemaElement, 0)
		gsiKeySchema = append(gsiKeySchema, types.KeySchemaElement{
			AttributeName: &setting.PartitionKeySchema.AttributeName,
			KeyType:       types.KeyTypeHash,
		})
		if setting.SortKeySchema != nil {
			gsiKeySchema = append(gsiKeySchema, types.KeySchemaElement{
				AttributeName: &setting.SortKeySchema.AttributeName,
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
			KeySchema: gsiKeySchema,
			// TODO: fix it later, GSI item count might be different from the main table, but for now, we just use the same item count
			ItemCount:      &itemCount,
			IndexSizeBytes: &tableSizeBytes,
			Projection:     &projection,
		})
	}

	readCapacityUnits := int64(0)
	writeCapacityUnits := int64(0)
	if m.ProvisionedThroughput != nil {
		readCapacityUnits = *m.ProvisionedThroughput.ReadCapacityUnits
		writeCapacityUnits = *m.ProvisionedThroughput.WriteCapacityUnits
	}
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

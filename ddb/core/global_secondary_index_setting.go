package core

type GlobalSecondaryIndexSetting struct {
	IndexName          *string
	PartitionKeySchema *KeySchema
	SortKeySchema      *KeySchema
	NonKeyAttributes   []string
	ProjectionType     ProjectionType
}

func (gsi GlobalSecondaryIndexSetting) PartitionKeyName() *string {
	return &gsi.PartitionKeySchema.AttributeName
}

func (gsi GlobalSecondaryIndexSetting) SortKeyName() *string {
	if gsi.SortKeySchema != nil {
		return &gsi.SortKeySchema.AttributeName
	}

	return nil
}

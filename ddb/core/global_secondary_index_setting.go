package core

type GlobalSecondaryIndexSetting struct {
	IndexName             *string
	PartitionKeySchema    *KeySchema
	SortKeySchema         *KeySchema
	NonKeyAttributes      []string
	ProjectionType        ProjectionType
	ProvisionedThroughput *ProvisionedThroughput
}

func (gsi GlobalSecondaryIndexSetting) PartitionKeyName() *string {
	return &gsi.PartitionKeySchema.AttributeName
}

// SortKeyName returns the name of the sort key if it exists, otherwise nil.
func (gsi GlobalSecondaryIndexSetting) SortKeyName() *string {
	if gsi.SortKeySchema != nil {
		return &gsi.SortKeySchema.AttributeName
	}

	return nil
}

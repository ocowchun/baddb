package core

type GlobalSecondaryIndexSetting struct {
	IndexName        *string
	PartitionKeyName *string
	SortKeyName      *string
	NonKeyAttributes []string
	ProjectionType   ProjectionType
}

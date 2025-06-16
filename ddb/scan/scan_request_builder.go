package scan

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ocowchun/baddb/ddb/condition"
	"github.com/ocowchun/baddb/ddb/core"
)

type RequestBuilder struct {
	FilterExpressionStr       *string
	ExpressionAttributeValues map[string]core.AttributeValue
	ExpressionAttributeNames  map[string]string
	TableMetadata             *core.TableMetaData
	ExclusiveStartKey         map[string]types.AttributeValue
	ConsistentRead            *bool
	Limit                     *int32
	IndexName                 *string
	Segment                   *int32
	TotalSegments             *int32
}

type Request struct {
	ConsistentRead    bool
	ExclusiveStartKey *[]byte
	Limit             int
	TableName         string
	IndexName         *string
	Filter            *condition.Condition
	Segment           *int32
	TotalSegments     *int32
}

type InvalidFilterExpressionError struct {
	rawErr error
}

func (e *InvalidFilterExpressionError) Error() string {
	return fmt.Sprintf("Invalid FilterExpression: %v", e.rawErr)
}

func (b *RequestBuilder) Build() (*Request, error) {
	req := &Request{
		ConsistentRead: b.ConsistentRead != nil && *b.ConsistentRead,
		TableName:      b.TableMetadata.Name,
		IndexName:      b.IndexName,
		Segment:        b.Segment,
		TotalSegments:  b.TotalSegments,
	}
	if req.ConsistentRead && req.IndexName != nil {
		return nil, fmt.Errorf("ConsistentRead cannot be true when IndexName is set")
	}

	if b.Limit != nil {
		req.Limit = int(*b.Limit)
	} else {
		req.Limit = 100
	}
	if b.FilterExpressionStr != nil {
		filter, err := condition.BuildCondition(
			*b.FilterExpressionStr,
			b.ExpressionAttributeNames,
			b.ExpressionAttributeValues,
		)
		if err != nil {
			return nil, &InvalidFilterExpressionError{rawErr: err}
		}
		req.Filter = filter
	}

	if len(b.ExclusiveStartKey) > 0 {
		bs := make([]byte, 0)
		tablePartitionKey := b.TableMetadata.PartitionKeySchema.AttributeName
		if val, ok := b.ExclusiveStartKey[tablePartitionKey]; ok {
			attrVal, err := core.TransformDdbAttributeValue(val)
			if err != nil {
				return nil, err
			}
			bs = attrVal.Bytes()
		} else {
			return nil, fmt.Errorf("partition key %s not found in ExclusiveStartKey", tablePartitionKey)
		}

		if b.TableMetadata.SortKeySchema != nil {
			tableSortKey := b.TableMetadata.SortKeySchema.AttributeName
			if val, ok := b.ExclusiveStartKey[tableSortKey]; ok {
				attrVal, err := core.TransformDdbAttributeValue(val)
				if err != nil {
					return nil, err
				}

				bs = append(bs, []byte("|")...)
				bs = append(bs, attrVal.Bytes()...)
			} else {
				return nil, fmt.Errorf("sort key %s not found in ExclusiveStartKey", tableSortKey)
			}
		}
		req.ExclusiveStartKey = &bs
	}

	return req, nil
}

package request

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ocowchun/baddb/ddb/condition"
	"github.com/ocowchun/baddb/ddb/core"
	"github.com/ocowchun/baddb/ddb/storage"
)

type PutRequestBuilder struct {
	ConditionExpression       *string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]types.AttributeValue
	Item                      map[string]types.AttributeValue
	TableName                 *string
}

func (b *PutRequestBuilder) Build() (*storage.PutRequest, error) {
	if b.TableName == nil {
		return nil, fmt.Errorf("TableName is required")
	}
	tableName := *b.TableName

	var cond *condition.Condition
	entry, err := core.NewEntryFromItem(b.Item)
	if err != nil {
		return nil, err
	}

	if b.ConditionExpression != nil {
		attrVals, err := core.TransformAttributeValueMap(b.ExpressionAttributeValues)
		if err != nil {
			return nil, err
		}
		cond, err = condition.BuildCondition(
			*b.ConditionExpression,
			b.ExpressionAttributeNames,
			attrVals,
		)
		if err != nil {
			return nil, &core.InvalidConditionExpressionError{
				RawErr: err,
			}
		}
	}

	req := &storage.PutRequest{
		Entry:     entry,
		TableName: tableName,
		Condition: cond,
	}

	return req, nil
}

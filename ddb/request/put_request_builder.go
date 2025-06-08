package request

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ocowchun/baddb/ddb/condition"
	"github.com/ocowchun/baddb/ddb/core"
	"github.com/ocowchun/baddb/ddb/inner_storage"
)

type PutRequestBuilder struct {
	ConditionExpression       *string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]types.AttributeValue
	Item                      map[string]types.AttributeValue
	TableName                 *string
}

func (b *PutRequestBuilder) Build() (*inner_storage.PutRequest, error) {
	if b.TableName == nil {
		return nil, fmt.Errorf("TableName is required")
	}
	tableName := *b.TableName

	var cond *condition.Condition
	var err error
	if b.ConditionExpression != nil {
		cond, err = condition.BuildCondition(
			*b.ConditionExpression,
			b.ExpressionAttributeNames,
			core.NewEntryFromItem(b.ExpressionAttributeValues).Body,
		)
		if err != nil {
			return nil, &core.InvalidConditionExpressionError{
				RawErr: err,
			}
		}
	}
	entry := core.NewEntryFromItem(b.Item)

	req := &inner_storage.PutRequest{
		Entry:     entry,
		TableName: tableName,
		Condition: cond,
	}

	return req, nil
}

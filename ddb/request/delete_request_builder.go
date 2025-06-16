package request

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ocowchun/baddb/ddb/condition"
	"github.com/ocowchun/baddb/ddb/core"
	"github.com/ocowchun/baddb/ddb/inner_storage"
)

type DeleteRequestBuilder struct {
	TableName                 *string
	ConditionExpression       *string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]types.AttributeValue
	Key                       map[string]types.AttributeValue
}

func (b *DeleteRequestBuilder) Build() (*inner_storage.DeleteRequest, error) {
	if b.TableName == nil {
		return nil, fmt.Errorf("TableName is required")
	}
	tableName := *b.TableName

	entry, err := core.NewEntryFromItem(b.Key)
	if err != nil {
		return nil, err
	}

	var cond *condition.Condition
	if b.ConditionExpression != nil {
		cond, err = condition.BuildCondition(
			*b.ConditionExpression,
			b.ExpressionAttributeNames,
			entry.Body,
		)
		if err != nil {
			return nil, &core.InvalidConditionExpressionError{
				RawErr: err,
			}
		}
	}

	req := &inner_storage.DeleteRequest{
		Entry:     entry,
		TableName: tableName,
		Condition: cond,
	}

	return req, nil
}

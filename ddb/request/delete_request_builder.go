package request

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	condition2 "github.com/ocowchun/baddb/ddb/condition"
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

	var condition *condition2.Condition
	var err error
	if b.ConditionExpression != nil {
		condition, err = condition2.BuildCondition(
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

	entry := core.NewEntryFromItem(b.Key)
	req := &inner_storage.DeleteRequest{
		Entry:     entry,
		TableName: tableName,
		Condition: condition,
	}

	return req, nil
}

package request

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ocowchun/baddb/ddb/condition"
	"github.com/ocowchun/baddb/ddb/core"
	"github.com/ocowchun/baddb/ddb/storage"
	"github.com/ocowchun/baddb/ddb/update"
)

type UpdateRequestBuilder struct {
	TableName                 *string
	UpdateExpression          *string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]types.AttributeValue
	ConditionExpression       *string
	Key                       map[string]types.AttributeValue
}

func (b *UpdateRequestBuilder) Build() (*storage.UpdateRequest, error) {
	if b.TableName == nil {
		return nil, fmt.Errorf("TableName is required")
	}
	tableName := *b.TableName

	if b.UpdateExpression == nil {
		msg := "UpdateExpression must be provided"
		return nil, fmt.Errorf(msg)
	}

	exprVals, err := core.NewEntryFromItem(b.ExpressionAttributeValues)
	if err != nil {
		return nil, err
	}

	updateOperation, err := update.BuildUpdateOperation(
		*b.UpdateExpression,
		b.ExpressionAttributeNames,
		exprVals.Body,
	)
	if err != nil {
		return nil, err
	}

	var cond *condition.Condition
	if b.ConditionExpression != nil {
		cond, err = condition.BuildCondition(
			*b.ConditionExpression,
			b.ExpressionAttributeNames,
			exprVals.Body,
		)
		if err != nil {
			return nil, &core.InvalidConditionExpressionError{
				RawErr: err,
			}
		}
	}

	key, err := core.NewEntryFromItem(b.Key)
	if err != nil {
		return nil, err
	}

	req := &storage.UpdateRequest{
		Key:             key,
		UpdateOperation: updateOperation,
		TableName:       tableName,
		Condition:       cond,
	}
	return req, nil
}

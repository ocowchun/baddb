package request

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ocowchun/baddb/ddb/condition"
	"github.com/ocowchun/baddb/ddb/core"
	"github.com/ocowchun/baddb/ddb/inner_storage"
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

func (b *UpdateRequestBuilder) Build() (*inner_storage.UpdateRequest, error) {
	if b.TableName == nil {
		return nil, fmt.Errorf("TableName is required")
	}
	tableName := *b.TableName

	if b.UpdateExpression == nil {
		msg := "UpdateExpression must be provided"
		return nil, fmt.Errorf(msg)
	}

	updateOperation, err := update.BuildUpdateOperation(
		*b.UpdateExpression,
		b.ExpressionAttributeNames,
		core.NewEntryFromItem(b.ExpressionAttributeValues).Body)
	if err != nil {
		return nil, err
	}

	var cond *condition.Condition
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

	req := &inner_storage.UpdateRequest{
		Key:             core.NewEntryFromItem(b.Key),
		UpdateOperation: updateOperation,
		TableName:       tableName,
		Condition:       cond,
	}
	return req, nil
}

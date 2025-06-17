package request

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/ocowchun/baddb/ddb/core"
	"github.com/ocowchun/baddb/ddb/inner_storage"
)

type GetRequestBuilder struct {
	Input         *dynamodb.GetItemInput
	TableMetaData *core.TableMetaData
}

func (b *GetRequestBuilder) Build() (*inner_storage.GetRequest, error) {

	consistentRead := false
	if b.Input.ConsistentRead != nil {
		consistentRead = *b.Input.ConsistentRead
	}

	key, err := core.NewEntryFromItem(b.Input.Key)
	if err != nil {
		return nil, err
	}
	if pkValue, ok := key.Body[b.TableMetaData.PartitionKeySchema.AttributeName]; ok {
		if !pkValue.IsScalarAttributeType(b.TableMetaData.PartitionKeySchema.AttributeType) {
			return nil, fmt.Errorf("One or more parameter values were invalid: Type mismatch for key")
		}
	} else {
		return nil, fmt.Errorf("One of the required keys was not given a value")
	}
	if b.TableMetaData.SortKeySchema != nil {
		if skValue, ok := key.Body[b.TableMetaData.SortKeySchema.AttributeName]; ok {
			if !skValue.IsScalarAttributeType(b.TableMetaData.SortKeySchema.AttributeType) {
				return nil, fmt.Errorf("One or more parameter values were invalid: Type mismatch for key")
			}
		} else {
			return nil, fmt.Errorf("One of the required keys was not given a value")
		}
	}

	req := &inner_storage.GetRequest{
		Entry:          key,
		ConsistentRead: consistentRead,
		TableName:      b.TableMetaData.Name,
	}
	return req, nil
}

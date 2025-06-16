package query

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ocowchun/baddb/ddb/condition"
	"github.com/ocowchun/baddb/ddb/core"
	"github.com/ocowchun/baddb/expression/ast"
	"log"
)

type QueryBuilder struct {
	KeyConditionExpression    *ast.KeyConditionExpression
	ExpressionAttributeValues map[string]core.AttributeValue
	ExpressionAttributeNames  map[string]string
	FilterExpressionStr       *string
	TableMetadata             *core.TableMetaData
	ExclusiveStartKey         map[string]types.AttributeValue
	ConsistentRead            *bool
	Limit                     *int32
	IndexName                 *string
	ScanIndexForward          *bool
}

func (b *QueryBuilder) getKeyName(predicate ast.PredicateExpression) (string, error) {
	var attributeName ast.AttributeName
	switch predicate.PredicateType() {
	case ast.SIMPLE:
		pred, ok := predicate.(*ast.SimplePredicateExpression)
		if !ok {
			return "", fmt.Errorf("failed to cast to SimplePredicateExpression")
		}
		attributeName = pred.AttributeName
	case ast.BETWEEN:
		pred, ok := predicate.(*ast.BetweenPredicateExpression)
		if !ok {
			return "", fmt.Errorf("failed to cast to BetweenPredicateExpression")
		}

		attributeName = pred.AttributeName
	case ast.BEGINS_WITH:
		pred, ok := predicate.(*ast.BeginsWithPredicateExpression)
		if !ok {
			return "", fmt.Errorf("failed to cast to BetweenPredicateExpression")
		}
		attributeName = pred.AttributeName
	}

	key, err := b.extractAttributeName(attributeName)
	if err != nil {
		return "", err
	}
	return key, nil
}

type Query struct {
	PartitionKey      *[]byte
	SortKeyPredicate  *Predicate
	ConsistentRead    bool
	ExclusiveStartKey *[]byte
	Limit             int
	ScanIndexForward  bool
	TableName         string
	IndexName         *string
	Filter            *condition.Condition
}

func (b *QueryBuilder) BuildQuery() (*Query, error) {
	query := &Query{
		ScanIndexForward: true,
		IndexName:        b.IndexName,
	}
	if b.ConsistentRead != nil {
		query.ConsistentRead = *b.ConsistentRead
	}
	if b.ScanIndexForward != nil {
		query.ScanIndexForward = *b.ScanIndexForward
	}

	if b.Limit != nil && *b.Limit > 0 && *b.Limit < 100 {
		query.Limit = int(*b.Limit)
	} else {
		query.Limit = 100
	}

	predicateExpressions := []ast.PredicateExpression{
		b.KeyConditionExpression.Predicate1,
	}
	if b.KeyConditionExpression.Predicate2 != nil {
		predicateExpressions = append(predicateExpressions, b.KeyConditionExpression.Predicate2)
	}
	for _, expression := range predicateExpressions {
		keyName, err := b.getKeyName(expression)
		if err != nil {
			return nil, err
		}

		if keyName == *b.expectedPartitionKey() {
			prefix, err := b.extractPartitionKeyPrefix(b.KeyConditionExpression.Predicate1)
			if err != nil {
				return nil, err
			}
			query.PartitionKey = &prefix
		} else if keyName == *b.expectedSortKey() {
			predicate, err := b.EvaluatePredicateExpression(expression)
			if err != nil {
				return nil, err
			}
			query.SortKeyPredicate = &predicate
		} else {
			return nil, fmt.Errorf("KeyConditionExpression only support PartitionKey and sortKey, but got %s", keyName)
		}
	}

	if query.PartitionKey == nil {
		return nil, fmt.Errorf("Query condition missed key schema element")
	}

	if b.FilterExpressionStr != nil {
		filter, err := condition.BuildCondition(
			*b.FilterExpressionStr,
			b.ExpressionAttributeNames,
			b.ExpressionAttributeValues,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to build filter expression: %v", err)
		}
		query.Filter = filter
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
		query.ExclusiveStartKey = &bs
	}

	return query, nil
}

func (b *QueryBuilder) extractPartitionKeyPrefix(expression ast.PredicateExpression) ([]byte, error) {
	if expression.PredicateType() == ast.SIMPLE {
		pred, ok := expression.(*ast.SimplePredicateExpression)
		if !ok {
			return nil, fmt.Errorf("failed to cast to SimplePredicateExpression")
		}
		key, err := b.extractAttributeName(pred.AttributeName)
		if err != nil {
			return nil, fmt.Errorf("failed to cast to SimplePredicateExpression")
		}
		if pred.Operator != "=" {
			return nil, fmt.Errorf("Query key condition not supported")
		}

		if key == *b.expectedPartitionKey() {
			val, err := b.extractAttributeValue(pred.Value)
			if err != nil {
				return nil, fmt.Errorf("failed to extract attribute value")
			}
			keySchema := b.TableMetadata.FindKeySchema(key)
			if keySchema == nil || !val.IsScalarAttributeType(keySchema.AttributeType) {
				return nil, fmt.Errorf("One or more parameter values were invalid: Condition parameter type does not match schema type")
			}

			return val.Bytes(), nil
		}
	}
	return nil, fmt.Errorf("failed to extract PartitionKey PartitionKey")
}

type Predicate func(entry *core.Entry) (bool, error)

func (b *QueryBuilder) extractAttributeName(attributeName ast.AttributeName) (string, error) {
	a, ok := attributeName.(*ast.AttributeNameIdentifier)
	if !ok {
		return attributeName.String(), nil
	}

	key, ok := b.ExpressionAttributeNames[a.String()]
	if !ok {
		return "", fmt.Errorf("attribute name not found: %s", a.String())
	}

	return key, nil
}

func (b *QueryBuilder) extractAttributeValue(identifier *ast.AttributeValueIdentifier) (*core.AttributeValue, error) {
	key := identifier.String()
	val, ok := b.ExpressionAttributeValues[key]
	if !ok {
		return nil, fmt.Errorf("attribute %s not found", key)
	}

	return &val, nil
}

func (b *QueryBuilder) expectedPartitionKey() *string {
	if b.IndexName != nil {
		for _, gsi := range b.TableMetadata.GlobalSecondaryIndexSettings {
			if *gsi.IndexName == *b.IndexName {
				return gsi.PartitionKeyName()
			}
		}
		log.Fatalf("index %s not found", *b.IndexName)
	}
	return &b.TableMetadata.PartitionKeySchema.AttributeName
}

func (b *QueryBuilder) expectedSortKey() *string {
	if b.IndexName != nil {
		for _, gsi := range b.TableMetadata.GlobalSecondaryIndexSettings {
			if *gsi.IndexName == *b.IndexName {
				return gsi.SortKeyName()
			}
		}
		log.Fatalf("index %s not found", *b.IndexName)
	}

	if b.TableMetadata.SortKeySchema != nil {
		return &b.TableMetadata.SortKeySchema.AttributeName
	}
	return nil
}

func (b *QueryBuilder) EvaluatePredicateExpression(expression ast.PredicateExpression) (Predicate, error) {
	// TODO: check partition key only use eq predicate
	switch expression.PredicateType() {
	case ast.SIMPLE:
		pred, ok := expression.(*ast.SimplePredicateExpression)
		if !ok {
			panic("failed to cast as SimplePredicateExpression")
		}

		key, err := b.extractAttributeName(pred.AttributeName)
		if err != nil {
			return nil, err
		}
		isPartitionKey := key == *b.expectedPartitionKey()
		isSortKey := b.expectedSortKey() != nil && key == *b.expectedSortKey()
		if !isPartitionKey && !isSortKey {
			return nil, fmt.Errorf("key %s is not partition nor sort key", key)
		}

		otherVal, err := b.extractAttributeValue(pred.Value)
		if err != nil {
			return nil, err
		}

		if isPartitionKey && pred.Operator != "=" {
			return nil, fmt.Errorf("partition key only support = operator")
		}

		switch pred.Operator {
		case "=":
			return func(entry *core.Entry) (bool, error) {
				val, ok := entry.Body[key]
				if !ok {
					return false, fmt.Errorf("key %s not found", key)
				}

				return val.Equal(*otherVal), nil
			}, nil
		case "<":
			return func(entry *core.Entry) (bool, error) {
				val, ok := entry.Body[key]
				if !ok {
					return false, fmt.Errorf("key %s not found", key)
				}

				res, err := val.Compare(*otherVal)
				if err != nil {
					return false, err
				}
				return res == -1, nil
			}, nil

		case "<=":
			return func(entry *core.Entry) (bool, error) {
				val, ok := entry.Body[key]
				if !ok {
					return false, fmt.Errorf("key %s not found", key)
				}

				res, err := val.Compare(*otherVal)
				if err != nil {
					return false, err
				}
				return res <= 0, nil
			}, nil

		case ">":
			return func(entry *core.Entry) (bool, error) {
				val, ok := entry.Body[key]
				if !ok {
					return false, fmt.Errorf("key %s not found", key)
				}

				res, err := val.Compare(*otherVal)
				if err != nil {
					return false, err
				}
				return res > 0, nil
			}, nil
		case ">=":
			return func(entry *core.Entry) (bool, error) {
				val, ok := entry.Body[key]
				if !ok {
					return false, fmt.Errorf("key %s not found", key)
				}

				res, err := val.Compare(*otherVal)
				if err != nil {
					return false, err
				}
				return res >= 0, nil
			}, nil
		}
		return nil, fmt.Errorf("predicate op %s not found", pred.Operator)
	case ast.BETWEEN:
		pred, ok := expression.(*ast.BetweenPredicateExpression)

		if !ok {
			panic("failed to cast as SimplePredicateExpression")
		}

		key, err := b.extractAttributeName(pred.AttributeName)
		if err != nil {
			return nil, err
		}

		isSortKey := b.expectedSortKey() != nil && key == *b.expectedSortKey()
		if !isSortKey {
			return nil, fmt.Errorf("only sort key support between predicate expression")
		}

		leftVal, err := b.extractAttributeValue(pred.LeftValue)
		if err != nil {
			return nil, err
		}

		rightVal, err := b.extractAttributeValue(pred.RightValue)
		if err != nil {
			return nil, err
		}

		//pred.
		return func(entry *core.Entry) (bool, error) {
			val, ok := entry.Body[key]
			if !ok {
				return false, fmt.Errorf("key %s not found", key)
			}
			res, err := val.Compare(*leftVal)
			if err != nil {
				return false, err
			}
			if res == -1 {
				return false, nil
			}

			res, err = val.Compare(*rightVal)
			if err != nil {
				return false, err
			}
			return res <= 0, nil
		}, nil

	case ast.BEGINS_WITH:
		pred, ok := expression.(*ast.BeginsWithPredicateExpression)

		if !ok {
			panic("failed to cast as BeginsWithPredicateExpression")
		}

		key, err := b.extractAttributeName(pred.AttributeName)
		if err != nil {
			return nil, err
		}

		isSortKey := b.expectedSortKey() != nil && key == *b.expectedSortKey()
		if !isSortKey {
			return nil, fmt.Errorf("only sort key support begins_with predicate expression")
		}

		prefixVal, err := b.extractAttributeValue(pred.Value)
		if err != nil {
			return nil, err
		}
		if prefixVal.S == nil {
			return nil, fmt.Errorf("begins_with predicate value must be a string")
		}
		prefix := *prefixVal.S

		return func(entry *core.Entry) (bool, error) {
			val, ok := entry.Body[key]
			if !ok {
				return false, fmt.Errorf("key %s not found", key)
			}

			return val.BeginsWith(prefix)
		}, nil
	}

	panic("unreachable")
}

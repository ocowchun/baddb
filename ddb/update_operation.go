package ddb

import (
	"fmt"
	"github.com/ocowchun/baddb/expression"
	"github.com/ocowchun/baddb/expression/ast"
	"strconv"
)

type UpdateOperation struct {
	//entry                     *Entry
	updateExpression          *ast.UpdateExpression
	expressionAttributeNames  map[string]string
	expressionAttributeValues map[string]AttributeValue
}

func BuildUpdateOperation(
	//entry *Entry,
	updateExpressionContent string,
	expressionAttributeNames map[string]string,
	expressionAttributeValues map[string]AttributeValue,
) (*UpdateOperation, error) {
	updateExpression, err := expression.ParseUpdateExpression(updateExpressionContent)
	if err != nil {
		return nil, err
	}
	op := &UpdateOperation{
		//entry:                     entry,
		updateExpression:          updateExpression,
		expressionAttributeNames:  expressionAttributeNames,
		expressionAttributeValues: expressionAttributeValues,
	}

	return op, nil
}

func (o *UpdateOperation) Perform(entry *Entry) error {
	if o.updateExpression.Set != nil {
		err := o.performSetClause(entry)
		if err != nil {
			return err
		}
	}

	if o.updateExpression.Remove != nil {
		err := o.performRemoveClause(entry)
		if err != nil {
			return err
		}
	}

	if o.updateExpression.Add != nil {
		err := o.performAddClause(entry)
		if err != nil {
			return err
		}
	}

	if o.updateExpression.Delete != nil {
		err := o.performDeleteClause(entry)
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *UpdateOperation) performSetClause(entry *Entry) error {
	for _, action := range o.updateExpression.Set.Actions {
		path, err := o.buildPath(action.Path)

		if err != nil {
			return err
		}

		val, err := o.extractValue(entry, action.Value)
		if err != nil {
			return err
		}

		err = entry.Set(path, val)
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *UpdateOperation) performRemoveClause(entry *Entry) error {
	for _, action := range o.updateExpression.Remove.Paths {
		path, err := o.buildPath(action)
		if err != nil {
			return err
		}

		err = entry.Remove(path)
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *UpdateOperation) performAddClause(entry *Entry) error {
	// The ADD action only supports Number and set data types. In addition, ADD can only be used on top-level attributes, not nested attributes.
	for _, action := range o.updateExpression.Add.Actions {
		path, err := o.buildPath(action.Path)
		if err != nil {
			return err
		}

		attributeName, ok := path.(*AttributeNameOperand)
		if !ok {
			return fmt.Errorf("Invalid UpdateExpression: Syntax error; token: \"%s\"", attributeName.String())
		}

		val, err := o.extractValue(entry, action.Value)
		if err != nil {
			return err
		}

		err = entry.Add(attributeName, val)
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *UpdateOperation) performDeleteClause(entry *Entry) error {
	//The DELETE action supports only Set data types.
	for _, action := range o.updateExpression.Delete.Actions {
		path, err := o.buildPath(action.Path)
		if err != nil {
			return err
		}

		attributeName, ok := path.(*AttributeNameOperand)
		if !ok {
			return fmt.Errorf("Invalid UpdateExpression: Syntax error; token: \"%s\"", attributeName.String())
		}

		val, err := o.extractValue(entry, action.Subset)
		if err != nil {
			return err
		}

		err = entry.Delete(attributeName, val)
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *UpdateOperation) extractValue(entry *Entry, v ast.SetActionValue) (AttributeValue, error) {
	switch v := v.(type) {
	case *ast.AttributeNameOperand:
		if v.HasColon != false {
			val, ok := o.expressionAttributeValues[v.String()]
			if !ok {
				return AttributeValue{}, fmt.Errorf("attribute value not found: %s", v.String())
			}
			return val, nil
		}

		path, err := o.buildPath(v)
		if err != nil {
			return AttributeValue{}, err
		}

		return getValueFromPath(entry.Body, path)
	case *ast.DotOperand:
		path, err := o.buildPath(v)
		if err != nil {
			return AttributeValue{}, err
		}

		return getValueFromPath(entry.Body, path)

	case *ast.IndexOperand:
		path, err := o.buildPath(v)
		if err != nil {
			return AttributeValue{}, err
		}

		return getValueFromPath(entry.Body, path)
	case *ast.IfNotExistsExpression:
		left, err := o.extractValue(entry, v.Path)
		// TODO: check err is pathNotFound
		if err != nil {
			return o.extractValue(entry, v.Value)
		}
		return left, nil
	case *ast.ListAppendExpression:
		target, err := o.extractValue(entry, v.Target)
		if err != nil {
			return AttributeValue{}, err
		}
		if target.L == nil {
			return AttributeValue{}, fmt.Errorf("target must be list")
		}

		source, err := o.extractValue(entry, v.Source)
		if err != nil {
			return AttributeValue{}, err
		}
		if source.L == nil {
			return AttributeValue{}, fmt.Errorf("source must be list")
		}

		newList := append(*target.L, *source.L...)
		return AttributeValue{
			L: &newList,
		}, nil
	case *ast.SetActionInfixExpression:
		left, err := o.extractValue(entry, v.Left)
		if err != nil {
			return AttributeValue{}, err
		}

		right, err := o.extractValue(entry, v.Right)
		if err != nil {
			return AttributeValue{}, err
		}
		if left.N == nil || right.N == nil {
			return AttributeValue{}, fmt.Errorf("left and right operand must be number")
		}

		numLeft, err := strconv.ParseFloat(*left.N, 64)
		if err != nil {
			return AttributeValue{}, err
		}

		numRight, err := strconv.ParseFloat(*right.N, 64)
		if err != nil {
			return AttributeValue{}, err
		}

		if v.Operator == "-" {
			val := fmt.Sprintf("%v", numLeft-numRight)
			return AttributeValue{
				N: &val,
			}, nil
		} else if v.Operator == "+" {
			val := fmt.Sprintf("%v", numLeft+numRight)
			return AttributeValue{
				N: &val,
			}, nil
		} else {
			return AttributeValue{}, fmt.Errorf("unsupported operator: %s", v.Operator)
		}

	default:
		return AttributeValue{}, fmt.Errorf("unsupported operand type: %T", v)
	}
}

// TODO: refactor this
func (o *UpdateOperation) buildPath(operand ast.PathOperand) (PathOperand, error) {
	switch operand := operand.(type) {
	case *ast.AttributeNameOperand:
		if operand.HasSharp {
			key := "#" + operand.Identifier.TokenLiteral()
			name, ok := o.expressionAttributeNames[key]
			if !ok {
				msg := fmt.Sprintf("An expression attribute name used in the document path is not defined; attribute name: %s", key)
				return nil, fmt.Errorf(msg)
			}
			return &AttributeNameOperand{
				Name: name,
			}, nil
		} else if operand.HasColon {
			return nil, fmt.Errorf("path contains attribute value: %s", operand.Identifier.TokenLiteral())
		} else {
			name := operand.Identifier.TokenLiteral()
			return &AttributeNameOperand{
				Name: name,
			}, nil
		}
	case *ast.IndexOperand:
		left, err := o.buildPath(operand.Left)
		if err != nil {
			return nil, err
		}

		return &IndexOperand{
			Left:  left,
			Index: operand.Index,
		}, nil

	case *ast.DotOperand:
		left, err := o.buildPath(operand.Left)
		if err != nil {
			return nil, err
		}
		right, err := o.buildPath(operand.Right)
		if err != nil {
			return nil, err
		}
		return &DotOperand{
			Left:  left,
			Right: right,
		}, nil
	default:
		return nil, fmt.Errorf("unknown operand type: %T", operand)
	}
}

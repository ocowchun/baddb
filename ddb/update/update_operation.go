package update

import (
	"fmt"
	"github.com/ocowchun/baddb/ddb/core"
	"github.com/ocowchun/baddb/expression"
	"github.com/ocowchun/baddb/expression/ast"
	"strconv"
)

type UpdateOperation struct {
	updateExpression          *ast.UpdateExpression
	expressionAttributeNames  map[string]string
	expressionAttributeValues map[string]core.AttributeValue
}

func BuildUpdateOperation(
	updateExpressionContent string,
	expressionAttributeNames map[string]string,
	expressionAttributeValues map[string]core.AttributeValue,
) (*UpdateOperation, error) {
	updateExpression, err := expression.ParseUpdateExpression(updateExpressionContent)
	if err != nil {
		return nil, err
	}
	op := &UpdateOperation{
		updateExpression:          updateExpression,
		expressionAttributeNames:  expressionAttributeNames,
		expressionAttributeValues: expressionAttributeValues,
	}

	return op, nil
}

func (o *UpdateOperation) Perform(entry *core.Entry) error {
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

func (o *UpdateOperation) performSetClause(entry *core.Entry) error {
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

func (o *UpdateOperation) performRemoveClause(entry *core.Entry) error {
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

func (o *UpdateOperation) performAddClause(entry *core.Entry) error {
	// The ADD action only supports Number and set data types. In addition, ADD can only be used on top-level attributes, not nested attributes.
	for _, action := range o.updateExpression.Add.Actions {
		path, err := o.buildPath(action.Path)
		if err != nil {
			return err
		}

		attributeName, ok := path.(*core.AttributeNameOperand)
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

func (o *UpdateOperation) performDeleteClause(entry *core.Entry) error {
	//The DELETE action supports only Set data types.
	for _, action := range o.updateExpression.Delete.Actions {
		path, err := o.buildPath(action.Path)
		if err != nil {
			return err
		}

		attributeName, ok := path.(*core.AttributeNameOperand)
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

func (o *UpdateOperation) extractValue(entry *core.Entry, v ast.SetActionValue) (core.AttributeValue, error) {
	switch v := v.(type) {
	case *ast.AttributeNameOperand:
		if v.HasColon != false {
			val, ok := o.expressionAttributeValues[v.String()]
			if !ok {
				return core.AttributeValue{}, fmt.Errorf("attribute value not found: %s", v.String())
			}
			return val, nil
		}

		path, err := o.buildPath(v)
		if err != nil {
			return core.AttributeValue{}, err
		}

		return entry.Get(path)
	case *ast.DotOperand:
		path, err := o.buildPath(v)
		if err != nil {
			return core.AttributeValue{}, err
		}

		return entry.Get(path)
	case *ast.IndexOperand:
		path, err := o.buildPath(v)
		if err != nil {
			return core.AttributeValue{}, err
		}

		return entry.Get(path)
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
			return core.AttributeValue{}, err
		}
		if target.L == nil {
			return core.AttributeValue{}, fmt.Errorf("target must be list")
		}

		source, err := o.extractValue(entry, v.Source)
		if err != nil {
			return core.AttributeValue{}, err
		}
		if source.L == nil {
			return core.AttributeValue{}, fmt.Errorf("source must be list")
		}

		newList := append(*target.L, *source.L...)
		return core.AttributeValue{
			L: &newList,
		}, nil
	case *ast.SetActionInfixExpression:
		left, err := o.extractValue(entry, v.Left)
		if err != nil {
			return core.AttributeValue{}, err
		}

		right, err := o.extractValue(entry, v.Right)
		if err != nil {
			return core.AttributeValue{}, err
		}
		if left.N == nil || right.N == nil {
			return core.AttributeValue{}, fmt.Errorf("left and right operand must be number")
		}

		numLeft, err := strconv.ParseFloat(*left.N, 64)
		if err != nil {
			return core.AttributeValue{}, err
		}

		numRight, err := strconv.ParseFloat(*right.N, 64)
		if err != nil {
			return core.AttributeValue{}, err
		}

		if v.Operator == "-" {
			val := fmt.Sprintf("%v", numLeft-numRight)
			return core.AttributeValue{
				N: &val,
			}, nil
		} else if v.Operator == "+" {
			val := fmt.Sprintf("%v", numLeft+numRight)
			return core.AttributeValue{
				N: &val,
			}, nil
		} else {
			return core.AttributeValue{}, fmt.Errorf("unsupported operator: %s", v.Operator)
		}

	default:
		return core.AttributeValue{}, fmt.Errorf("unsupported operand type: %T", v)
	}
}

func (o *UpdateOperation) buildPath(operand ast.PathOperand) (core.PathOperand, error) {
	switch operand := operand.(type) {
	case *ast.AttributeNameOperand:
		if operand.HasSharp {
			key := "#" + operand.Identifier.TokenLiteral()
			name, ok := o.expressionAttributeNames[key]
			if !ok {
				msg := fmt.Sprintf("An expression attribute name used in the document path is not defined; attribute name: %s", key)
				return nil, fmt.Errorf(msg)
			}
			return &core.AttributeNameOperand{
				Name: name,
			}, nil
		} else if operand.HasColon {
			return nil, fmt.Errorf("path contains attribute value: %s", operand.Identifier.TokenLiteral())
		} else {
			name := operand.Identifier.TokenLiteral()
			return &core.AttributeNameOperand{
				Name: name,
			}, nil
		}
	case *ast.IndexOperand:
		left, err := o.buildPath(operand.Left)
		if err != nil {
			return nil, err
		}

		return &core.IndexOperand{
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
		return &core.DotOperand{
			Left:  left,
			Right: right,
		}, nil
	default:
		return nil, fmt.Errorf("unknown operand type: %T", operand)
	}
}

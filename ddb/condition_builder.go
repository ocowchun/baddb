package ddb

import (
	"fmt"
	"github.com/ocowchun/baddb/expression"
	"github.com/ocowchun/baddb/expression/ast"
	"strconv"
	"strings"
)

type ConditionBuilder struct {
	conditionExpression       ast.ConditionExpression
	expressionAttributeNames  map[string]string
	expressionAttributeValues map[string]AttributeValue
}

func BuildCondition(
	conditionExpressionContent string,
	expressionAttributeNames map[string]string,
	expressionAttributeValues map[string]AttributeValue,
) (*Condition, error) {
	conditionExpression, err := expression.ParseConditionExpression(conditionExpressionContent)
	if err != nil {
		return nil, err
	}

	builder := &ConditionBuilder{
		conditionExpression:       conditionExpression,
		expressionAttributeNames:  expressionAttributeNames,
		expressionAttributeValues: expressionAttributeValues,
	}

	return builder.BuildCondition()
}

type Condition struct {
	f func(entry *Entry) (bool, error)
}

func (c *Condition) Check(entry *Entry) (bool, error) {
	return c.f(entry)
}

type Operand interface {
	operand()
}

type PathOperand interface {
	operand()
	pathOperand()
	String() string
}

type AttributeNameOperand struct {
	Name string
}

func (a *AttributeNameOperand) operand()     {}
func (a *AttributeNameOperand) pathOperand() {}
func (a *AttributeNameOperand) String() string {
	return a.Name
}

type IndexOperand struct {
	Left  PathOperand
	Index int
}

func (i *IndexOperand) operand()     {}
func (i *IndexOperand) pathOperand() {}
func (i *IndexOperand) String() string {
	return fmt.Sprintf("%s[%d]", i.Left, i.Index)
}

type DotOperand struct {
	Left  PathOperand
	Right PathOperand
}

func (d *DotOperand) operand()     {}
func (d *DotOperand) pathOperand() {}
func (d *DotOperand) String() string {
	return fmt.Sprintf("%s.%s", d.Left, d.Right)
}

type AttributeValueOperand struct {
	Value AttributeValue
}

func (a AttributeValueOperand) operand() {}

type SizeOperand struct {
	Path PathOperand
}

func (s *SizeOperand) operand()     {}
func (s *SizeOperand) pathOperand() {}
func (s *SizeOperand) String() string {
	return fmt.Sprintf("size(%s)", s.Path)
}

func (b *ConditionBuilder) buildOperand(operand ast.Operand) (Operand, error) {
	switch operand := operand.(type) {
	case *ast.AttributeNameOperand:
		if operand.HasColon {
			key := ":" + operand.Identifier.TokenLiteral()
			val, ok := b.expressionAttributeValues[key]
			if !ok {
				msg := fmt.Sprintf("An expression attribute name used in the document path is not defined; attribute name: %s", key)
				return nil, fmt.Errorf(msg)
			}
			return &AttributeValueOperand{
				Value: val,
			}, nil
		} else {
			return b.buildPath(operand)
		}
	case *ast.SizeOperand:
		path, err := b.buildPath(operand.Path)
		if err != nil {
			return nil, err
		}

		return &SizeOperand{
			Path: path,
		}, nil
	default:
		return b.buildPath(operand)
	}

}

func (b *ConditionBuilder) buildPath(operand ast.Operand) (PathOperand, error) {
	// it's ok to have condition like name = "ben", but is it also ok to have name = lastName?
	switch operand := operand.(type) {
	case *ast.AttributeNameOperand:
		if operand.HasSharp {
			key := "#" + operand.Identifier.TokenLiteral()
			name, ok := b.expressionAttributeNames[key]
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
		left, err := b.buildPath(operand.Left)
		if err != nil {
			return nil, err
		}

		return &IndexOperand{
			Left:  left,
			Index: operand.Index,
		}, nil

	case *ast.DotOperand:
		left, err := b.buildPath(operand.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildPath(operand.Right)
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

func (b *ConditionBuilder) BuildCondition() (*Condition, error) {
	return b.buildCondition(b.conditionExpression)
}

func (b *ConditionBuilder) buildCondition(expression ast.ConditionExpression) (*Condition, error) {
	switch exp := expression.(type) {
	case *ast.ComparatorConditionExpression:
		return b.BuildComparatorCondition(exp)
	case *ast.BetweenConditionExpression:
		return b.BuildBetweenCondition(exp)
	case *ast.InConditionExpression:
		return b.BuildInCondition(exp)
	case *ast.FunctionConditionExpression:
		return b.BuildFunctionCondition(exp)
	case *ast.AndConditionExpression:
		return b.BuildAndCondition(exp)
	case *ast.OrConditionExpression:
		return b.BuildOrCondition(exp)
	case *ast.NotConditionExpression:
		return b.BuildNotCondition(exp)
	}

	return nil, fmt.Errorf("unknown condition expression type: %T", b.conditionExpression)
}

func (b *ConditionBuilder) BuildNotCondition(exp *ast.NotConditionExpression) (*Condition, error) {
	condition, err := b.buildCondition(exp.Condition)
	if err != nil {
		return nil, err
	}

	f := func(entry *Entry) (bool, error) {
		result, err := condition.Check(entry)
		if err != nil {
			return false, err
		}
		return !result, nil
	}

	return &Condition{
		f: f,
	}, nil
}

func (b *ConditionBuilder) BuildOrCondition(exp *ast.OrConditionExpression) (*Condition, error) {
	left, err := b.buildCondition(exp.Left)
	if err != nil {
		return nil, err
	}

	right, err := b.buildCondition(exp.Right)
	if err != nil {
		return nil, err
	}

	f := func(entry *Entry) (bool, error) {
		leftResult, err := left.Check(entry)
		if err != nil {
			return false, err
		}

		rightResult, err := right.Check(entry)
		if err != nil {
			return false, err
		}

		return leftResult || rightResult, nil
	}

	return &Condition{
		f: f,
	}, nil
}

func (b *ConditionBuilder) BuildAndCondition(exp *ast.AndConditionExpression) (*Condition, error) {
	left, err := b.buildCondition(exp.Left)
	if err != nil {
		return nil, err
	}

	right, err := b.buildCondition(exp.Right)
	if err != nil {
		return nil, err
	}

	f := func(entry *Entry) (bool, error) {
		leftResult, err := left.Check(entry)
		if err != nil {
			return false, err
		}

		rightResult, err := right.Check(entry)
		if err != nil {
			return false, err
		}

		return leftResult && rightResult, nil
	}

	return &Condition{
		f: f,
	}, nil
}

func (b *ConditionBuilder) BuildFunctionCondition(exp *ast.FunctionConditionExpression) (*Condition, error) {
	switch fn := exp.Function.(type) {
	case *ast.AttributeExistsFunctionExpression:
		return b.BuildAttributeExistsFunction(fn)
	case *ast.AttributeNotExistsFunctionExpression:
		return b.BuildAttributeNotExistsFunction(fn)
	case *ast.AttributeTypeFunctionExpression:
		return b.BuildAttributeTypeFunction(fn)
	case *ast.BeginsWithFunctionExpression:
		return b.BuildBeginsWithFunction(fn)
	case *ast.ContainsFunctionExpression:
		return b.BuildContainsFunction(fn)
	}
	return nil, fmt.Errorf("unknown function expression type: %T", exp)
}

func (b *ConditionBuilder) BuildContainsFunction(exp *ast.ContainsFunctionExpression) (*Condition, error) {
	leftOperand, err := b.buildOperand(exp.Path)
	if err != nil {
		return nil, err
	}

	rightOperand, err := b.buildOperand(exp.Operand)
	if err != nil {
		return nil, err
	}

	f := func(entry *Entry) (bool, error) {
		leftVal, err := getValue(entry, leftOperand)
		if err != nil {
			return false, err
		}
		rightVal, err := getValue(entry, rightOperand)
		if err != nil {
			return false, err
		}
		if leftVal.S != nil && rightVal.S != nil {
			return strings.Contains(*leftVal.S, *rightVal.S), nil
		} else if leftVal.SS != nil && rightVal.S != nil {
			for _, s := range *leftVal.SS {
				if s == *rightVal.S {
					return true, nil
				}
			}
			return false, nil
		} else if leftVal.NS != nil && rightVal.N != nil {
			for _, n := range *leftVal.NS {
				if n == *rightVal.N {
					return true, nil
				}
			}
			return false, nil

		} else if leftVal.L != nil {
			//TODO: can we use map in list here?
			for _, v := range *leftVal.L {
				if v.S != nil && *v.S == *rightVal.S {
					return true, nil
				} else if v.N != nil && *v.N == *rightVal.N {
					return true, nil
				}
			}
			return false, nil
		}
		return false, fmt.Errorf("left operand must be a list of strings and right operand must be a string")
	}

	return &Condition{
		f: f,
	}, nil

}

func (b *ConditionBuilder) BuildBeginsWithFunction(exp *ast.BeginsWithFunctionExpression) (*Condition, error) {
	leftOperand, err := b.buildOperand(exp.Path)
	if err != nil {
		return nil, err
	}

	rightOperand, err := b.buildOperand(exp.Prefix)
	if err != nil {
		return nil, err
	}

	f := func(entry *Entry) (bool, error) {
		leftVal, err := getValue(entry, leftOperand)
		if err != nil {
			return false, err
		}
		rightVal, err := getValue(entry, rightOperand)
		if err != nil {
			return false, err
		}
		if leftVal.S != nil && rightVal.S != nil {
			return strings.HasPrefix(*leftVal.S, *rightVal.S), nil
		} else {
			return false, fmt.Errorf("both values must be string")
		}
	}

	return &Condition{
		f: f,
	}, nil

}

func (b *ConditionBuilder) BuildAttributeTypeFunction(exp *ast.AttributeTypeFunctionExpression) (*Condition, error) {
	operand, err := b.buildOperand(exp.Path)
	if err != nil {
		return nil, err
	}

	f := func(entry *Entry) (bool, error) {
		val, err := getValue(entry, operand)
		if err != nil {
			return false, err
		}

		return val.Type() == exp.Type, nil
	}

	return &Condition{
		f: f,
	}, nil
}

func (b *ConditionBuilder) BuildAttributeNotExistsFunction(exp *ast.AttributeNotExistsFunctionExpression) (*Condition, error) {
	operand, err := b.buildOperand(exp.Path)
	if err != nil {
		return nil, err
	}

	f := func(entry *Entry) (bool, error) {
		_, err := getValue(entry, operand)
		if err != nil {
			return true, nil
		}
		return false, nil
	}

	return &Condition{
		f: f,
	}, nil
}
func (b *ConditionBuilder) BuildAttributeExistsFunction(exp *ast.AttributeExistsFunctionExpression) (*Condition, error) {
	operand, err := b.buildOperand(exp.Path)
	if err != nil {
		return nil, err
	}

	f := func(entry *Entry) (bool, error) {
		_, err := getValue(entry, operand)
		if err != nil {
			return false, nil
		}
		return true, nil
	}

	return &Condition{
		f: f,
	}, nil

}

func (b *ConditionBuilder) BuildInCondition(exp *ast.InConditionExpression) (*Condition, error) {
	leftOperand, err := b.buildOperand(exp.Operand)
	if err != nil {
		return nil, err
	}

	rightOperands := make([]Operand, len(exp.Values))
	for i, right := range exp.Values {
		operand, err := b.buildOperand(right)
		if err != nil {
			return nil, err
		}
		rightOperands[i] = operand
	}

	f := func(entry *Entry) (bool, error) {
		leftVal, err := getValue(entry, leftOperand)
		if err != nil {
			return false, err
		}

		for _, rightOperand := range rightOperands {
			rightVal, err := getValue(entry, rightOperand)
			if err != nil {
				return false, err
			}
			if leftVal.Equal(rightVal) {
				return true, nil
			}
		}

		return false, nil
	}

	return &Condition{
		f: f,
	}, nil
}

func (b *ConditionBuilder) BuildBetweenCondition(exp *ast.BetweenConditionExpression) (*Condition, error) {
	leftOperand, err := b.buildOperand(exp.Operand)
	if err != nil {
		return nil, err
	}

	lowerBound, err := b.buildOperand(exp.LowerBound)
	if err != nil {
		return nil, err
	}

	upperBound, err := b.buildOperand(exp.UpperBound)
	if err != nil {
		return nil, err
	}

	f := func(entry *Entry) (bool, error) {
		leftVal, err := getValue(entry, leftOperand)
		if err != nil {
			return false, err
		}
		lowerBoundVal, err := getValue(entry, lowerBound)
		if err != nil {
			return false, err
		}
		upperBoundVal, err := getValue(entry, upperBound)
		if err != nil {
			return false, err
		}
		lowerCompared, err := compareValue(leftVal, lowerBoundVal, ">=")
		if err != nil {
			return false, err
		}
		upperCompared, err := compareValue(leftVal, upperBoundVal, "<=")
		if err != nil {
			return false, err
		}
		return lowerCompared && upperCompared, nil
	}
	return &Condition{
		f: f,
	}, nil
}

func (b *ConditionBuilder) BuildComparatorCondition(exp *ast.ComparatorConditionExpression) (*Condition, error) {
	leftOperand, err := b.buildOperand(exp.Left)
	if err != nil {
		return nil, err
	}

	rightOperand, err := b.buildOperand(exp.Right)
	if err != nil {
		return nil, err
	}

	f := func(entry *Entry) (bool, error) {
		leftVal, err := getValue(entry, leftOperand)
		if err != nil {
			return false, err
		}
		rightVal, err := getValue(entry, rightOperand)
		if err != nil {
			return false, err
		}
		return compareValue(leftVal, rightVal, exp.Operator)
	}

	return &Condition{
		f: f,
	}, nil
}

func compareValue(leftVal AttributeValue, rightVal AttributeValue, operator string) (bool, error) {
	compared, err := leftVal.Compare(rightVal)
	if err != nil {
		return false, err
	}

	switch operator {
	case "=":
		return compared == 0, nil
	case "<":
		return compared < 0, nil
	case "<=":
		return compared <= 0, nil
	case ">":
		return compared > 0, nil
	case ">=":
		return compared >= 0, nil
	default:
		return false, fmt.Errorf("predicate op %s not found", operator)
	}
}

func getValue(entry *Entry, operand Operand) (AttributeValue, error) {
	switch left := operand.(type) {
	case PathOperand:
		return getValueFromPath(entry.Body, left)
	case *AttributeValueOperand:
		return left.Value, nil
	case *SizeOperand:
		val, err := getValueFromPath(entry.Body, left.Path)
		if err != nil {
			return AttributeValue{}, err
		}

		if val.S != nil {
			l := strconv.Itoa(len(*val.S))
			return AttributeValue{N: &l}, nil
		} else if val.B != nil {
			l := strconv.Itoa(len(*val.B))
			return AttributeValue{N: &l}, nil
		} else if val.NS != nil {
			l := strconv.Itoa(len(*val.NS))
			return AttributeValue{N: &l}, nil
		} else if val.SS != nil {
			l := strconv.Itoa(len(*val.SS))
			return AttributeValue{N: &l}, nil
		} else if val.L != nil {
			l := strconv.Itoa(len(*val.L))
			return AttributeValue{N: &l}, nil
		} else if val.M != nil {
			l := strconv.Itoa(len(*val.M))
			return AttributeValue{N: &l}, nil
		} else {
			return AttributeValue{}, fmt.Errorf("The conditional request failed")
		}
	default:
		return AttributeValue{}, fmt.Errorf("unknown operand type: %T", left)
	}
}

func getValueFromPath(entry map[string]AttributeValue, path PathOperand) (AttributeValue, error) {
	switch path := path.(type) {
	case *AttributeNameOperand:
		key := path.Name
		val, ok := entry[key]
		if !ok {
			return AttributeValue{}, fmt.Errorf("key %s not found", key)
		}
		return val, nil
	case *IndexOperand:
		leftVal, err := getValueFromPath(entry, path.Left)
		if err != nil {
			return AttributeValue{}, err
		}
		if leftVal.L == nil {
			return AttributeValue{}, fmt.Errorf("operand is not a list")
		}
		list := *leftVal.L
		if path.Index < 0 || path.Index >= len(list) {
			return AttributeValue{}, fmt.Errorf("index out of range")
		}
		return list[path.Index], nil
	case *DotOperand:
		leftVal, err := getValueFromPath(entry, path.Left)
		if err != nil {
			return AttributeValue{}, err
		}
		if leftVal.M == nil {
			return AttributeValue{}, fmt.Errorf("operand is not a map")
		}
		return getValueFromPath(*leftVal.M, path.Right)
	default:
		return AttributeValue{}, fmt.Errorf("unknown path operand type: %T", path)
	}
}

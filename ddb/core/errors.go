package core

import "fmt"

type InvalidConditionExpressionError struct {
	RawErr error
}

func (e *InvalidConditionExpressionError) Error() string {
	return fmt.Sprintf("Invalid ConditionExpression: %v", e.RawErr)
}

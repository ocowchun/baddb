package core

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
)

type Entry struct {
	Body map[string]AttributeValue
}

func (e *Entry) Clone() *Entry {
	clonedBody := make(map[string]AttributeValue)
	for key, val := range e.Body {
		clonedBody[key] = val.Clone()
	}
	return &Entry{
		Body: clonedBody,
	}
}

func (e *Entry) Get(path PathOperand) (AttributeValue, error) {
	return getValueFromPath(e.Body, path)
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

func (e *Entry) Set(path PathOperand, val AttributeValue) error {
	return setAttribute(e.Body, path, val)
}

func setAttribute(m map[string]AttributeValue, path PathOperand, val AttributeValue) error {
	switch path := path.(type) {
	case *AttributeNameOperand:
		m[path.Name] = val
	case *IndexOperand:
		list, err := getValueFromPath(m, path.Left)
		if err != nil {
			return err
		}
		if list.L == nil {
			return errors.New("list is nil")
		}
		// TODO: ensure the implementation follows the spec
		// When you use SET to update a list element, the contents of that element are replaced with the new data that you specify.
		// If the element doesn't already exist, SET appends the new element at the end of the list.
		// If you add multiple elements in a single SET operation, the elements are sorted in order by element number.

		if path.Index < 0 {
			return errors.New("index out of range")
		} else if path.Index < len(*list.L) {
			(*list.L)[path.Index] = val
		} else if path.Index == len(*list.L) {
			*list.L = append(*list.L, val)
		} else {
			return errors.New("index out of range")
		}

		return setAttribute(m, path.Left, list)
	case *DotOperand:
		obj, err := getValueFromPath(m, path.Left)
		if err != nil {
			return err
		}
		if obj.M == nil {
			return errors.New("map is nil")
		}

		err = setAttribute(*obj.M, path.Right, val)
		if err != nil {
			return err
		}

		return setAttribute(m, path.Left, obj)
	}
	return nil
}

func (e *Entry) Remove(path PathOperand) error {
	return removeAttribute(e.Body, path)
}

func removeAttribute(m map[string]AttributeValue, path PathOperand) error {
	switch path := path.(type) {
	case *AttributeNameOperand:
		delete(m, path.Name)
	case *IndexOperand:
		list, err := getValueFromPath(m, path.Left)
		if err != nil {
			return err
		}
		if list.L == nil {
			return errors.New("list is nil")
		}
		if path.Index < 0 || path.Index >= len(*list.L) {
			return errors.New("index out of range")
		}
		*list.L = append((*list.L)[:path.Index], (*list.L)[path.Index+1:]...)
		return setAttribute(m, path.Left, list)
	case *DotOperand:
		obj, err := getValueFromPath(m, path.Left)
		if err != nil {
			return err
		}
		if obj.M == nil {
			return errors.New("map is nil")
		}

		err = removeAttribute(*obj.M, path.Right)
		if err != nil {
			return err
		}

		return setAttribute(m, path.Left, obj)
	}
	return nil

}

func (e *Entry) Add(path *AttributeNameOperand, val AttributeValue) error {
	if val.N != nil {
		currentVal, ok := e.Body[path.Name]
		if !ok {
			e.Body[path.Name] = val
		} else if currentVal.N != nil {

			numLeft, err := strconv.ParseFloat(*currentVal.N, 64)
			if err != nil {
				return err
			}

			numRight, err := strconv.ParseFloat(*val.N, 64)
			if err != nil {
				return err
			}

			newVal := fmt.Sprintf("%v", numLeft+numRight)
			e.Body[path.Name] = AttributeValue{
				N: &newVal,
			}
		} else {
			return errors.New("An operand in the update expression has an incorrect data type")
		}
	} else if val.SS != nil {
		currentVal, ok := e.Body[path.Name]
		if !ok {
			e.Body[path.Name] = val
		} else if currentVal.SS != nil {
			ss := make(map[string]bool)
			for _, v := range *currentVal.SS {
				ss[v] = true
			}
			for _, v := range *val.SS {
				ss[v] = true
			}
			newVal := make([]string, 0)
			for k := range ss {
				newVal = append(newVal, k)
			}
			sort.Strings(newVal)

			e.Body[path.Name] = AttributeValue{
				SS: &newVal,
			}
		} else {
			return errors.New("An operand in the update expression has an incorrect data type")
		}
	} else if val.NS != nil {
		currentVal, ok := e.Body[path.Name]
		if !ok {
			e.Body[path.Name] = val
		} else if currentVal.NS != nil {
			ss := make(map[string]bool)
			for _, v := range *currentVal.NS {
				ss[v] = true
			}
			for _, v := range *val.NS {
				ss[v] = true
			}
			newVal := make([]string, 0)
			for k := range ss {
				newVal = append(newVal, k)
			}
			sort.Strings(newVal)

			e.Body[path.Name] = AttributeValue{
				NS: &newVal,
			}
		} else {
			return errors.New("An operand in the update expression has an incorrect data type")
		}
	} else {
		return fmt.Errorf("Incorrect operand type for operator or function; operator: ADD, operand type: %s, typeSet: ALLOWED_FOR_ADD_OPERAND", val.Type())
	}

	return nil
}

func (e *Entry) Delete(path *AttributeNameOperand, val AttributeValue) error {
	if val.SS != nil {
		currentVal, ok := e.Body[path.Name]
		if !ok {
			// no op
		} else if currentVal.SS != nil {
			ss := make(map[string]bool)
			for _, v := range *currentVal.SS {
				ss[v] = true
			}
			for _, v := range *val.SS {
				delete(ss, v)
			}

			newVal := make([]string, 0)
			for k := range ss {
				newVal = append(newVal, k)
			}
			sort.Strings(newVal)

			e.Body[path.Name] = AttributeValue{
				SS: &newVal,
			}
		} else {
			return errors.New("An operand in the update expression has an incorrect data type")
		}

	} else if val.NS != nil {
		currentVal, ok := e.Body[path.Name]
		if !ok {
			// no op
		} else if currentVal.NS != nil {
			ss := make(map[string]bool)
			for _, v := range *currentVal.NS {
				ss[v] = true
			}
			for _, v := range *val.NS {
				delete(ss, v)
			}

			newVal := make([]string, 0)
			for k := range ss {
				newVal = append(newVal, k)
			}
			sort.Strings(newVal)

			e.Body[path.Name] = AttributeValue{
				NS: &newVal,
			}
		} else {
			return errors.New("An operand in the update expression has an incorrect data type")
		}
	} else {
		return fmt.Errorf("Incorrect operand type for operator or function; operator: DELETE, operand type: %s, typeSet: ALLOWED_FOR_DELETE_OPERAND", val.Type())

	}

	return nil
}

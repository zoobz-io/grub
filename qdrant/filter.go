package qdrant

import (
	"fmt"

	"github.com/qdrant/go-client/qdrant"
	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/vecna"
)

// translateFilter converts a vecna.Filter to a qdrant.Filter.
func translateFilter(f *vecna.Filter) (*qdrant.Filter, error) {
	if f == nil {
		return nil, nil
	}

	if err := f.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", grub.ErrInvalidQuery, err)
	}

	return translateNode(f)
}

// translateNode recursively translates a filter node.
func translateNode(f *vecna.Filter) (*qdrant.Filter, error) {
	switch f.Op() {
	case vecna.And:
		return translateAnd(f.Children())
	case vecna.Or:
		return translateOr(f.Children())
	case vecna.Not:
		return translateNot(f.Children())
	default:
		// Field condition - wrap in filter with Must
		cond, err := translateFieldCondition(f)
		if err != nil {
			return nil, err
		}
		return &qdrant.Filter{Must: []*qdrant.Condition{cond}}, nil
	}
}

// translateAnd translates an AND filter.
func translateAnd(children []*vecna.Filter) (*qdrant.Filter, error) {
	conditions := make([]*qdrant.Condition, 0, len(children))
	for _, child := range children {
		cond, err := translateToCondition(child)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, cond)
	}
	return &qdrant.Filter{Must: conditions}, nil
}

// translateOr translates an OR filter.
func translateOr(children []*vecna.Filter) (*qdrant.Filter, error) {
	conditions := make([]*qdrant.Condition, 0, len(children))
	for _, child := range children {
		cond, err := translateToCondition(child)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, cond)
	}
	return &qdrant.Filter{Should: conditions}, nil
}

// translateNot translates a NOT filter.
func translateNot(children []*vecna.Filter) (*qdrant.Filter, error) {
	if len(children) != 1 {
		return nil, fmt.Errorf("%w: NOT requires exactly one child", grub.ErrInvalidQuery)
	}
	cond, err := translateToCondition(children[0])
	if err != nil {
		return nil, err
	}
	return &qdrant.Filter{MustNot: []*qdrant.Condition{cond}}, nil
}

// translateToCondition converts a filter to a qdrant.Condition.
func translateToCondition(f *vecna.Filter) (*qdrant.Condition, error) {
	switch f.Op() {
	case vecna.And, vecna.Or, vecna.Not:
		// Nested logical operators become filter conditions
		nested, err := translateNode(f)
		if err != nil {
			return nil, err
		}
		return &qdrant.Condition{
			ConditionOneOf: &qdrant.Condition_Filter{Filter: nested},
		}, nil
	default:
		return translateFieldCondition(f)
	}
}

// translateFieldCondition translates a field condition.
func translateFieldCondition(f *vecna.Filter) (*qdrant.Condition, error) {
	field := f.Field()
	value := f.Value()

	switch f.Op() {
	case vecna.Eq:
		return translateMatch(field, value)
	case vecna.Ne:
		// Ne is implemented as MustNot with Eq
		match, err := translateMatch(field, value)
		if err != nil {
			return nil, err
		}
		return &qdrant.Condition{
			ConditionOneOf: &qdrant.Condition_Filter{
				Filter: &qdrant.Filter{MustNot: []*qdrant.Condition{match}},
			},
		}, nil
	case vecna.Gt:
		return translateRange(field, value, "gt")
	case vecna.Gte:
		return translateRange(field, value, "gte")
	case vecna.Lt:
		return translateRange(field, value, "lt")
	case vecna.Lte:
		return translateRange(field, value, "lte")
	case vecna.In:
		return translateIn(field, value)
	case vecna.Nin:
		// Nin is implemented as MustNot with In
		inCond, err := translateIn(field, value)
		if err != nil {
			return nil, err
		}
		return &qdrant.Condition{
			ConditionOneOf: &qdrant.Condition_Filter{
				Filter: &qdrant.Filter{MustNot: []*qdrant.Condition{inCond}},
			},
		}, nil
	case vecna.Like:
		return translateLike(field, value)
	case vecna.Contains:
		return translateContains(field, value)
	default:
		return nil, fmt.Errorf("%w: %s", grub.ErrOperatorNotSupported, f.Op())
	}
}

// translateMatch translates an equality match.
func translateMatch(field string, value any) (*qdrant.Condition, error) {
	switch v := value.(type) {
	case string:
		return qdrant.NewMatchKeyword(field, v), nil
	case int:
		return qdrant.NewMatchInt(field, int64(v)), nil
	case int64:
		return qdrant.NewMatchInt(field, v), nil
	case float64:
		// Qdrant doesn't have direct float match, use range with equal bounds
		return &qdrant.Condition{
			ConditionOneOf: &qdrant.Condition_Field{
				Field: &qdrant.FieldCondition{
					Key:   field,
					Range: &qdrant.Range{Gte: &v, Lte: &v},
				},
			},
		}, nil
	case bool:
		return qdrant.NewMatchBool(field, v), nil
	default:
		return nil, fmt.Errorf("%w: unsupported value type %T for Eq", grub.ErrInvalidQuery, value)
	}
}

// translateRange translates a range condition.
func translateRange(field string, value any, op string) (*qdrant.Condition, error) {
	var floatVal float64
	switch v := value.(type) {
	case int:
		floatVal = float64(v)
	case int64:
		floatVal = float64(v)
	case float64:
		floatVal = v
	default:
		return nil, fmt.Errorf("%w: range operators require numeric value, got %T", grub.ErrInvalidQuery, value)
	}

	r := &qdrant.Range{}
	switch op {
	case "gt":
		r.Gt = &floatVal
	case "gte":
		r.Gte = &floatVal
	case "lt":
		r.Lt = &floatVal
	case "lte":
		r.Lte = &floatVal
	}

	return &qdrant.Condition{
		ConditionOneOf: &qdrant.Condition_Field{
			Field: &qdrant.FieldCondition{Key: field, Range: r},
		},
	}, nil
}

// translateIn translates an IN condition.
func translateIn(field string, value any) (*qdrant.Condition, error) {
	slice, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: In requires slice value", grub.ErrInvalidQuery)
	}

	if len(slice) == 0 {
		return nil, fmt.Errorf("%w: In requires at least one value", grub.ErrInvalidQuery)
	}

	// Determine type from first element
	switch slice[0].(type) {
	case string:
		strings := make([]string, len(slice))
		for i, v := range slice {
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%w: In values must be same type", grub.ErrInvalidQuery)
			}
			strings[i] = s
		}
		return qdrant.NewMatchKeywords(field, strings...), nil
	case int, int64:
		ints := make([]int64, len(slice))
		for i, v := range slice {
			switch val := v.(type) {
			case int:
				ints[i] = int64(val)
			case int64:
				ints[i] = val
			default:
				return nil, fmt.Errorf("%w: In values must be same type", grub.ErrInvalidQuery)
			}
		}
		return qdrant.NewMatchInts(field, ints...), nil
	default:
		return nil, fmt.Errorf("%w: unsupported value type for In", grub.ErrInvalidQuery)
	}
}

// translateLike translates a LIKE pattern match.
func translateLike(field string, value any) (*qdrant.Condition, error) {
	pattern, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("%w: Like requires string pattern", grub.ErrInvalidQuery)
	}

	// Qdrant uses MatchText for text matching
	return &qdrant.Condition{
		ConditionOneOf: &qdrant.Condition_Field{
			Field: &qdrant.FieldCondition{
				Key: field,
				Match: &qdrant.Match{
					MatchValue: &qdrant.Match_Text{Text: pattern},
				},
			},
		},
	}, nil
}

// translateContains translates an array contains condition.
func translateContains(field string, value any) (*qdrant.Condition, error) {
	// Qdrant handles array contains via Match on the array field
	return translateMatch(field, value)
}

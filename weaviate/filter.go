package weaviate

import (
	"fmt"
	"strings"

	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/vecna"
)

// translateFilter converts a vecna.Filter to a Weaviate WhereBuilder.
func translateFilter(f *vecna.Filter) (*filters.WhereBuilder, error) {
	if f == nil {
		return nil, nil
	}

	if err := f.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", grub.ErrInvalidQuery, err)
	}

	return translateNode(f)
}

// translateNode recursively translates a filter node.
func translateNode(f *vecna.Filter) (*filters.WhereBuilder, error) {
	switch f.Op() {
	case vecna.And:
		return translateLogical(f.Children(), filters.And)
	case vecna.Or:
		return translateLogical(f.Children(), filters.Or)
	case vecna.Not:
		return translateNot(f.Children())
	default:
		return translateCondition(f)
	}
}

// translateLogical translates AND/OR filters.
// For OR: if any operand is nil (match-all), return nil (short-circuit match-all).
// For AND: filter out nil operands (they're no-ops).
func translateLogical(children []*vecna.Filter, op filters.WhereOperator) (*filters.WhereBuilder, error) {
	if len(children) == 0 {
		return nil, nil
	}

	clauses := make([]*filters.WhereBuilder, 0, len(children))
	for _, child := range children {
		clause, err := translateNode(child)
		if err != nil {
			return nil, err
		}
		// Handle nil clauses based on operator
		if clause == nil {
			if op == filters.Or {
				// OR with match-all = match-all
				return nil, nil
			}
			// AND with match-all = skip (no-op)
			continue
		}
		clauses = append(clauses, clause)
	}

	if len(clauses) == 0 {
		return nil, nil
	}

	if len(clauses) == 1 {
		return clauses[0], nil
	}

	return filters.Where().
		WithOperator(op).
		WithOperands(clauses), nil
}

// translateNot translates a NOT filter by negating at the leaf level.
// Weaviate doesn't have a NOT operator, so we must negate conditions directly.
func translateNot(children []*vecna.Filter) (*filters.WhereBuilder, error) {
	if len(children) != 1 {
		return nil, fmt.Errorf("%w: NOT requires exactly one child", grub.ErrInvalidQuery)
	}

	child := children[0]
	return negateFilter(child)
}

// negateFilter negates a filter by inverting operators or applying De Morgan's laws.
func negateFilter(f *vecna.Filter) (*filters.WhereBuilder, error) {
	switch f.Op() {
	case vecna.And:
		// De Morgan: NOT(AND(a,b)) = OR(NOT(a), NOT(b))
		// If any negated child is nil (match-all), OR short-circuits to match-all.
		negated := make([]*filters.WhereBuilder, 0, len(f.Children()))
		for _, child := range f.Children() {
			n, err := negateFilter(child)
			if err != nil {
				return nil, err
			}
			if n == nil {
				// OR with match-all = match-all
				return nil, nil
			}
			negated = append(negated, n)
		}
		if len(negated) == 0 {
			return nil, nil
		}
		if len(negated) == 1 {
			return negated[0], nil
		}
		return filters.Where().WithOperator(filters.Or).WithOperands(negated), nil

	case vecna.Or:
		// De Morgan: NOT(OR(a,b)) = AND(NOT(a), NOT(b))
		// Nil operands (match-all) are filtered out for AND (they're no-ops).
		negated := make([]*filters.WhereBuilder, 0, len(f.Children()))
		for _, child := range f.Children() {
			n, err := negateFilter(child)
			if err != nil {
				return nil, err
			}
			if n == nil {
				// AND with match-all = skip (no-op)
				continue
			}
			negated = append(negated, n)
		}
		if len(negated) == 0 {
			return nil, nil
		}
		if len(negated) == 1 {
			return negated[0], nil
		}
		return filters.Where().WithOperator(filters.And).WithOperands(negated), nil

	case vecna.Not:
		// Double negation: NOT(NOT(x)) = x
		if len(f.Children()) != 1 {
			return nil, fmt.Errorf("%w: NOT requires exactly one child", grub.ErrInvalidQuery)
		}
		return translateNode(f.Children()[0])

	case vecna.Eq:
		return applyValue(filters.Where().WithPath([]string{f.Field()}), filters.NotEqual, f.Value())
	case vecna.Ne:
		return applyValue(filters.Where().WithPath([]string{f.Field()}), filters.Equal, f.Value())
	case vecna.Lt:
		return applyValue(filters.Where().WithPath([]string{f.Field()}), filters.GreaterThanEqual, f.Value())
	case vecna.Lte:
		return applyValue(filters.Where().WithPath([]string{f.Field()}), filters.GreaterThan, f.Value())
	case vecna.Gt:
		return applyValue(filters.Where().WithPath([]string{f.Field()}), filters.LessThanEqual, f.Value())
	case vecna.Gte:
		return applyValue(filters.Where().WithPath([]string{f.Field()}), filters.LessThan, f.Value())
	case vecna.In:
		// NOT(IN([a,b,c])) = AND(NotEqual(a), NotEqual(b), NotEqual(c))
		return translateNin(f.Field(), f.Value())
	case vecna.Nin:
		// NOT(NIN) = IN
		return translateIn(f.Field(), f.Value())
	default:
		return nil, fmt.Errorf("%w: cannot negate %s", grub.ErrOperatorNotSupported, f.Op())
	}
}

// translateCondition translates a field condition.
func translateCondition(f *vecna.Filter) (*filters.WhereBuilder, error) {
	field := f.Field()
	value := f.Value()

	clause := filters.Where().WithPath([]string{field})

	switch f.Op() {
	case vecna.Eq:
		return applyValue(clause, filters.Equal, value)
	case vecna.Ne:
		return applyValue(clause, filters.NotEqual, value)
	case vecna.Gt:
		return applyValue(clause, filters.GreaterThan, value)
	case vecna.Gte:
		return applyValue(clause, filters.GreaterThanEqual, value)
	case vecna.Lt:
		return applyValue(clause, filters.LessThan, value)
	case vecna.Lte:
		return applyValue(clause, filters.LessThanEqual, value)
	case vecna.In:
		return translateIn(field, value)
	case vecna.Nin:
		return translateNin(field, value)
	case vecna.Like:
		pattern, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("%w: Like requires string pattern", grub.ErrInvalidQuery)
		}
		// Convert SQL-style wildcards to Weaviate wildcards: % -> *, _ -> ?
		pattern = strings.ReplaceAll(pattern, "%", "*")
		pattern = strings.ReplaceAll(pattern, "_", "?")
		return clause.WithOperator(filters.Like).WithValueText(pattern), nil
	case vecna.Contains:
		return translateContains(field, value)
	default:
		return nil, fmt.Errorf("%w: %s", grub.ErrOperatorNotSupported, f.Op())
	}
}

// applyValue applies a value to a where clause based on its type.
func applyValue(clause *filters.WhereBuilder, op filters.WhereOperator, value any) (*filters.WhereBuilder, error) {
	clause = clause.WithOperator(op)

	switch v := value.(type) {
	case string:
		return clause.WithValueText(v), nil
	case int:
		return clause.WithValueInt(int64(v)), nil
	case int64:
		return clause.WithValueInt(v), nil
	case float64:
		return clause.WithValueNumber(v), nil
	case bool:
		return clause.WithValueBoolean(v), nil
	default:
		return nil, fmt.Errorf("%w: unsupported value type %T", grub.ErrInvalidQuery, value)
	}
}

// translateIn translates an IN condition using ContainsAny.
func translateIn(field string, value any) (*filters.WhereBuilder, error) {
	slice, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: In requires slice value", grub.ErrInvalidQuery)
	}

	if len(slice) == 0 {
		// Empty IN matches nothing - return a filter that will always be false
		return filters.Where().WithPath([]string{field}).WithOperator(filters.Equal).WithValueText("__impossible_value__"), nil
	}

	// Convert slice to appropriate type
	clause := filters.Where().WithPath([]string{field})

	// Check first element type
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
		return clause.WithOperator(filters.ContainsAny).WithValueText(strings...), nil
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
		return clause.WithOperator(filters.ContainsAny).WithValueInt(ints...), nil
	case float64:
		floats := make([]float64, len(slice))
		for i, v := range slice {
			f, ok := v.(float64)
			if !ok {
				return nil, fmt.Errorf("%w: In values must be same type", grub.ErrInvalidQuery)
			}
			floats[i] = f
		}
		return clause.WithOperator(filters.ContainsAny).WithValueNumber(floats...), nil
	default:
		return nil, fmt.Errorf("%w: unsupported value type for In", grub.ErrInvalidQuery)
	}
}

// translateNin translates a NIN (not in) condition as AND of NotEqual clauses.
// Enforces homogeneous types like translateIn.
func translateNin(field string, value any) (*filters.WhereBuilder, error) {
	slice, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: Nin requires slice value", grub.ErrInvalidQuery)
	}

	if len(slice) == 0 {
		// Empty NIN matches everything - return nil (no filter)
		return nil, nil
	}

	// Build AND of NotEqual clauses, enforcing homogeneous types
	clauses := make([]*filters.WhereBuilder, 0, len(slice))

	// Check first element type and enforce all elements match
	switch slice[0].(type) {
	case string:
		for _, v := range slice {
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%w: Nin values must be same type", grub.ErrInvalidQuery)
			}
			c := filters.Where().WithPath([]string{field}).WithOperator(filters.NotEqual).WithValueText(s)
			clauses = append(clauses, c)
		}
	case int, int64:
		for _, v := range slice {
			var intVal int64
			switch val := v.(type) {
			case int:
				intVal = int64(val)
			case int64:
				intVal = val
			default:
				return nil, fmt.Errorf("%w: Nin values must be same type", grub.ErrInvalidQuery)
			}
			c := filters.Where().WithPath([]string{field}).WithOperator(filters.NotEqual).WithValueInt(intVal)
			clauses = append(clauses, c)
		}
	case float64:
		for _, v := range slice {
			f, ok := v.(float64)
			if !ok {
				return nil, fmt.Errorf("%w: Nin values must be same type", grub.ErrInvalidQuery)
			}
			c := filters.Where().WithPath([]string{field}).WithOperator(filters.NotEqual).WithValueNumber(f)
			clauses = append(clauses, c)
		}
	default:
		return nil, fmt.Errorf("%w: unsupported value type for Nin", grub.ErrInvalidQuery)
	}

	if len(clauses) == 1 {
		return clauses[0], nil
	}

	return filters.Where().WithOperator(filters.And).WithOperands(clauses), nil
}

// translateContains translates a Contains condition.
func translateContains(field string, value any) (*filters.WhereBuilder, error) {
	clause := filters.Where().WithPath([]string{field}).WithOperator(filters.ContainsAny)

	switch v := value.(type) {
	case string:
		return clause.WithValueText(v), nil
	case int:
		return clause.WithValueInt(int64(v)), nil
	case int64:
		return clause.WithValueInt(v), nil
	case float64:
		return clause.WithValueNumber(v), nil
	default:
		return nil, fmt.Errorf("%w: unsupported value type %T for Contains", grub.ErrInvalidQuery, value)
	}
}

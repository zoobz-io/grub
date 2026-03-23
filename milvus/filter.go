package milvus

import (
	"fmt"
	"strings"

	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/vecna"
)

// translateFilter converts a vecna.Filter to a Milvus expression string.
func translateFilter(f *vecna.Filter, metadataField string) (string, error) {
	if f == nil {
		return "", nil
	}

	if err := f.Err(); err != nil {
		return "", fmt.Errorf("%w: %v", grub.ErrInvalidQuery, err)
	}

	return translateNode(f, metadataField)
}

// translateNode recursively translates a filter node.
func translateNode(f *vecna.Filter, metadataField string) (string, error) {
	switch f.Op() {
	case vecna.And:
		return translateLogical(f.Children(), metadataField, "and")
	case vecna.Or:
		return translateLogical(f.Children(), metadataField, "or")
	case vecna.Not:
		return translateNot(f.Children(), metadataField)
	default:
		return translateCondition(f, metadataField)
	}
}

// translateLogical translates AND/OR filters.
func translateLogical(children []*vecna.Filter, metadataField, op string) (string, error) {
	if len(children) == 0 {
		return "", nil
	}

	clauses := make([]string, 0, len(children))
	for _, child := range children {
		clause, err := translateNode(child, metadataField)
		if err != nil {
			return "", err
		}
		clauses = append(clauses, clause)
	}

	if len(clauses) == 1 {
		return clauses[0], nil
	}

	return "(" + strings.Join(clauses, " "+op+" ") + ")", nil
}

// translateNot translates a NOT filter.
func translateNot(children []*vecna.Filter, metadataField string) (string, error) {
	if len(children) != 1 {
		return "", fmt.Errorf("%w: NOT requires exactly one child", grub.ErrInvalidQuery)
	}

	child, err := translateNode(children[0], metadataField)
	if err != nil {
		return "", err
	}

	return "not (" + child + ")", nil
}

// translateCondition translates a field condition.
func translateCondition(f *vecna.Filter, metadataField string) (string, error) {
	field := f.Field()
	value := f.Value()

	// Build the field path for JSON metadata
	fieldPath := fmt.Sprintf(`%s["%s"]`, metadataField, field)

	switch f.Op() {
	case vecna.Eq:
		return fmt.Sprintf(`%s == %s`, fieldPath, formatValue(value)), nil
	case vecna.Ne:
		return fmt.Sprintf(`%s != %s`, fieldPath, formatValue(value)), nil
	case vecna.Gt:
		return fmt.Sprintf(`%s > %s`, fieldPath, formatValue(value)), nil
	case vecna.Gte:
		return fmt.Sprintf(`%s >= %s`, fieldPath, formatValue(value)), nil
	case vecna.Lt:
		return fmt.Sprintf(`%s < %s`, fieldPath, formatValue(value)), nil
	case vecna.Lte:
		return fmt.Sprintf(`%s <= %s`, fieldPath, formatValue(value)), nil
	case vecna.In:
		return translateIn(fieldPath, value)
	case vecna.Nin:
		return translateNin(fieldPath, value)
	case vecna.Like:
		pattern, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("%w: Like requires string pattern", grub.ErrInvalidQuery)
		}
		return fmt.Sprintf(`%s like "%s"`, fieldPath, pattern), nil
	case vecna.Contains:
		return translateContains(fieldPath, value)
	default:
		return "", fmt.Errorf("%w: %s", grub.ErrOperatorNotSupported, f.Op())
	}
}

// translateIn translates an IN condition.
func translateIn(fieldPath string, value any) (string, error) {
	slice, ok := value.([]any)
	if !ok {
		return "", fmt.Errorf("%w: In requires slice value", grub.ErrInvalidQuery)
	}

	if len(slice) == 0 {
		// Empty IN matches nothing
		return "false", nil
	}

	values := make([]string, len(slice))
	for i, v := range slice {
		values[i] = formatValue(v)
	}

	return fmt.Sprintf(`%s in [%s]`, fieldPath, strings.Join(values, ", ")), nil
}

// translateNin translates a NOT IN condition.
func translateNin(fieldPath string, value any) (string, error) {
	slice, ok := value.([]any)
	if !ok {
		return "", fmt.Errorf("%w: Nin requires slice value", grub.ErrInvalidQuery)
	}

	if len(slice) == 0 {
		// Empty NOT IN matches everything
		return "true", nil
	}

	values := make([]string, len(slice))
	for i, v := range slice {
		values[i] = formatValue(v)
	}

	return fmt.Sprintf(`%s not in [%s]`, fieldPath, strings.Join(values, ", ")), nil
}

// translateContains translates a Contains condition using array_contains.
func translateContains(fieldPath string, value any) (string, error) {
	return fmt.Sprintf(`array_contains(%s, %s)`, fieldPath, formatValue(value)), nil
}

// formatValue formats a value for use in a Milvus expression.
func formatValue(v any) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf(`"%s"`, val)
	case int:
		return fmt.Sprintf(`%d`, val)
	case int64:
		return fmt.Sprintf(`%d`, val)
	case float64:
		return fmt.Sprintf(`%v`, val)
	case bool:
		return fmt.Sprintf(`%t`, val)
	default:
		return fmt.Sprintf(`%v`, val)
	}
}

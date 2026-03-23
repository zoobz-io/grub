package pinecone

import (
	"encoding/json"
	"fmt"

	"github.com/pinecone-io/go-pinecone/v2/pinecone"
	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/vecna"
)

// translateFilter converts a vecna.Filter to a Pinecone metadata filter.
// Note: Pinecone does not support Gt, Gte, Lt, Lte, Like, or Contains operators.
func translateFilter(f *vecna.Filter) (*pinecone.Metadata, error) {
	if f == nil {
		return nil, nil
	}

	if err := f.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", grub.ErrInvalidQuery, err)
	}

	filterMap, err := translateNode(f)
	if err != nil {
		return nil, err
	}

	return toStruct(filterMap)
}

// translateNode recursively translates a filter node to Pinecone filter map.
func translateNode(f *vecna.Filter) (map[string]any, error) {
	switch f.Op() {
	case vecna.And:
		return translateAnd(f.Children())
	case vecna.Or:
		return translateOr(f.Children())
	case vecna.Not:
		return translateNot(f.Children())
	default:
		return translateCondition(f)
	}
}

// translateAnd translates an AND filter.
func translateAnd(children []*vecna.Filter) (map[string]any, error) {
	clauses := make([]map[string]any, 0, len(children))
	for _, child := range children {
		clause, err := translateNode(child)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, clause)
	}
	return map[string]any{"$and": clauses}, nil
}

// translateOr translates an OR filter.
func translateOr(children []*vecna.Filter) (map[string]any, error) {
	clauses := make([]map[string]any, 0, len(children))
	for _, child := range children {
		clause, err := translateNode(child)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, clause)
	}
	return map[string]any{"$or": clauses}, nil
}

// translateNot translates a NOT filter.
func translateNot(children []*vecna.Filter) (map[string]any, error) {
	if len(children) != 1 {
		return nil, fmt.Errorf("%w: NOT requires exactly one child", grub.ErrInvalidQuery)
	}

	// Pinecone doesn't have a direct NOT, so we need to handle specific cases
	child := children[0]
	switch child.Op() {
	case vecna.Eq:
		// NOT Eq becomes Ne
		return map[string]any{child.Field(): map[string]any{"$ne": child.Value()}}, nil
	case vecna.In:
		// NOT In becomes Nin
		return map[string]any{child.Field(): map[string]any{"$nin": child.Value()}}, nil
	default:
		return nil, fmt.Errorf("%w: NOT with %s operator", grub.ErrOperatorNotSupported, child.Op())
	}
}

// translateCondition translates a field condition.
func translateCondition(f *vecna.Filter) (map[string]any, error) {
	field := f.Field()
	value := f.Value()

	switch f.Op() {
	case vecna.Eq:
		return map[string]any{field: map[string]any{"$eq": value}}, nil
	case vecna.Ne:
		return map[string]any{field: map[string]any{"$ne": value}}, nil
	case vecna.In:
		return map[string]any{field: map[string]any{"$in": value}}, nil
	case vecna.Nin:
		return map[string]any{field: map[string]any{"$nin": value}}, nil
	case vecna.Gt, vecna.Gte, vecna.Lt, vecna.Lte:
		return nil, fmt.Errorf("%w: Pinecone does not support %s operator", grub.ErrOperatorNotSupported, f.Op())
	case vecna.Like:
		return nil, fmt.Errorf("%w: Pinecone does not support Like operator", grub.ErrOperatorNotSupported)
	case vecna.Contains:
		return nil, fmt.Errorf("%w: Pinecone does not support Contains operator", grub.ErrOperatorNotSupported)
	default:
		return nil, fmt.Errorf("%w: %s", grub.ErrOperatorNotSupported, f.Op())
	}
}

// toStruct converts map[string]any to *pinecone.Metadata.
func toStruct(m map[string]any) (*pinecone.Metadata, error) {
	if m == nil {
		return nil, nil
	}
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	var meta pinecone.Metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

package ddb

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"testing"
)

func TestPerformSetClause(t *testing.T) {
	// TODO: add more failure cases

	tests := []struct {
		name                      string
		entry                     *Entry
		updateExpressionContent   string
		expressionAttributeNames  map[string]string
		expressionAttributeValues map[string]AttributeValue
		expected                  map[string]AttributeValue
		expectError               bool
	}{
		{
			name: "Set simple attribute",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"year":  {N: aws.String("2024")},
					"title": {S: aws.String("Old Title")},
				},
			},
			updateExpressionContent:  "SET title = :newTitle",
			expressionAttributeNames: make(map[string]string),
			expressionAttributeValues: map[string]AttributeValue{
				":newTitle": {S: aws.String("New Title")},
			},
			expected: map[string]AttributeValue{
				"year":  {N: aws.String("2024")},
				"title": {S: aws.String("New Title")},
			},
			expectError: false,
		},
		{
			name: "Set attribute with IfNotExists",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"year": {N: aws.String("2024")},
				},
			},
			updateExpressionContent: "SET title = if_not_exists(title, :newTitle)",
			expressionAttributeValues: map[string]AttributeValue{
				":newTitle": {S: aws.String("New Title")},
			},
			expected: map[string]AttributeValue{
				"year":  {N: aws.String("2024")},
				"title": {S: aws.String("New Title")},
			},
			expectError: false,
		},
		{
			name: "Set attribute with ListAppend",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"tags": {L: &[]AttributeValue{{S: aws.String("tag1")}}},
				},
			},
			updateExpressionContent: "SET tags = list_append(tags, :newTags)",
			expressionAttributeValues: map[string]AttributeValue{
				":newTags": {L: &[]AttributeValue{{S: aws.String("tag2")}}},
			},
			expected: map[string]AttributeValue{
				"tags": {L: &[]AttributeValue{{S: aws.String("tag1")}, {S: aws.String("tag2")}}},
			},
			expectError: false,
		},
		{
			name: "Set attribute with ListAppend to the beginning",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"tags": {L: &[]AttributeValue{{S: aws.String("tag1")}}},
				},
			},
			updateExpressionContent: "SET tags = list_append(:newTags, tags)",
			expressionAttributeValues: map[string]AttributeValue{
				":newTags": {L: &[]AttributeValue{{S: aws.String("tag2")}}},
			},
			expected: map[string]AttributeValue{
				"tags": {L: &[]AttributeValue{{S: aws.String("tag2")}, {S: aws.String("tag1")}}},
			},
			expectError: false,
		},
		{
			name: "Set attribute with InfixExpression",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"price": {N: aws.String("100")},
				},
			},
			updateExpressionContent: "SET price = price - :discount",
			expressionAttributeValues: map[string]AttributeValue{
				":discount": {N: aws.String("10")},
			},
			expected: map[string]AttributeValue{
				"price": {N: aws.String("90")},
			},
			expectError: false,
		},
		{
			name: "Set multiple attributes",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"Id":              {N: aws.String("789")},
					"ProductCategory": {S: aws.String("Home Improvement")},
					"Price":           {N: aws.String("52")},
					"InStock":         {Bool: aws.Bool(true)},
					"Brand":           {S: aws.String("Acme")},
				},
			},
			updateExpressionContent:  "SET ProductCategory = :c, Price = :p",
			expressionAttributeNames: make(map[string]string),
			expressionAttributeValues: map[string]AttributeValue{
				":c": {S: aws.String("Hardware")},
				":p": {N: aws.String("60")},
			},
			expected: map[string]AttributeValue{
				"Id":              {N: aws.String("789")},
				"ProductCategory": {S: aws.String("Hardware")},
				"Price":           {N: aws.String("60")},
				"InStock":         {Bool: aws.Bool(true)},
				"Brand":           {S: aws.String("Acme")},
			},
			expectError: false,
		},
		{
			name: "Set new lists and maps",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"Id":              {N: aws.String("789")},
					"ProductCategory": {S: aws.String("Home Improvement")},
				},
			},
			updateExpressionContent:  "SET RelatedItems = :ri, ProductReviews = :pr",
			expressionAttributeNames: make(map[string]string),
			expressionAttributeValues: map[string]AttributeValue{
				":ri": {
					L: &[]AttributeValue{
						{S: aws.String("Hammer")},
					},
				},
				":pr": {
					M: &map[string]AttributeValue{
						"FiveStar": {
							L: &[]AttributeValue{
								{S: aws.String("Best product ever!")},
							},
						},
					},
				},
			},
			expected: map[string]AttributeValue{
				"Id":              {N: aws.String("789")},
				"ProductCategory": {S: aws.String("Home Improvement")},
				"RelatedItems": {
					L: &[]AttributeValue{
						{S: aws.String("Hammer")},
					},
				},
				"ProductReviews": {
					M: &map[string]AttributeValue{
						"FiveStar": {
							L: &[]AttributeValue{
								{S: aws.String("Best product ever!")},
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "Set add elements to a list",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"Id":              {N: aws.String("789")},
					"ProductCategory": {S: aws.String("Home Improvement")},
					"RelatedItems": {
						L: &[]AttributeValue{
							{S: aws.String("Hammer")},
						},
					},
				},
			},
			updateExpressionContent:  "SET RelatedItems[1] = :ri",
			expressionAttributeNames: make(map[string]string),
			expressionAttributeValues: map[string]AttributeValue{
				":ri": {
					S: aws.String("Nails"),
				},
			},
			expected: map[string]AttributeValue{
				"Id":              {N: aws.String("789")},
				"ProductCategory": {S: aws.String("Home Improvement")},
				"RelatedItems": {
					L: &[]AttributeValue{
						{S: aws.String("Hammer")},
						{S: aws.String("Nails")},
					},
				},
			},
			expectError: false,
		},
		{
			name: "Set add nested map attributes",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"Id":              {N: aws.String("789")},
					"ProductCategory": {S: aws.String("Home Improvement")},
					"ProductReviews": {
						M: &map[string]AttributeValue{
							"FiveStar": {
								L: &[]AttributeValue{
									{S: aws.String("Best product ever!")},
								},
							},
						},
					},
				},
			},
			updateExpressionContent: "SET #pr.#5star[1] = :r5, #pr.#3star = :r3",
			expressionAttributeNames: map[string]string{
				"#pr":    "ProductReviews",
				"#5star": "FiveStar",
				"#3star": "ThreeStar",
			},
			expressionAttributeValues: map[string]AttributeValue{
				":r5": {
					S: aws.String("Very happy with my purchase"),
				},
				":r3": {
					L: &[]AttributeValue{
						{
							S: aws.String("Just OK - not that great"),
						},
					},
				},
			},
			expected: map[string]AttributeValue{
				"Id":              {N: aws.String("789")},
				"ProductCategory": {S: aws.String("Home Improvement")},
				"ProductReviews": {
					M: &map[string]AttributeValue{
						"FiveStar": {
							L: &[]AttributeValue{
								{S: aws.String("Best product ever!")},
								{S: aws.String("Very happy with my purchase")},
							},
						},
						"ThreeStar": {
							L: &[]AttributeValue{
								{S: aws.String("Just OK - not that great")},
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "Set simple attribute with wrong value",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"year":  {N: aws.String("2024")},
					"title": {S: aws.String("Old Title")},
				},
			},
			updateExpressionContent:  "SET title = :wrongTitle",
			expressionAttributeNames: make(map[string]string),
			expressionAttributeValues: map[string]AttributeValue{
				":newTitle": {S: aws.String("New Title")},
			},
			expected: map[string]AttributeValue{
				"year":  {N: aws.String("2024")},
				"title": {S: aws.String("Old Title")},
			},
			expectError: true,
		},
		{
			name: "Set attribute with InfixExpression and wrong operand",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"price": {S: aws.String("¥100")},
				},
			},
			updateExpressionContent: "SET price = price - :discount",
			expressionAttributeValues: map[string]AttributeValue{
				":discount": {N: aws.String("0.9")},
			},
			expected: map[string]AttributeValue{
				"price": {S: aws.String("¥100")},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation, err := BuildUpdateOperation(
				tt.updateExpressionContent,
				tt.expressionAttributeNames,
				tt.expressionAttributeValues,
			)
			if err != nil {
				t.Fatalf("Unexpected error: %v, when build operation", err)
			}

			err = operation.Perform(tt.entry)
			if (err != nil) != tt.expectError {
				t.Fatalf("Expected error: %v, got: %v", tt.expectError, err)
			}

			if err == nil {
				for key, expectedValue := range tt.expected {
					if val, ok := tt.entry.Body[key]; !ok || !val.Equal(expectedValue) {
						t.Fatalf("Expected %v for key `%s`, got %v", expectedValue, key, val)
					}
				}
			}
		})
	}
}

func TestPerformRemoveClause(t *testing.T) {
	tests := []struct {
		name                     string
		entry                    *Entry
		updateExpressionContent  string
		expressionAttributeNames map[string]string
		expected                 map[string]AttributeValue
		expectError              bool
	}{
		{
			name: "Remove simple attribute",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"year":  {N: aws.String("2024")},
					"title": {S: aws.String("Old Title")},
				},
			},
			updateExpressionContent: "REMOVE title",
			expected: map[string]AttributeValue{
				"year": {N: aws.String("2024")},
			},
			expectError: false,
		},
		{
			name: "Remove attribute from map",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"ProductReviews": {
						M: &map[string]AttributeValue{
							"FiveStar":  {S: aws.String("Excellent")},
							"ThreeStar": {S: aws.String("Average")},
						},
					},
				},
			},
			updateExpressionContent: "REMOVE ProductReviews.ThreeStar",
			expected: map[string]AttributeValue{
				"ProductReviews": {
					M: &map[string]AttributeValue{
						"FiveStar": {S: aws.String("Excellent")},
					},
				},
			},
			expectError: false,
		},
		{
			name: "Remove element from list",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"tags": {L: &[]AttributeValue{{S: aws.String("tag1")}, {S: aws.String("tag2")}}},
				},
			},
			updateExpressionContent: "REMOVE tags[0]",
			expected: map[string]AttributeValue{
				"tags": {L: &[]AttributeValue{{S: aws.String("tag2")}}},
			},
			expectError: false,
		},
		{
			name: "Remove non-existent attribute",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"year": {N: aws.String("2024")},
				},
			},
			updateExpressionContent: "REMOVE title",
			expected: map[string]AttributeValue{
				"year": {N: aws.String("2024")},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation, err := BuildUpdateOperation(
				tt.updateExpressionContent,
				tt.expressionAttributeNames,
				make(map[string]AttributeValue),
			)
			if err != nil {
				t.Fatalf("Unexpected error: %v, when build operation", err)
			}

			err = operation.Perform(tt.entry)
			if (err != nil) != tt.expectError {
				t.Fatalf("Expected error: %v, got: %v", tt.expectError, err)
			}

			if err == nil {
				for key, expectedValue := range tt.expected {
					if val, ok := tt.entry.Body[key]; !ok || !val.Equal(expectedValue) {
						t.Fatalf("Expected %v for key %s, got %v", expectedValue, key, val)
					}
				}
			}
		})
	}
}

func TestPerformAddClause(t *testing.T) {
	tests := []struct {
		name                      string
		entry                     *Entry
		updateExpressionContent   string
		expressionAttributeNames  map[string]string
		expressionAttributeValues map[string]AttributeValue
		expected                  map[string]AttributeValue
		expectError               bool
	}{
		{
			name: "Add to number attribute",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"views": {N: aws.String("100")},
				},
			},
			updateExpressionContent: "ADD views :increment",
			expressionAttributeValues: map[string]AttributeValue{
				":increment": {N: aws.String("10")},
			},
			expected: map[string]AttributeValue{
				"views": {N: aws.String("110")},
			},
			expectError: false,
		},
		{
			name: "Add to set attribute",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"tags": {SS: &[]string{"tag1", "tag2"}},
				},
			},
			updateExpressionContent: "ADD tags :newTags",
			expressionAttributeValues: map[string]AttributeValue{
				":newTags": {SS: &[]string{"tag2", "tag3", "tag4"}},
			},
			expected: map[string]AttributeValue{
				"tags": {SS: &[]string{"tag1", "tag2", "tag3", "tag4"}},
			},
			expectError: false,
		},
		{
			name: "Add to non-existent attribute",
			entry: &Entry{
				Body: map[string]AttributeValue{},
			},
			updateExpressionContent: "ADD views :increment",
			expressionAttributeValues: map[string]AttributeValue{
				":increment": {N: aws.String("10")},
			},
			expected: map[string]AttributeValue{
				"views": {N: aws.String("10")},
			},
			expectError: false,
		},
		{
			name: "Add to non-number attribute",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"views": {S: aws.String("one hundred")},
				},
			},
			updateExpressionContent: "ADD views :increment",
			expressionAttributeValues: map[string]AttributeValue{
				":increment": {N: aws.String("10")},
			},
			expected: map[string]AttributeValue{
				"views": {S: aws.String("one hundred")},
			},
			expectError: true,
		},
		{
			name: "Add to non-set attribute",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"tags": {S: aws.String("tag1")},
				},
			},
			updateExpressionContent: "ADD tags :newTags",
			expressionAttributeValues: map[string]AttributeValue{
				":newTags": {SS: &[]string{"tag2"}},
			},
			expected: map[string]AttributeValue{
				"tags": {S: aws.String("tag1")},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation, err := BuildUpdateOperation(
				tt.updateExpressionContent,
				tt.expressionAttributeNames,
				tt.expressionAttributeValues,
			)
			if err != nil {
				t.Fatalf("Unexpected error: %v, when build operation", err)
			}

			err = operation.Perform(tt.entry)
			if (err != nil) != tt.expectError {
				t.Fatalf("Expected error: %v, got: %v", tt.expectError, err)
			}

			if err == nil {
				for key, expectedValue := range tt.expected {
					if val, ok := tt.entry.Body[key]; !ok || !val.Equal(expectedValue) {
						t.Fatalf("Expected %v for key %s, got %v", expectedValue, key, val)
					}
				}
			}
		})
	}
}

func TestPerformDeleteClause(t *testing.T) {
	tests := []struct {
		name                      string
		entry                     *Entry
		updateExpressionContent   string
		expressionAttributeNames  map[string]string
		expressionAttributeValues map[string]AttributeValue
		expected                  map[string]AttributeValue
		expectError               bool
	}{
		{
			name: "Delete from set attribute",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"tags": {SS: &[]string{"tag1", "tag2", "tag3"}},
				},
			},
			updateExpressionContent: "DELETE tags :removeTags",
			expressionAttributeValues: map[string]AttributeValue{
				":removeTags": {SS: &[]string{"tag2"}},
			},
			expected: map[string]AttributeValue{
				"tags": {SS: &[]string{"tag1", "tag3"}},
			},
			expectError: false,
		},
		{
			name: "Delete non-existent element from set",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"tags": {SS: &[]string{"tag1", "tag2"}},
				},
			},
			updateExpressionContent: "DELETE tags :removeTags",
			expressionAttributeValues: map[string]AttributeValue{
				":removeTags": {SS: &[]string{"tag3"}},
			},
			expected: map[string]AttributeValue{
				"tags": {SS: &[]string{"tag1", "tag2"}},
			},
			expectError: false,
		},
		{
			name: "Delete from non-set attribute",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"views": {N: aws.String("100")},
				},
			},
			updateExpressionContent: "DELETE views :removeViews",
			expressionAttributeValues: map[string]AttributeValue{
				":removeViews": {N: aws.String("10")},
			},
			expected: map[string]AttributeValue{
				"views": {N: aws.String("100")},
			},
			expectError: true,
		},
		{
			name: "Delete from non-existent attribute",
			entry: &Entry{
				Body: map[string]AttributeValue{
					"views": {N: aws.String("100")},
				},
			},
			updateExpressionContent: "DELETE tags :removeTags",
			expressionAttributeValues: map[string]AttributeValue{
				":removeTags": {SS: &[]string{"tag1"}},
			},
			expected: map[string]AttributeValue{
				"views": {N: aws.String("100")},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation, err := BuildUpdateOperation(
				tt.updateExpressionContent,
				tt.expressionAttributeNames,
				tt.expressionAttributeValues,
			)
			if err != nil {
				t.Fatalf("Unexpected error: %v, when build operation", err)
			}

			err = operation.Perform(tt.entry)
			if (err != nil) != tt.expectError {
				t.Fatalf("Expected error: %v, got: %v", tt.expectError, err)
			}

			if err == nil {
				for key, expectedValue := range tt.expected {
					if val, ok := tt.entry.Body[key]; !ok || !val.Equal(expectedValue) {
						t.Fatalf("Expected %v for key %s, got %v", expectedValue, key, val)
					}
				}
			}
		})
	}
}

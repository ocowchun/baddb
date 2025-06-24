package integration

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"testing"
)

func TestPutItemBehavior(t *testing.T) {

	tests := []struct {
		name                      string
		condition                 *string
		expectErr                 bool
		existsItem                map[string]types.AttributeValue
		expressionAttributeNames  map[string]string
		expressionAttributeValues map[string]types.AttributeValue
	}{
		{
			name:       "normal insert",
			condition:  nil,
			expectErr:  false,
			existsItem: nil,
		},
		{
			name:       "conditional insert success",
			condition:  aws.String("attribute_not_exists(info)"),
			expectErr:  false,
			existsItem: nil,
		},
		{
			name:      "conditional insert fails",
			condition: aws.String("attribute_not_exists(info)"),
			expectErr: true,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.0"}}},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
		},
		{
			name:       "conditional insert fails due to reserved keyword",
			condition:  aws.String("attribute_not_exists(language)"),
			expectErr:  true,
			existsItem: nil,
		},
		// Comparison operators on rating (number)
		{
			name:      "rating equals success",
			condition: aws.String("info.rating = :rating"),
			expectErr: false,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.3"}}},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":rating": &types.AttributeValueMemberN{Value: "9.3"},
			},
		},
		{
			name:      "rating equals failure",
			condition: aws.String("info.rating = :rating"),
			expectErr: true,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.3"}}},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":rating": &types.AttributeValueMemberN{Value: "9.0"},
			},
		},
		{
			name:      "rating not equals success",
			condition: aws.String("info.rating <> :rating"),
			expectErr: false,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.3"}}},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":rating": &types.AttributeValueMemberN{Value: "5.3"},
			},
		},
		{
			name:      "rating not equals failure",
			condition: aws.String("info.rating <> :rating"),
			expectErr: true,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.3"}}},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":rating": &types.AttributeValueMemberN{Value: "9.3"},
			},
		},
		{
			name:      "rating greater than success",
			condition: aws.String("info.rating > :rating"),
			expectErr: false,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.3"}}},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":rating": &types.AttributeValueMemberN{Value: "9.0"},
			},
		},
		{
			name:      "rating greater than failure",
			condition: aws.String("info.rating > :rating"),
			expectErr: true,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.3"}}},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":rating": &types.AttributeValueMemberN{Value: "9.9"},
			},
		},
		{
			name:      "rating less than success",
			condition: aws.String("info.rating < :rating"),
			expectErr: false,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.3"}}},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":rating": &types.AttributeValueMemberN{Value: "9.5"},
			},
		},
		{
			name:      "rating less than failure",
			condition: aws.String("info.rating < :rating"),
			expectErr: true,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.3"}}},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":rating": &types.AttributeValueMemberN{Value: "9.0"},
			},
		},
		// Attribute functions
		{
			name:      "attribute_exists success",
			condition: aws.String("attribute_exists(#language)"),
			expectErr: false,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeNames: map[string]string{
				"#language": "language",
			},
		},
		{
			name:      "attribute_exists failure",
			condition: aws.String("attribute_exists(director)"),
			expectErr: true,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
		},
		{
			name:      "attribute_type success",
			condition: aws.String("attribute_type(#language, :type)"),
			expectErr: false,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeNames: map[string]string{
				"#language": "language",
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":type": &types.AttributeValueMemberS{Value: "S"}, // S for String
			},
		},
		{
			name:      "attribute_type failure",
			condition: aws.String("attribute_type(#language, :type)"),
			expectErr: true,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeNames: map[string]string{
				"#language": "language",
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":type": &types.AttributeValueMemberS{Value: "N"}, // S for String
			},
		},
		{
			name:      "begins_with success",
			condition: aws.String("begins_with(#language, :prefix)"),
			expectErr: false,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeNames: map[string]string{
				"#language": "language",
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":prefix": &types.AttributeValueMemberS{Value: "En"},
			},
		},
		{
			name:      "begins_with failure",
			condition: aws.String("begins_with(#language, :wrong_prefix)"),
			expectErr: true,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeNames: map[string]string{
				"#language": "language",
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":wrong_prefix": &types.AttributeValueMemberS{Value: "Jp"},
			},
		},
		{
			name:      "contains success",
			condition: aws.String("contains(stars, :star)"),
			expectErr: false,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
				"stars": &types.AttributeValueMemberSS{
					Value: []string{"Tim Robbins", "Morgan Freeman"},
				},
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":star": &types.AttributeValueMemberS{Value: "Tim Robbins"},
			},
		},
		{
			name:      "contains failure",
			condition: aws.String("contains(stars, :star)"),
			expectErr: true,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
				"stars": &types.AttributeValueMemberSS{
					Value: []string{"Tim Robbins", "Morgan Freeman"},
				},
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":star": &types.AttributeValueMemberS{Value: "Tom Hanks"},
			},
		},
		{
			name:      "size function success",
			condition: aws.String("size(#language) > :min_size"),
			expectErr: false,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeNames: map[string]string{
				"#language": "language",
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":min_size": &types.AttributeValueMemberN{Value: "5"}, // English has 7 characters
			},
		},
		{
			name:      "size function failure",
			condition: aws.String("size(#language) > :max_size"),
			expectErr: true,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeNames: map[string]string{
				"#language": "language",
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":max_size": &types.AttributeValueMemberN{Value: "500"}, // English has 7 characters
			},
		},
		// BETWEEN and IN operators
		{
			name:      "between success",
			condition: aws.String("info.rating BETWEEN :low AND :high"),
			expectErr: false,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.0"}}},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":low":  &types.AttributeValueMemberN{Value: "8.0"},
				":high": &types.AttributeValueMemberN{Value: "9.5"},
			},
		},
		{
			name:      "between failure",
			condition: aws.String("info.rating BETWEEN :high_low AND :high_high"),
			expectErr: true,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.0"}}},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":high_low":  &types.AttributeValueMemberN{Value: "9.5"},
				":high_high": &types.AttributeValueMemberN{Value: "10.0"},
			},
		},
		{
			name:      "in list success",
			condition: aws.String("#language IN (:lang1, :lang2)"),
			expectErr: false,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeNames: map[string]string{
				"#language": "language",
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":lang1": &types.AttributeValueMemberS{Value: "English"},
				":lang2": &types.AttributeValueMemberS{Value: "French"},
			},
		},
		{
			name:      "in list failure",
			condition: aws.String("#language IN (:lang1, :lang2)"),
			expectErr: true,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeNames: map[string]string{
				"#language": "language",
			},
			expressionAttributeValues: map[string]types.AttributeValue{
				":lang1": &types.AttributeValueMemberS{Value: "Spanish"},
				":lang2": &types.AttributeValueMemberS{Value: "French"},
			},
		},
		// Logical operators
		{
			name:      "and success",
			condition: aws.String("attribute_exists(#language) AND attribute_exists(info)"),
			expectErr: false,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.0"}}},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeNames: map[string]string{
				"#language": "language",
			},
		},
		{
			name:      "and failure",
			condition: aws.String("attribute_exists(#language) AND attribute_exists(director)"),
			expectErr: true,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeNames: map[string]string{
				"#language": "language",
			},
		},
		{
			name:      "or success",
			condition: aws.String("attribute_exists(#language) OR attribute_exists(director)"),
			expectErr: false,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeNames: map[string]string{
				"#language": "language",
			},
		},
		{
			name:      "or failure",
			condition: aws.String("attribute_exists(director) OR attribute_exists(producer)"),
			expectErr: true,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
		},
		{
			name:      "not success",
			condition: aws.String("NOT attribute_exists(director)"),
			expectErr: false,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
		},
		{
			name:      "not failure",
			condition: aws.String("NOT attribute_exists(language)"),
			expectErr: true,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
		},
		// Complex expressions with parentheses
		{
			name:      "complex expression success",
			condition: aws.String("(attribute_exists(#language) AND attribute_exists(info)) OR attribute_not_exists(director)"),
			expectErr: false,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.0"}}},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
			expressionAttributeNames: map[string]string{
				"#language": "language",
			},
		},
		{
			name:      "complex expression failure",
			condition: aws.String("(attribute_exists(director) AND attribute_exists(producer)) OR attribute_exists(studio)"),
			expectErr: true,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "1994"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
		},
	}
	newItem := map[string]types.AttributeValue{
		"year":     &types.AttributeValueMemberN{Value: "1994"},
		"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
		"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.9"}}},
		"language": &types.AttributeValueMemberS{Value: "English"},
	}

	for _, tt := range tests {
		ddbLocal := newDdbLocalClient()
		baddb := newBaddbClient()
		cleanDdbLocal(ddbLocal)
		shutdown := startServer()

		_, ddbErr := createTable(ddbLocal)
		_, baddbErr := createTable(baddb)
		if ddbErr != nil || baddbErr != nil {
			t.Fatalf("failed to create table: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
		}

		t.Run(tt.name, func(t *testing.T) {
			if tt.existsItem != nil {
				input := &dynamodb.PutItemInput{
					TableName: aws.String(TestTableName),
					Item:      tt.existsItem,
				}
				_, err := putItem(ddbLocal, input)
				if err != nil {
					t.Fatalf("failed to put existing item in ddbLocal: %v", err)
				}
				_, err = putItem(baddb, input)
				if err != nil {
					t.Fatalf("failed to put existing item in baddb: %v", err)
				}
			}

			input := &dynamodb.PutItemInput{
				TableName: aws.String(TestTableName),
				Item:      newItem,
			}
			if tt.condition != nil {
				input.ConditionExpression = tt.condition
				input.ExpressionAttributeNames = tt.expressionAttributeNames
				input.ExpressionAttributeValues = tt.expressionAttributeValues
			}

			ddbOut, ddbErr := putItem(ddbLocal, input)
			baddbOut, baddbErr := putItem(baddb, input)

			if (ddbErr != nil) != tt.expectErr {
				t.Errorf("ddbLocal: expected error=%v, got %v", tt.expectErr, ddbErr)
			}
			if (baddbErr != nil) != tt.expectErr {
				t.Errorf("baddb: expected error=%v, got %v", tt.expectErr, baddbErr)
			}

			// Compare error types if both errored
			if tt.expectErr && ddbErr != nil && baddbErr != nil {
				if !compareWithoutRequestID(ddbErr.Error(), baddbErr.Error()) {
					t.Errorf("expected errors to match, ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
				}
			}

			if !tt.expectErr {
				comparePutItemOutput(ddbOut, baddbOut, t)
			}
		})

		// Clean up for next test case
		shutdown()
	}
}

func putItem(client *dynamodb.Client, input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return client.PutItem(context.TODO(), input)
}

// comparePutItemOutput compares the Attributes maps of two PutItemOutput objects without using reflection.
func comparePutItemOutput(ddbOutput *dynamodb.PutItemOutput, baddbOutput *dynamodb.PutItemOutput, t *testing.T) {
	ddbAttrs := ddbOutput.Attributes
	baddbAttrs := baddbOutput.Attributes

	compareItem(ddbAttrs, baddbAttrs, t)
}

package integration

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ocowchun/baddb/server"
	"log"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"testing"
)

func cleanDdbLocal(client *dynamodb.Client) {
	// Clean up the table
	_, err := client.DeleteTable(context.TODO(), &dynamodb.DeleteTableInput{
		TableName: aws.String("movie"),
	})
	if err != nil && !strings.Contains(err.Error(), "Cannot do operations on a non-existent table") {
		log.Fatalf("failed to delete table from ddb-local, %v", err)
	}

}

func newBaddbClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-west-2"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Using the Config value, create the DynamoDB client
	client := dynamodb.NewFromConfig(cfg, func(options *dynamodb.Options) {
		options.BaseEndpoint = aws.String("http://localhost:8080")
	})

	return client
}

func newDdbLocalClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-west-2"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Using the Config value, create the DynamoDB client
	client := dynamodb.NewFromConfig(cfg, func(options *dynamodb.Options) {
		options.BaseEndpoint = aws.String("http://localhost:8000")
	})

	return client
}

func createTable(client *dynamodb.Client) (*dynamodb.CreateTableOutput, error) {
	createTableInput := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{{
			AttributeName: aws.String("year"),
			AttributeType: types.ScalarAttributeTypeN,
		}, {
			AttributeName: aws.String("title"),
			AttributeType: types.ScalarAttributeTypeS,
		}, {
			AttributeName: aws.String("language"),
			AttributeType: types.ScalarAttributeTypeS,
		},
		},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("year"),
			KeyType:       types.KeyTypeHash,
		}, {
			AttributeName: aws.String("title"),
			KeyType:       types.KeyTypeRange,
		}},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName: aws.String("gsiLanguage"),
			KeySchema: []types.KeySchemaElement{{
				AttributeName: aws.String("language"),
				KeyType:       types.KeyTypeHash,
			},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			// TODO: add check
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(50),
				WriteCapacityUnits: aws.Int64(50),
			},
		}},
		TableName: aws.String("movie"),
		//BillingMode: types.BillingModePayPerRequest,

		BillingMode: types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(50),
			WriteCapacityUnits: aws.Int64(50),
		},
	}
	output, err := client.CreateTable(context.TODO(), createTableInput)
	if err != nil {
		return nil, err
	}

	return output, nil
}

func startServer() func() {
	svr := server.NewDdbServer()
	mux := http.NewServeMux()
	mux.HandleFunc("/", svr.Handler)

	port := 8080
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	log.Printf("baddb server is running on port %d...", port)

	go func() {
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("Server error: %v\n", err)
		}

	}()

	return func() {
		err := server.Shutdown(context.Background())
		if err != nil {
			log.Printf("Server error: %v\n", err)
		}
	}
}

func compareItem(ddbItem map[string]types.AttributeValue, baddbItem map[string]types.AttributeValue, t *testing.T) {
	if len(ddbItem) != len(baddbItem) {
		t.Errorf("Item length differ: ddbLocal=%d, baddb=%d", len(ddbItem), len(baddbItem))
		return
	}

	for k, v := range ddbItem {
		bv, ok := baddbItem[k]
		if !ok {
			t.Errorf("Key %q present in ddbLocal but missing in baddb", k)
			continue
		}
		if !compareAttributeValue(v, bv, t, k) {
			t.Errorf("Attribute value mismatch for key %q: ddbLocal=%#v, baddb=%#v", k, v, bv)
		}
	}

	for k := range baddbItem {
		if _, ok := ddbItem[k]; !ok {
			t.Errorf("Key %q present in baddb but missing in ddbLocal", k)
		}
	}

}

// compareAttributeValue compares two types.AttributeValue for equality.
func compareAttributeValue(a, b types.AttributeValue, t *testing.T, key string) bool {
	switch av := a.(type) {
	case *types.AttributeValueMemberS:
		bv, ok := b.(*types.AttributeValueMemberS)
		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberN:
		bv, ok := b.(*types.AttributeValueMemberN)
		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberBOOL:
		bv, ok := b.(*types.AttributeValueMemberBOOL)
		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberM:
		bv, ok := b.(*types.AttributeValueMemberM)
		if !ok || len(av.Value) != len(bv.Value) {
			return false
		}
		for mk, mv := range av.Value {
			bmv, ok := bv.Value[mk]
			if !ok || !compareAttributeValue(mv, bmv, t, key+"."+mk) {
				return false
			}
		}
		for mk := range bv.Value {
			if _, ok := av.Value[mk]; !ok {
				return false
			}
		}
		return true
	case *types.AttributeValueMemberL:
		bv, ok := b.(*types.AttributeValueMemberL)
		if !ok || len(av.Value) != len(bv.Value) {
			return false
		}
		for i := range av.Value {
			if !compareAttributeValue(av.Value[i], bv.Value[i], t, key) {
				return false
			}
		}
		return true
	case *types.AttributeValueMemberNULL:
		_, ok := b.(*types.AttributeValueMemberNULL)
		return ok
	default:
		t.Errorf("Unsupported AttributeValue type for key %q", key)
		return false
	}
}

func compareWithoutRequestID(s1, s2 string) bool {
	re := regexp.MustCompile(`RequestID: [\w-]+,? ?`)
	clean1 := re.ReplaceAllString(s1, "")
	clean2 := re.ReplaceAllString(s2, "")
	return strings.TrimSpace(clean1) == strings.TrimSpace(clean2)
}

func compareItems(ddbItems, baddbItems []map[string]types.AttributeValue, t *testing.T) {
	if len(ddbItems) != len(baddbItems) {
		t.Errorf("Scan item count differ: ddbLocal=%d, baddb=%d", len(ddbItems), len(baddbItems))
		return
	}

	sortItems(ddbItems)
	sortItems(baddbItems)
	for i := range ddbItems {
		compareItem(ddbItems[i], baddbItems[i], t)
	}
}

func sortItems(items []map[string]types.AttributeValue) {
	sort.Slice(items, func(i, j int) bool {
		yearI := items[i]["year"].(*types.AttributeValueMemberN).Value
		yearJ := items[j]["year"].(*types.AttributeValueMemberN).Value
		if yearI != yearJ {
			return yearI < yearJ
		}
		titleI := items[i]["title"].(*types.AttributeValueMemberS).Value
		titleJ := items[j]["title"].(*types.AttributeValueMemberS).Value
		return titleI < titleJ
	})
}

type TestContext struct {
	ddbLocal *dynamodb.Client
	baddb    *dynamodb.Client
	shutdown func()
}

func setupTest(t *testing.T) *TestContext {
	ddbLocal := newDdbLocalClient()
	baddb := newBaddbClient()
	cleanDdbLocal(ddbLocal)
	shutdown := startServer()

	_, ddbErr := createTable(ddbLocal)
	_, baddbErr := createTable(baddb)
	if ddbErr != nil || baddbErr != nil {
		t.Fatalf("failed to create table: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	return &TestContext{
		ddbLocal: ddbLocal,
		baddb:    baddb,
		shutdown: shutdown,
	}

}

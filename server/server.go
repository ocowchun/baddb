package server

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/ocowchun/baddb/ddb"
	"github.com/ocowchun/baddb/encoding"
	"hash/crc32"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
)

type ErrorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"Message"`
}

func handleDdbError(w http.ResponseWriter, outputErr error) {
	var resourceInUseException *types.ResourceInUseException
	var resourceNotFoundException *types.ResourceNotFoundException
	var validationException *ddb.ValidationException
	var provisionedThroughputExceededException *types.ProvisionedThroughputExceededException
	switch {

	case errors.As(outputErr, &resourceInUseException):
		w.WriteHeader(http.StatusBadRequest)
		errResponse := ErrorResponse{
			Type:    "ResourceInUseException",
			Message: resourceInUseException.ErrorMessage(),
		}

		bs, err := json.Marshal(errResponse)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			// TODO describe reason
			return
		}
		_, err = w.Write(bs)
		if err != nil {
			log.Printf("Error writing response: %v", err)
			return
		}

	case errors.As(outputErr, &resourceNotFoundException):
		w.WriteHeader(http.StatusBadRequest)
		errResponse := ErrorResponse{
			Type:    "ResourceNotFoundException",
			Message: resourceNotFoundException.ErrorMessage(),
		}

		bs, err := json.Marshal(errResponse)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			// TODO describe reason
			return
		}
		_, err = w.Write(bs)
		if err != nil {
			log.Printf("Error writing response: %v", err)
			return
		}

		return

	case errors.As(outputErr, &provisionedThroughputExceededException):
		w.WriteHeader(http.StatusBadRequest)
		errResponse := ErrorResponse{
			Type:    "ProvisionedThroughputExceededException",
			Message: provisionedThroughputExceededException.ErrorMessage(),
		}

		bs, err := json.Marshal(errResponse)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			// TODO describe reason
			return
		}
		_, err = w.Write(bs)
		if err != nil {
			log.Printf("Error writing response: %v", err)
			return
		}

		return
	case errors.As(outputErr, &validationException):
		w.WriteHeader(http.StatusBadRequest)
		errResponse := ErrorResponse{
			Type:    "com.amazon.coral.validate#ValidationException",
			Message: validationException.Error(),
		}

		bs, err := json.Marshal(errResponse)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, err = w.Write(bs)
		if err != nil {
			log.Printf("Error writing response: %v", err)
			return
		}

		return

	}
}

func genericHandler(
	w http.ResponseWriter,
	req *http.Request,
	decodeInput func(bs io.ReadCloser) (interface{}, error),
	handle func(ctx context.Context, input interface{}) (interface{}, error),
	encodeOutput func(interface{}) ([]byte, error),
) {
	input, err := decodeInput(req.Body)

	if err != nil {
		log.Printf("Error reading request body: %v", err)
		http.Error(w, "Failed to parse request body", http.StatusInternalServerError)
		return
	}

	output, err := handle(context.Background(), input)
	if err != nil {
		handleDdbError(w, err)
		return
	}

	bs, err := encodeOutput(output)
	if err != nil {
		handleDdbError(w, err)
		return
	}

	writeResHeaders(bs, w)
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(bs)

	if err != nil {
		log.Printf("Error writing response: %v", err)
		return
	}
}

func writeResHeaders(bs []byte, w http.ResponseWriter) {
	crc32Code := crc32.ChecksumIEEE(bs)
	w.Header().Add("X-Amz-Crc32", strconv.FormatUint(uint64(crc32Code), 10))
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
}

type Foo struct {
	Message string `json:"message"`
}

func (svr *DdbServer) Handler(w http.ResponseWriter, req *http.Request) {
	targetActions := req.Header["X-Amz-Target"]
	if len(targetActions) != 1 {
		w.WriteHeader(http.StatusBadRequest)
		// TODO: describe invalid target
		return
	}

	targetAction := strings.Replace(targetActions[0], "DynamoDB_20120810.", "", -1)

	id := uuid.New()
	w.Header().Set("X-Amzn-Requestid", id.String())
	log.Println("received request", id.String(), targetAction)
	switch targetAction {
	case "BatchGetItem":
		genericHandler(
			w,
			req,
			func(bs io.ReadCloser) (interface{}, error) {
				return encoding.DecodeBatchGetItemInput(bs)
			},
			func(ctx context.Context, input interface{}) (interface{}, error) {
				return svr.inner.BatchGetItem(ctx, input.(*dynamodb.BatchGetItemInput))
			},
			func(i interface{}) ([]byte, error) {
				return encoding.EncodeBatchGetItemOutput(i.(*dynamodb.BatchGetItemOutput))
			},
		)
	case "BatchWriteItem":
		genericHandler(
			w,
			req,
			func(bs io.ReadCloser) (interface{}, error) {
				return encoding.DecodeBatchWriteItemInput(bs)
			},
			func(ctx context.Context, input interface{}) (interface{}, error) {
				return svr.inner.BatchWriteItem(ctx, input.(*dynamodb.BatchWriteItemInput))
			},
			func(i interface{}) ([]byte, error) {
				return encoding.EncodeBatchWriteItemOutput(i.(*dynamodb.BatchWriteItemOutput))
			},
		)

	case "ListTables":
		genericHandler(
			w,
			req,
			func(bs io.ReadCloser) (interface{}, error) {
				return encoding.DecodeListTablesInput(bs)
			},
			func(ctx context.Context, input interface{}) (interface{}, error) {
				return svr.inner.ListTables(ctx, input.(*dynamodb.ListTablesInput))
			},
			func(i interface{}) ([]byte, error) {
				return encoding.EncodeListTablesOutput(i.(*dynamodb.ListTablesOutput))
			},
		)
	case "CreateTable":
		genericHandler(
			w,
			req,
			func(bs io.ReadCloser) (interface{}, error) {
				return encoding.DecodeCreateTableInput(bs)
			},
			func(ctx context.Context, input interface{}) (interface{}, error) {
				return svr.inner.CreateTable(ctx, input.(*dynamodb.CreateTableInput))
			},
			func(i interface{}) ([]byte, error) {
				return encoding.EncodeCreateTableOutput(i.(*dynamodb.CreateTableOutput))
			},
		)
	case "DescribeTable":
		genericHandler(
			w,
			req,
			func(bs io.ReadCloser) (interface{}, error) {
				return encoding.DecodeDescribeTableInput(bs)
			},
			func(ctx context.Context, input interface{}) (interface{}, error) {
				return svr.inner.DescribeTable(ctx, input.(*dynamodb.DescribeTableInput))
			},
			func(i interface{}) ([]byte, error) {
				return encoding.EncodeDescribeTableOutput(i.(*dynamodb.DescribeTableOutput))
			},
		)
	case "DeleteTable":
		genericHandler(
			w,
			req,
			func(bs io.ReadCloser) (interface{}, error) {
				return encoding.DecodingDeleteTableInput(bs)
			},
			func(ctx context.Context, input interface{}) (interface{}, error) {
				return svr.inner.DeleteTable(ctx, input.(*dynamodb.DeleteTableInput))
			},
			func(i interface{}) ([]byte, error) {
				return encoding.EncodeDeleteTableOutput(i.(*dynamodb.DeleteTableOutput))
			},
		)
	case "PutItem":
		genericHandler(
			w,
			req,
			func(bs io.ReadCloser) (interface{}, error) {
				return encoding.DecodePutItemInput(bs)
			},
			func(ctx context.Context, input interface{}) (interface{}, error) {
				return svr.inner.PutItem(ctx, input.(*dynamodb.PutItemInput))
			},
			func(i interface{}) ([]byte, error) {
				return encoding.EncodePutItemOutput(i.(*dynamodb.PutItemOutput))
			},
		)
	case "GetItem":
		genericHandler(
			w,
			req,
			func(bs io.ReadCloser) (interface{}, error) {
				return encoding.DecodeGetItemInput(bs)
			},
			func(ctx context.Context, input interface{}) (interface{}, error) {
				return svr.inner.GetItem(ctx, input.(*dynamodb.GetItemInput))
			},
			func(i interface{}) ([]byte, error) {
				return encoding.EncodeGetItemOutput(i.(*dynamodb.GetItemOutput))
			},
		)
	case "DeleteItem":
		genericHandler(
			w,
			req,
			func(bs io.ReadCloser) (interface{}, error) {
				return encoding.DecodeDeleteItemInput(bs)
			},
			func(ctx context.Context, input interface{}) (interface{}, error) {
				return svr.inner.DeleteItem(ctx, input.(*dynamodb.DeleteItemInput))
			},
			func(i interface{}) ([]byte, error) {
				return encoding.EncodeDeleteItemOutput(i.(*dynamodb.DeleteItemOutput))
			},
		)

	case "Query":
		genericHandler(
			w,
			req,
			func(bs io.ReadCloser) (interface{}, error) {
				return encoding.DecodeQueryInput(bs)
			},
			func(ctx context.Context, input interface{}) (interface{}, error) {
				return svr.inner.Query(ctx, input.(*dynamodb.QueryInput))
			},
			func(i interface{}) ([]byte, error) {
				return encoding.EncodeQueryOutput(i.(*dynamodb.QueryOutput))
			},
		)
	case "TransactWriteItems":
		genericHandler(
			w,
			req,
			func(bs io.ReadCloser) (interface{}, error) {
				return encoding.DecodeTransactWriteItemsInput(bs)
			},
			func(ctx context.Context, input interface{}) (interface{}, error) {
				return svr.inner.TransactWriteItems(ctx, input.(*dynamodb.TransactWriteItemsInput))
			},
			func(i interface{}) ([]byte, error) {
				return encoding.EncodeTransactWriteItemsOutput(i.(*dynamodb.TransactWriteItemsOutput))
			},
		)
	default:
		w.WriteHeader(http.StatusBadRequest)
		return
	}
}

type DdbServer struct {
	inner *ddb.Service
}

func NewDdbServer() *DdbServer {
	return &DdbServer{
		inner: ddb.NewDdbService(),
	}
}

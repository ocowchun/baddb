FROM golang:1.23-alpine AS builder
ENV CGO_ENABLED=1

WORKDIR /app

RUN apk add --no-cache build-base sqlite-libs

COPY go.mod go.sum ./
RUN go mod download

COPY cli/ cli/
COPY ddb/ ddb/
COPY expression/ expression/
COPY server/ server/

RUN go build ./cli/baddb

## Final image
FROM alpine:latest

WORKDIR /app

# Copy the binary from the builder
COPY --from=builder /app/baddb .

EXPOSE 9527

CMD ["./baddb"]
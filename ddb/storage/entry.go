package storage

import (
	"time"

	"github.com/ocowchun/baddb/ddb/core"
)

type EntryWrapper struct {
	Entry     *core.Entry
	IsDeleted bool
	CreatedAt time.Time
}

type EntryWithKey struct {
	Key   []byte
	Entry *core.Entry
}

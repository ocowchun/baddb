package inner_storage

import (
	"github.com/ocowchun/baddb/ddb/core"
	"time"
)

type Tuple struct {
	Entries []EntryWrapper
}

// return prevEntry, found
func (t *Tuple) prevEntry() *core.Entry {
	if len(t.Entries) < 2 {
		return nil
	} else {
		prevEntry := t.Entries[0]
		if prevEntry.IsDeleted {
			return nil
		}
		return prevEntry.Entry
	}
}

func (t *Tuple) getEntry(consistentRead bool, readTs time.Time, isGsi bool) *core.Entry {
	if len(t.Entries) == 2 {
		if (!isGsi && consistentRead) || t.Entries[1].CreatedAt.Before(readTs) {
			if t.Entries[1].IsDeleted {
				return nil
			}
			return t.Entries[1].Entry
		} else {
			if t.Entries[0].IsDeleted {
				return nil
			}
			return t.Entries[0].Entry
		}
	} else if len(t.Entries) == 1 {
		if (!isGsi && consistentRead) || t.Entries[0].CreatedAt.Before(readTs) {
			if t.Entries[0].IsDeleted {
				return nil
			}
			return t.Entries[0].Entry
		} else {
			return nil
		}
	} else {
		// no entry
		return nil
	}
}

// return lastEntry, found
func (t *Tuple) currentEntry() *core.Entry {
	if len(t.Entries) == 0 {
		return nil
	} else {
		lastEntry := t.Entries[len(t.Entries)-1]
		if lastEntry.IsDeleted {
			return nil
		}
		return lastEntry.Entry
	}
}

// addEntry only keep last 2 Entries
func (t *Tuple) addEntry(entryWrapper *EntryWrapper) {

	t.Entries = append(t.Entries, *entryWrapper)
	if len(t.Entries) > 2 {
		t.Entries = t.Entries[1:]
	}
}

package inner_storage

import (
	"bytes"
	"fmt"
)

type PrimaryKey struct {
	PartitionKey []byte
	SortKey      []byte
}

func (k *PrimaryKey) Bytes() []byte {
	bs := make([]byte, 0)
	bs = append(bs, k.PartitionKey...)

	//	// TODO: need a better way to generate primary key, current approach will failed for below case
	//	// pk: ab, sk: |cd and pk: ab|, sk: cd
	if len(k.SortKey) > 0 {
		bs = append(bs, []byte("|")...)
		bs = append(bs, k.SortKey...)
	}

	return bs
}

func (k *PrimaryKey) String() string {
	var out bytes.Buffer
	out.WriteString(fmt.Sprintf("(PartitionKey: '%s'", string(k.PartitionKey)))
	if len(k.SortKey) > 0 {
		out.WriteString(fmt.Sprintf(", SortKey: '%s'", string(k.SortKey)))
	}
	out.WriteString(")")
	return out.String()
}

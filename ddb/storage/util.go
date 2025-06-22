package storage

import "hash/fnv"

// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#DDB-Scan-request-TotalSegments
const TOTAL_SEGMENTS = 1000000

func buildShardId(bs []byte) int32 {
	h := fnv.New32a()
	h.Write(bs)
	return int32(h.Sum32() % TOTAL_SEGMENTS) // for testing purpose, shard id is 0-999
}

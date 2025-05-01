package core

type ProjectionType uint8

const (
	PROJECTION_TYPE_KEYS_ONLY ProjectionType = iota
	PROJECTION_TYPE_INCLUDE
	PROJECTION_TYPE_ALL
)

package utils

import "hash/crc32"

// meta
const (
	BitDelete       byte = 1 << 0 // Set if the key has been deleted.
	BitValuePointer byte = 1 << 1 // Set if the value is NOT stored directly next to key.
)

// codec
var (
	MagicText    = [4]byte{'H', 'A', 'R', 'D'}
	MagicVersion = uint32(1)
	// CastagnoliCrcTable is a CRC32 polynomial table
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)

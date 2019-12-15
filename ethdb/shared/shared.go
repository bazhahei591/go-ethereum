package shared

import (
	"bytes"
	"github.com/ethereum/go-ethereum/common"
)

var (
	// databaseVerisionKey tracks the current database version.
	databaseVerisionKey = []byte("DatabaseVersion")

	// headHeaderKey tracks the latest known header's hash.
	headHeaderKey = []byte("LastHeader")

	// headBlockKey tracks the latest known full block's hash.
	headBlockKey = []byte("LastBlock")

	// headFastBlockKey tracks the latest known incomplete block's hash during fast sync.
	headFastBlockKey = []byte("LastFast")

	// fastTrieProgressKey tracks the number of trie entries imported during fast sync.
	fastTrieProgressKey = []byte("TrieSync")

	// Data item prefixes (use single byte to avoid mixing data types, avoid `i`, used for indexes).
	headerPrefix       = []byte("h") // headerPrefix + num (uint64 big endian) + hash -> header
	headerTDSuffix     = []byte("t") // headerPrefix + num (uint64 big endian) + hash + headerTDSuffix -> td
	headerHashSuffix   = []byte("n") // headerPrefix + num (uint64 big endian) + headerHashSuffix -> hash
	headerNumberPrefix = []byte("H") // headerNumberPrefix + hash -> num (uint64 big endian)

	blockBodyPrefix     = []byte("b") // blockBodyPrefix + num (uint64 big endian) + hash -> block body
	blockReceiptsPrefix = []byte("r") // blockReceiptsPrefix + num (uint64 big endian) + hash -> block receipts

	txLookupPrefix  = []byte("l") // txLookupPrefix + hash -> transaction/receipt lookup metadata
	bloomBitsPrefix = []byte("B") // bloomBitsPrefix + bit (uint16 big endian) + section (uint64 big endian) + hash -> bloom bits

	preimagePrefix = []byte("secure-key-")      // preimagePrefix + hash -> preimage
	configPrefix   = []byte("ethereum-config-") // config prefix for the db

	// Chain index prefixes (use `i` + single byte to avoid mixing data types).

	// BloomBitsIndexPrefix is the data table of a chain indexer to track its progress
	BloomBitsIndexPrefix = []byte("iB")

)

func KeyType(key []byte) (string, bool) {
	switch {
	case bytes.HasPrefix(key, headerPrefix) && bytes.HasSuffix(key, headerTDSuffix):
		return "h-td", true
	case bytes.HasPrefix(key, headerPrefix) && bytes.HasSuffix(key, headerHashSuffix):
		return "h-hash", false
	case bytes.HasPrefix(key, headerPrefix) && len(key) == (len(headerPrefix)+8+common.HashLength):
		return "h", true
	case bytes.HasPrefix(key, headerNumberPrefix) && len(key) == (len(headerNumberPrefix)+common.HashLength):
		return "H-num", true
	case bytes.HasPrefix(key, blockBodyPrefix) && len(key) == (len(blockBodyPrefix)+8+common.HashLength):
		return "body", true
	case bytes.HasPrefix(key, blockReceiptsPrefix) && len(key) == (len(blockReceiptsPrefix)+8+common.HashLength):
		return "receipt", true
	case bytes.HasPrefix(key, txLookupPrefix) && len(key) == (len(txLookupPrefix)+common.HashLength):
		return "tx", false
	case bytes.HasPrefix(key, preimagePrefix) && len(key) == (len(preimagePrefix)+common.HashLength):
		return "preimage", false // must be false
	case bytes.HasPrefix(key, bloomBitsPrefix) && len(key) == (len(bloomBitsPrefix)+10+common.HashLength):
		return "bloom", false
	case bytes.HasPrefix(key, []byte("clique-")) && len(key) == 7+common.HashLength:
		return "clique", false
	case bytes.HasPrefix(key, []byte("cht-")) && len(key) == 4+common.HashLength:
		return "cht", false
	case bytes.HasPrefix(key, []byte("blt-")) && len(key) == 4+common.HashLength:
		return "blt", false
	case len(key) == common.HashLength:
		return "hash", false // must be false
	default:
		return "unknown", false
	}
}

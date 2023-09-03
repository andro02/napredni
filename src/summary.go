package src

import (
	"encoding/binary"
	"os"
)

func ReadSummaryRow(summaryFile *os.File) *IndexEntry {

	summaryEntry := NewIndexEntry()

	keySize := make([]byte, 8)
	_, err := summaryFile.Read(keySize)
	if err != nil {
		panic(err)
	}
	summaryEntry.KeySize = binary.LittleEndian.Uint64(keySize)

	key := make([]byte, summaryEntry.KeySize)
	_, err = summaryFile.Read(key)
	if err != nil {
		panic(err)
	}
	summaryEntry.Key = key

	offset := make([]byte, 4)
	_, err = summaryFile.Read(offset)
	if err != nil {
		panic(err)
	}
	summaryEntry.Offset = binary.LittleEndian.Uint32(offset)

	return summaryEntry

}

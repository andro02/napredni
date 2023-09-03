package src

import (
	"encoding/binary"
	"os"
)

func CreateSummary(indexFile *os.File, filepath string, indexSize uint32) {

	indexFile.Seek(0, 0)
	summaryFile, err := os.Create(filepath)
	if err != nil {
		panic(err)
	}

	var i uint32 = 0
	var offset uint32 = 0
	summaryEntries := make([]*IndexEntry, 0)

	for offset != indexSize {
		indexEntry, indexEntrySize := ReadIndexRow(indexFile)

		if i%10 == 0 {
			summaryEntries = append(summaryEntries, indexEntry)
			summaryEntries[len(summaryEntries)-1].Offset = offset
		}
		i++
		offset += indexEntrySize
	}

	WriteSummaryRow(summaryEntries[0], summaryFile)
	WriteSummaryRow(summaryEntries[len(summaryEntries)-1], summaryFile)
	for _, summaryEntry := range summaryEntries {
		WriteSummaryRow(summaryEntry, summaryFile)
	}
	summaryFile.Close()

}

func WriteSummaryRow(indexEntry *IndexEntry, summaryFile *os.File) uint32 {

	keySize := make([]byte, 8)
	binary.LittleEndian.PutUint64(keySize, indexEntry.KeySize)
	summaryFile.Write(keySize)

	summaryFile.Write(indexEntry.Key)

	offsetBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(offsetBytes, indexEntry.Offset)
	summaryFile.Write(offsetBytes)

	return uint32(len(keySize) + len(indexEntry.Key) + len(offsetBytes))

}
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

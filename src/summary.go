package src

import (
	"encoding/binary"
	"os"

	"github.com/andro02/napredni/config"
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

		if int(i)%config.SSTABLE_SEGMENT_SIZE == 0 || offset+indexEntrySize == indexSize {
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

func CreateSummarySingle(file *os.File, indexStart int64, summaryStart int64) {

	file.Seek(indexStart, 0)

	indexSizeBytes := make([]byte, 4)
	_, err := file.Read(indexSizeBytes)
	if err != nil {
		panic(err)
	}

	indexSize := binary.LittleEndian.Uint32(indexSizeBytes)
	i := 0
	var currentSize uint32 = 0
	summaryEntries := make([]*IndexEntry, 0)

	for currentSize != indexSize {
		indexEntry, indexEntrySize := ReadIndexRow(file)

		if int(i)%config.SSTABLE_SEGMENT_SIZE == 0 || currentSize+indexEntrySize == indexSize {
			summaryEntries = append(summaryEntries, indexEntry)
			summaryEntries[len(summaryEntries)-1].Offset = uint32(indexStart) + uint32(currentSize) + 4
		}
		i++
		currentSize += indexEntrySize
	}

	file.Seek(summaryStart, 0)
	WriteSummaryRow(summaryEntries[0], file)
	WriteSummaryRow(summaryEntries[len(summaryEntries)-1], file)
	for _, summaryEntry := range summaryEntries {
		WriteSummaryRow(summaryEntry, file)
	}

	file.Seek(summaryStart-4, 0)
	summarySizeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(summarySizeBytes, uint32(len(summaryEntries)))
	file.Write(summarySizeBytes)
	file.Seek(summaryStart+int64(len(summaryEntries)), 0)

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

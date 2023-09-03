package src

import (
	"encoding/binary"
	"os"
)

type IndexEntry struct {
	KeySize uint64
	Key     []byte
	Offset  uint32
}

func NewIndexEntry() *IndexEntry {

	indexEntry := IndexEntry{
		KeySize: 0,
		Key:     nil,
		Offset:  0,
	}
	return &indexEntry

}

func (indexEntry *IndexEntry) ToBytes() []byte {

	bytes := make([]byte, 0)

	keySize := make([]byte, 8)
	binary.LittleEndian.PutUint64(keySize, indexEntry.KeySize)
	bytes = append(bytes, keySize...)

	bytes = append(bytes, indexEntry.Key...)

	offset := make([]byte, 4)
	binary.LittleEndian.PutUint64(offset, uint64(indexEntry.Offset))
	bytes = append(bytes, offset...)

	return bytes

}

func IndexEntryFromBytes(bytes []byte) *IndexEntry {

	indexEntry := NewIndexEntry()
	indexEntry.KeySize = binary.LittleEndian.Uint64(bytes[:8])
	indexEntry.Key = bytes[8 : 8+indexEntry.KeySize]
	indexEntry.Offset = binary.LittleEndian.Uint32(bytes[8+indexEntry.KeySize : 12+indexEntry.KeySize])
	return indexEntry

}

func ReadIndexRow(indexFile *os.File) (*IndexEntry, uint32) {

	indexEntry := NewIndexEntry()

	keySize := make([]byte, 8)
	_, err := indexFile.Read(keySize)
	if err != nil {
		panic(err)
	}
	indexEntry.KeySize = binary.LittleEndian.Uint64(keySize)

	key := make([]byte, indexEntry.KeySize)
	_, err = indexFile.Read(key)
	if err != nil {
		panic(err)
	}
	indexEntry.Key = key

	offset := make([]byte, 4)
	_, err = indexFile.Read(offset)
	if err != nil {
		panic(err)
	}
	indexEntry.Offset = binary.LittleEndian.Uint32(offset)

	return indexEntry, uint32(len(keySize) + len(key) + len(offset))

}

// func (indexEntry *IndexEntry) Write(file *os.File) {

// 	// time := strconv.FormatInt(time.Now().Unix(), 10)
// 	// path := "sstable" + string(os.PathSeparator) + time + "_" + "data.bin"
// 	// dataFile, err := os.Create(path)
// 	// if err != nil {
// 	// 	log.Fatal(err)
// 	// }
// 	// dataFile.Close()

// 	_, err := file.Write(indexEntry.ToBytes())
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// }

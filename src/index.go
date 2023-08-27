package src

import (
	"encoding/binary"
	"log"
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

func (indexEntry *IndexEntry) Write(file *os.File) {

	// time := strconv.FormatInt(time.Now().Unix(), 10)
	// path := "sstable" + string(os.PathSeparator) + time + "_" + "data.bin"
	// dataFile, err := os.Create(path)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// dataFile.Close()

	_, err := file.Write(indexEntry.ToBytes())
	if err != nil {
		log.Fatal(err)
	}

}

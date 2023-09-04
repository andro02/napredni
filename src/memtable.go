package src

import (
	"encoding/binary"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/andro02/napredni/config"
)

type Memtable struct {
	Threshhold uint32
	Size       uint32
	BT         *BTree
	SL         *SkipList
}

func NewMT() *Memtable {
	memtable := Memtable{
		Threshhold: uint32(config.MEMTABLE_THRESHOLD),
		Size:       0,
		BT:         nil,
		SL:         nil,
	}
	if config.MEMTABLE_STRUCTURE == 0 {
		memtable.BT = NewBTree()
	} else {
		memtable.SL = NewSkipList(config.SKIPLIST_SIZE)
	}
	return &memtable
}

func (memtable *Memtable) Set(key string, value []byte) {
	newEntrySize := uint32(binary.Size([]byte(key))) + uint32(len(value))
	if memtable.Size+newEntrySize >= memtable.Threshhold {
		memtable.flush()
		if config.MEMTABLE_STRUCTURE == 0 {
			memtable.BT = NewBTree()
		} else {
			memtable.SL = NewSkipList(config.SKIPLIST_SIZE)
		}
		memtable.Size = 0
	}
	if config.MEMTABLE_STRUCTURE == 0 {
		memtable.BT.Insert(key, value)
	} else {
		memtable.SL.InsertElement(key, value)
	}
	memtable.Size += newEntrySize

}

func (memtable *Memtable) Delete(key string, value []byte) {
	var found []byte
	if config.MEMTABLE_STRUCTURE == 0 {
		found, _ = memtable.BT.SearchKey(key)
	} else {
		found = memtable.SL.SearchElement(key)
	}
	if found == nil {
		memtable.Set(key, value)
		return
	}
	if config.MEMTABLE_STRUCTURE == 0 {
		memtable.BT.Update(key, value)
	} else {
		memtable.SL.UpdateElement(key, value)
	}

}

func (memtable *Memtable) flush() {
	time := strconv.FormatInt(time.Now().UnixMicro(), 10)
	path := "sstable" + string(os.PathSeparator) + time + "_"
	dataFile, err := os.Create(path + "data.bin")
	if err != nil {
		panic(err)
	}

	indexFile, err := os.Create(path + "index.bin")
	if err != nil {
		panic(err)
	}

	elements := make([]*KeyValuePair, 0)
	if config.MEMTABLE_STRUCTURE == 0 {
		elements = memtable.BT.GetAllElements()
	} else {
		entries := memtable.SL.GetAll()
		for _, entry := range entries {
			elements = append(elements, NewKeyValuePair(entry.key, entry.value))
		}
	}

	var dataSize uint32 = 0
	var indexSize uint32 = 0

	bloomFilter := NewBF(30, 0.1)
	data := make([][]byte, 0)

	for _, element := range elements {

		offset, dataRowSize := WriteDataRow(element.Value, dataFile)
		data = append(data, element.Value)
		dataSize += dataRowSize
		indexRowSize := WriteIndexRow(element.Value, indexFile, offset)
		indexSize += indexRowSize
		bloomFilter.Add(element.Key)

	}

	bloomFilter.Encode(path)
	merkle := CreateMerkle(data)
	merkle.WriteMetadata(path)
	CreateSummary(indexFile, path+"summary.bin", indexSize)
	CreateToc(path)

	dataFile.Close()
	indexFile.Close()
}

func CreateToc(path string) {
	tocFile, err := os.Create(path + "toc.txt")
	if err != nil {
		panic(err)
	}

	_, err = tocFile.WriteString(path + "data.bin\n")
	if err != nil {
		panic(err)
	}
	_, err = tocFile.WriteString(path + "index.bin\n")
	if err != nil {
		panic(err)
	}
	_, err = tocFile.WriteString(path + "summary.bin\n")
	if err != nil {
		panic(err)
	}
	_, err = tocFile.WriteString(path + "filter.bin\n")
	if err != nil {
		panic(err)
	}
	_, err = tocFile.WriteString(path + "metadata.txt\n")
	if err != nil {
		panic(err)
	}

	tocFile.Close()
}

func WriteDataRow(bytes []byte, dataFile *os.File) (uint32, uint32) {

	offset, _ := dataFile.Seek(0, io.SeekCurrent)
	dataFile.Write(bytes)
	return uint32(offset), uint32(len(bytes))

}

func WriteIndexRow(bytes []byte, indexFile *os.File, offset uint32) uint32 {

	walEntry := WalEntryFromBytes(bytes)

	keySize := make([]byte, 8)
	binary.LittleEndian.PutUint64(keySize, walEntry.KeySize)
	indexFile.Write(keySize)

	indexFile.Write(walEntry.Key)

	offsetBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(offsetBytes, offset)
	indexFile.Write(offsetBytes)

	return uint32(len(keySize) + len(walEntry.Key) + len(offsetBytes))

}

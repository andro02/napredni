package src

import (
	"encoding/binary"
	"io"
	"os"
	"strconv"
	"time"
)

type Memtable struct {
	Threshhold uint32
	Size       uint32
	BT         *BTree
	SL         *SkipList
}

func NewMT() *Memtable {
	memtable := Memtable{
		Threshhold: 200,
		Size:       0,
		BT:         NewBTree(),
		//SL:         NewSkipList(3),
	}
	return &memtable
}

func (memtable *Memtable) Set(key string, value []byte) {
	newEntrySize := uint32(binary.Size([]byte(key))) + uint32(len(value))
	if memtable.Size+newEntrySize >= memtable.Threshhold {
		memtable.flush()
		bTree := NewBTree()

		*memtable = Memtable{
			Threshhold: 50,
			Size:       0,
			BT:         bTree,
			SL:         nil,
		}

	}
	memtable.BT.Insert(key, value)
	memtable.Size += newEntrySize

}

func (memtable *Memtable) flush() {
	time := strconv.FormatInt(time.Now().Unix(), 10)
	path := "sstable" + string(os.PathSeparator) + time + "_"
	dataFile, err := os.Create(path + "data.bin")
	if err != nil {
		panic(err)
	}

	indexFile, err := os.Create(path + "index.bin")
	if err != nil {
		panic(err)
	}

	elements := memtable.BT.GetAllElements()

	var dataSize uint32 = 0
	var indexSize uint32 = 0

	//bloomFilter := NewBF()
	data := make([][]byte, 0)

	for _, element := range elements {

		offset, dataRowSize := WriteDataRow(element.Value, dataFile)
		data = append(data, element.Value)
		dataSize += dataRowSize
		indexRowSize := WriteIndexRow(element.Value, indexFile, offset)
		indexSize += indexRowSize

	}

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
	// _, err = tocFile.WriteString(path + "filter.bin\n")
	// if err != nil {
	// 	panic(err)
	// }
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

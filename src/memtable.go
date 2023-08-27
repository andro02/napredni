package src

import (
	"encoding/binary"
	"os"
	"path/filepath"
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
		Threshhold: 50,
		Size:       0,
		BT:         NewBTree(),
		//SL:         NewSkipList(3),
	}
	return &memtable
}

func (memtable *Memtable) Set(index string, value []byte) bool {
	newEntrySize := uint32(binary.Size([]byte(index))) + uint32(len(value))
	if memtable.Size+newEntrySize >= memtable.Threshhold {
		bTree := NewBTree()

		*memtable = Memtable{
			Threshhold: 50,
			Size:       0,
			BT:         bTree,
			SL:         nil,
		}

		return true

	}
	memtable.Size += uint32(binary.Size([]byte(index))) + uint32(len(value))
	return false

}

func (memtable *Memtable) flush() {
	nowStr := strconv.FormatInt(time.Now().UnixMicro(), 10)
	fl, err := os.Create("res" + string(filepath.Separator) + "L-1-" + nowStr + "Data.bin")
	if err != nil {
		panic(err)
	}
	defer fl.Close()
	if err != nil {
		panic(err)
	}

	indexPath := "res" + string(filepath.Separator) + "L-1-" + nowStr + "Index.bin"
	indexF, err := os.Create(indexPath)
	if err != nil {
		panic(err)
	}
	defer indexF.Close()
	if err != nil {
		panic(err)
	}

}

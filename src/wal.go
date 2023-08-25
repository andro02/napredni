package src

import (
	"io"
	"log"
	"os"
	"strconv"
)

type Wal struct {
	Data               []*WalEntry
	MaxDataSize        uint32
	Path               string
	CurrentFileEntries uint32
	MaxFileSize        uint32
	Prefix             string
	CurrentFilename    uint32
}

func NewWal() *Wal {

	files, _ := os.ReadDir("logs" + string(os.PathSeparator))
	currentFilename := len(files)

	wal := Wal{
		Data:               make([]*WalEntry, 0),
		MaxDataSize:        3,
		Path:               "logs",
		CurrentFileEntries: 0,
		MaxFileSize:        3,
		Prefix:             "wal.0.0.",
		CurrentFilename:    uint32(currentFilename),
	}
	return &wal

}

func (wal *Wal) Write(key string, value []byte) {

	if uint32(len(wal.Data)) >= wal.MaxDataSize {
		wal.Dump()
	}

	newWalEntry := NewWalEntry()
	newWalEntry.Write(key, value)
	wal.Data = append(wal.Data, newWalEntry)

}

func (wal *Wal) Delete(key string) {

	newWalEntry := NewWalEntry()
	newWalEntry.Tombstone = 1
	newWalEntry.Write(key, nil)
	wal.Data = append(wal.Data, newWalEntry)

}

func (wal *Wal) Dump() bool {

	currentFile, err := os.OpenFile(wal.Path+string(os.PathSeparator)+wal.Prefix+strconv.Itoa(int(wal.CurrentFilename))+".log", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(wal.Data); i++ {

		currentFile.Seek(0, io.SeekEnd)
		currentFile.Write(wal.Data[i].ToBytes())
		wal.CurrentFileEntries++

		if wal.CurrentFileEntries >= wal.MaxFileSize {
			wal.CurrentFilename++
			currentFile.Close()
			currentFile, _ = os.OpenFile(wal.Path+string(os.PathSeparator)+strconv.Itoa(int(wal.CurrentFilename))+".log", os.O_RDWR|os.O_CREATE, 0666)
		}

	}

	wal.CurrentFileEntries = 0
	wal.Data = make([]*WalEntry, 0)
	currentFile.Close()
	return true

}

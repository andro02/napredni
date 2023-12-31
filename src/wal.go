package src

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/andro02/napredni/config"
)

type Wal struct {
	Data               []*WalEntry
	MaxDataSize        uint32
	Path               string
	CurrentFileEntries uint32
	MaxFileSize        uint32
	Prefix             string
	CurrentFilename    uint32
	LowWatermark       uint32
}

func NewWal() *Wal {

	files, _ := os.ReadDir("logs" + string(os.PathSeparator))
	currentFilename := len(files)

	wal := Wal{
		Data:               make([]*WalEntry, 0),
		MaxDataSize:        uint32(config.WAL_DATA_SIZE),
		Path:               "logs",
		CurrentFileEntries: 0,
		MaxFileSize:        uint32(config.WAL_FILE_SIZE),
		Prefix:             "wal.0.0.",
		CurrentFilename:    uint32(currentFilename),
		LowWatermark:       uint32(config.WAL_LOW_WATER_MARK),
	}
	return &wal

}

func (wal *Wal) Write(key string, value []byte, tombstone byte) *WalEntry {

	if uint32(len(wal.Data)) >= wal.MaxDataSize {
		wal.Dump()
	}

	newWalEntry := NewWalEntry(tombstone)
	newWalEntry.Write(key, value)
	wal.Data = append(wal.Data, newWalEntry)

	return newWalEntry

}

func (wal *Wal) Delete(key string, tombstone byte) {

	newWalEntry := NewWalEntry(tombstone)
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
			currentFile, _ = os.OpenFile(wal.Path+string(os.PathSeparator)+wal.Prefix+strconv.Itoa(int(wal.CurrentFilename))+".log", os.O_RDWR|os.O_CREATE, 0666)
		}

	}

	wal.CurrentFileEntries = 0
	wal.Data = make([]*WalEntry, 0)
	currentFile.Close()
	return true

}

func (wal *Wal) DeleteSegments() {

	files, _ := os.ReadDir(wal.Path + string(os.PathSeparator))
	fileCount := len(files)

	if len(files) > int(wal.LowWatermark) {

		for _, file := range files {
			os.Remove(wal.Path + string(os.PathSeparator) + file.Name())
			fileCount--
			if fileCount == int(wal.LowWatermark) {
				break
			}
		}
		files, _ = os.ReadDir(wal.Path + string(os.PathSeparator))

		i := 0
		for _, file := range files {
			os.Rename(wal.Path+string(os.PathSeparator)+file.Name(), wal.Path+string(os.PathSeparator)+wal.Prefix+strconv.Itoa(int(i))+".log")
			i++
		}
	}

}

func (wal *Wal) Recovery() {

	files, _ := os.ReadDir(wal.Path + string(os.PathSeparator))
	fileCount := len(files)

	for i := 0; i < fileCount; i++ {

		file, _ := os.Open(wal.Path + string(os.PathSeparator) + wal.Prefix + strconv.Itoa(i) + ".log")

		fileInformation, err := file.Stat()
		if err != nil {
			panic(err)
		}

		if fileInformation.Size() == 0 {
			return
		}

		for {
			walEntry, err := ReadWalEntry(file)
			if err == io.EOF {
				file.Close()
				break
			}
			fmt.Println(walEntry.Validate())
		}

	}

}

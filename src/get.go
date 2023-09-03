package src

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func Get(memtable *Memtable, tokens []string) string {

	if len(tokens) != 2 {
		fmt.Println("Invalid input. Please try again.")
		return ""
	}

	var key string = tokens[1]

	files, _ := os.ReadDir("sstable" + string(filepath.Separator))
	var path string = ""

	for _, toc := range files {
		if strings.Contains(toc.Name(), "toc") {

			file, err := os.Open("sstable" + string(filepath.Separator) + toc.Name())
			if err != nil {
				panic(err)
			}

			scanner := bufio.NewScanner(file)
			scanner.Scan()
			data := scanner.Text()
			path = data[0 : len(data)-8]

			file.Close()

			value := Search(key, memtable, path)
			if value != "Not found" {
				return value
			}
		}
	}

	return "Not found"

}

func Search(key string, memtable *Memtable /*cache *LRUCache, */, path string) string {

	foundELement, _ := memtable.BT.SearchKey(key)
	if foundELement != nil {
		return string(WalEntryFromBytes(foundELement).Value)
	}

	// value, cacheFound := cache.Get([]byte(key))
	// if cacheFound {
	// 	return value
	// }

	if SearchBloomFilter(key, path) {
		if path != "" {
			offset, found := SearchSummary(key, path)

			if found {
				offset, found = SearchIndex(key, path, offset)

				if found {
					value := GetValueFromDataFile(path, offset)
					return value
				}
			}
		}
	}

	return "Not found"
}

func SearchBloomFilter(key string, path string) bool {
	bloomFilter := Decode(path)
	return bloomFilter.IsInBF(key)
}

func SearchSummary(key string, path string) (uint32, bool) {
	summaryFile, err := os.Open(path + "summary.bin")
	if err != nil {
		panic(err)
	}
	defer summaryFile.Close()
	first := ReadSummaryRow(summaryFile)
	last := ReadSummaryRow(summaryFile)
	if key >= string(first.Key[:]) && key <= string(last.Key[:]) {
		first = ReadSummaryRow(summaryFile)
		for {
			second := ReadSummaryRow(summaryFile)
			if key >= string(first.Key[:]) && key <= string(second.Key[:]) {
				break
			}
			first = second

		}
		return first.Offset, true
	}

	return 0, false
}
func SearchIndex(key string, path string, offset uint32) (uint32, bool) {

	indexFile, err := os.Open(path + "index.bin")
	if err != nil {
		panic(err)
	}
	defer indexFile.Close()
	indexFile.Seek(int64(offset), 0)

	indexEntry, _ := ReadIndexRow(indexFile)
	if string(indexEntry.Key[:]) == key {
		return indexEntry.Offset, true
	}

	for string(indexEntry.Key[:]) < key {
		indexEntry, _ = ReadIndexRow(indexFile)
		if string(indexEntry.Key[:]) == key {
			return indexEntry.Offset, true
		}
	}
	return 0, false

}

func GetValueFromDataFile(path string, offset uint32) string {
	dataFile, err := os.Open(path + "data.bin")
	if err != nil {
		panic(err)
	}
	defer dataFile.Close()
	dataFile.Seek(int64(offset), 0)
	dataEntry := ReadWalEntry(dataFile)
	return string(dataEntry.Value)

}

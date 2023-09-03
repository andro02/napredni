package src

import (
	"fmt"
	"os"
)

func Get(key string, sl *SkipList, cache *LRUCache, path string) []byte {


	foundELement := sl.SearchElement(key)
	if foundELement != nil {
		fmt.Println(foundELement)
		return foundELement
	} 

	value, cacheFound := cache.Get([]byte (key))
	if cacheFound {
		return value
	}
		
	offset, found := SearchSummary(key, path)
	if found{
		offset, found = SearchIndex(key, path, offset)
		if found{
			value = GetValueFromDataFile(path, offset)
			return value
		}

	}
	
	

	return nil
}

func SearchSummary(key string, path string) (uint32, bool) {
	summaryFile, err := os.Open(path)
	defer summaryFile.Close()
	if err != nil {
		panic(err)
	}
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
 func SearchIndex(key string , path string, offset uint32) (uint32, bool){

	indexFile, err := os.Open(path)
	defer indexFile.Close()
	if err != nil{
		panic(err)
	}
	indexFile.Seek(int64(offset), 0)
	indexEntry := ReadIndexRow(indexFile)
	for string(indexEntry.Key [:] ) < key {
		indexEntry = ReadIndexRow(indexFile)
	}
	if string(indexEntry.Key [:]) == key{
		return indexEntry.Offset, true
	}
	return 0, false

 }
 
 func GetValueFromDataFile(path string , offset uint32) []byte{
	dataFile, err := os.Open(path)
	defer dataFile.Close()
	if err != nil{
		panic(err)
	}
	dataFile.Seek(int64(offset), 0)
	dataEntry := ReadWalEntry(dataFile)
	return dataEntry

 }
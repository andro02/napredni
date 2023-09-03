package config

import (
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

//default vr
var CACHE_SIZE = 0
var LSM_DEPTH = 0
var MEMTABLE_CAPACITY = 0
var MEMTABLE_THRESHOLD = 0
var MEMTABLE_STRUCTURE = 0
var REQUEST_PERMIN = 0 
var SSTABLE_MULTIPLE_FILES = 0
var SSTABLE_SEGMENT_SIZE = 0
var WAL_SEGMENT_SIZE = 0

func ReadConfig(filename string) (map[string]int, error) {
	config := make(map[string]int)
	txtFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer txtFile.Close()

	txtContent, err := ioutil.ReadAll(txtFile)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(txtContent), "\n")
	for _, line := range lines {
		parts := strings.Split(line, "=")
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		intValue, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}

		config[key] = intValue
	}
	return config, nil
}

func LoadValues(data map[string]int) {
	CACHE_SIZE = data["cacheSize"]
	LSM_DEPTH = data["lsmDepth"]
	MEMTABLE_CAPACITY = data["memtableCapacity"]
	MEMTABLE_THRESHOLD = data["memtableThreshold"]
	MEMTABLE_STRUCTURE = data["memtableStructure"]
	REQUEST_PERMIN = data["requestPermin"]
	SSTABLE_MULTIPLE_FILES = data["sstableMultipleFiles"]
	SSTABLE_SEGMENT_SIZE = data["sstableSegmentSize"]
	WAL_SEGMENT_SIZE = data["walSegmentSize"]
	
}



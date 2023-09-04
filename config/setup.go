package config

import (
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

// default vr
var CACHE_SIZE = 10
var MEMTABLE_THRESHOLD = 500
var MEMTABLE_STRUCTURE = 0
var MEMTABLE_SINGLE_FILE = 0
var REQUEST_PERMIN = 5
var SSTABLE_MULTIPLE_FILES = 1
var SSTABLE_SEGMENT_SIZE = 5
var WAL_DATA_SIZE = 10
var WAL_FILE_SIZE = 10
var WAL_LOW_WATER_MARK = 3
var BF_EXPECTED_ELEMENTS = 50
var BF_FALSE_POSITIVE_RATE = 0.02
var BTREE_LIMIT = 3
var SKIPLIST_SIZE = 3

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
	MEMTABLE_THRESHOLD = data["memtableThreshold"]
	MEMTABLE_STRUCTURE = data["memtableStructure"]
	REQUEST_PERMIN = data["requestPermin"]
	SSTABLE_MULTIPLE_FILES = data["sstableMultipleFiles"]
	SSTABLE_SEGMENT_SIZE = data["sstableSegmentSize"]
	WAL_DATA_SIZE = data["walDataSize"]
	WAL_FILE_SIZE = data["walFileSize"]
	WAL_LOW_WATER_MARK = data["walLowWaterMark"]
	BF_EXPECTED_ELEMENTS = data["bfExpectedElements"]
	BF_FALSE_POSITIVE_RATE = float64(data["bfFalsePositiveRate"]) / 100
	BTREE_LIMIT = data["bTreeLimit"]
	SKIPLIST_SIZE = data["skipListSize"]
	MEMTABLE_SINGLE_FILE = data["memtableSingleFile"]

}

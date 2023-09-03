package src

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"time"
)

type HashWithSeed struct {
	Seed []byte
}

type BloomFilter struct {
	M             uint32
	K             uint32
	HashFunctions []HashWithSeed
	Set           []byte
}

func NewBF(expectedElements int, falsePositiveRate float64) *BloomFilter {

	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)
	bloomFilter := BloomFilter{
		M:             m,
		K:             k,
		HashFunctions: CreateHashFunctions(k),
		Set:           make([]byte, m),
	}
	return &bloomFilter
}

func (h *HashWithSeed) Hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

func CreateHashFunctions(k uint32) []HashWithSeed {
	h := make([]HashWithSeed, k)
	ts := uint32(time.Now().Unix())
	for i := uint32(0); i < k; i++ {
		seed := make([]byte, 32)
		binary.BigEndian.PutUint32(seed, uint32(ts+i))
		hfn := HashWithSeed{Seed: seed}
		h[i] = hfn
	}
	return h
}

func CalculateM(expectedElements int, falsePositiveRate float64) uint32 {
	return uint32(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint32) uint32 {
	return uint32(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func (bloomFilter *BloomFilter) Add(element string) {
	for _, function := range bloomFilter.HashFunctions {
		index := function.Hash([]byte(element)) % uint64(bloomFilter.M)
		bloomFilter.Set[index] = 1
	}
}

func (bloomFilter *BloomFilter) IsInBF(element string) bool {
	for _, function := range bloomFilter.HashFunctions {
		index := function.Hash([]byte(element)) % uint64(bloomFilter.M)
		if bloomFilter.Set[index] != 1 {
			return false
		}
	}
	return true
}

func (bloomFilter *BloomFilter) Encode(path string) {
	bfFile, err := os.Create(path + "filter.bin")
	if err != nil {
		panic(err)
	}

	m := make([]byte, 4)
	binary.LittleEndian.PutUint32(m, bloomFilter.M)
	bfFile.Write(m)

	k := make([]byte, 4)
	binary.LittleEndian.PutUint32(k, bloomFilter.K)
	bfFile.Write(k)

	for _, hash := range bloomFilter.HashFunctions {
		bfFile.Write(hash.Seed)
	}

	bfFile.Write(bloomFilter.Set)
	bf2 := Decode(path)
	if bf2 == bloomFilter {
		fmt.Println("Success")
	}
}

func Decode(path string) *BloomFilter {
	bf := NewBF(0, 0)

	bfFile, err := os.Open(path + "filter.bin")
	if err != nil {
		panic(err)
	}

	m := make([]byte, 4)
	_, err = bfFile.Read(m)
	if err != nil {
		panic(err)
	}
	bf.M = binary.LittleEndian.Uint32(m)

	k := make([]byte, 4)
	_, err = bfFile.Read(k)
	if err != nil {
		panic(err)
	}
	bf.K = binary.LittleEndian.Uint32(k)

	bf.HashFunctions = make([]HashWithSeed, bf.K)
	for i := 0; i < int(bf.K); i++ {
		bf.HashFunctions[i].Seed = make([]byte, 32)
		_, err = bfFile.Read(bf.HashFunctions[i].Seed)
		if err != nil {
			panic(err)
		}
	}

	bf.Set = make([]byte, bf.M)
	_, err = bfFile.Read(bf.Set)
	if err != nil {
		panic(err)
	}

	return bf
}

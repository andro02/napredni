package src

import (
	"crypto/md5"
	"encoding/binary"
	"math"
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

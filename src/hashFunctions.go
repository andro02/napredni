package src

import (
	"encoding/binary"
	"hash/fnv"
	"time"
)

func (h HashWithSeed) HashAlg(data []byte) uint64 {
	fn := fnv.New64a()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

func GenerateHashFunctions(k uint32) ([]HashWithSeed, [][]byte) {
	seeds := make([][]byte, k)
	h := make([]HashWithSeed, k)
	ts := uint32(time.Now().Unix())
	for i := uint32(0); i < k; i++ {
		seed := make([]byte, 32)
		seeds[i] = seed
		binary.BigEndian.PutUint32(seed, ts+i)
		hfn := HashWithSeed{Seed: seed}
		h[i] = hfn
	}
	return h, seeds
}

func InitializeHashFunctionsFromSeeds(k uint32, seeds [][]byte) []HashWithSeed {
	h := make([]HashWithSeed, k)
	for i := uint32(0); i < k; i++ {
		seed := seeds[i]
		hfn := HashWithSeed{Seed: seed}
		h[i] = hfn
	}
	return h
}

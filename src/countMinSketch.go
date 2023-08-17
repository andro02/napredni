package src

import (
	"fmt"
)


type CountMinSketch struct {
	d uint32 //br hash func
	w uint32 //kolone
	frequencyTable [][]uint64
	hashFunctions []HashWithSeed
	seeds [][]byte
}

func NewCountMinSketch(epsilon float64, delta float64) (*CountMinSketch, error){
	if epsilon <= 0 || delta <= 0 {
		return nil, fmt.Errorf("countminsketch error: values of epsilon and delta should both be greater than 0")
	}

	k := CalculateK(delta)
	m := CalculateM(epsilon)

	hashFunctions, seeds := GenerateHashFunctions(k)
	cms := &CountMinSketch{
		d : k,
		w : m, 
		hashFunctions: hashFunctions,
		seeds:  seeds,
	}
	cms.frequencyTable = make([][]uint64, k)

	for r := range cms.frequencyTable {
		cms.frequencyTable[r] = make([]uint64, m)
	}

	return cms, nil
}

// func (cms *CountMinSketch) D() uint32{
// 	return cms.d
// }

// func (cms *CountMinSketch) W() uint32{
// 	return cms.w
// }

func (cms *CountMinSketch) AddToSketch(data []byte) {
	for i, hashFunc := range cms.hashFunctions {
		hashedValue := hashFunc.Hash(data)
		tableIndex := hashedValue % uint64(cms.w)
		cms.frequencyTable[i][tableIndex] += 1
	}
}

func (cms *CountMinSketch) GetMinFrequency(data []byte) uint64 {
	frequencyValues := make([]uint64, cms.d)

	for i, hashFunc := range cms.hashFunctions {
		hashedValue := hashFunc.Hash(data)
		tableIndex := hashedValue % uint64(cms.w)

		frequencyValues[i] = cms.frequencyTable[i][tableIndex]
	}

	minFrequency := frequencyValues[0]
	for _, freq := range frequencyValues {
		if freq < minFrequency {
			minFrequency = freq
		}
	}
	return minFrequency
}

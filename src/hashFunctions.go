package src

import (
	"encoding/binary"
	"hash/fnv"
	"time"
)

type HashWithSeed struct {
	Seed []byte
}

func (h HashWithSeed) Hash(data []byte) uint64 {
	fn := fnv.New64a()
	fn.Write(append(data, h.Seed...))
	return fn.Sum64()
}



func GenerateHashFunctions(k uint32) ([]HashWithSeed, [][]byte) {
	hash_array := make([]HashWithSeed, k) 
	seconds := uint32(time.Now().Unix()) 

	for i := uint32(0); i < k; i++ { // Prolazak kroz sve has funkcije.
		seed := make([]byte, 4) // Kreiranje niza za seed od 4 bajta.

		// Postavljanje vrednosti seed-a na osnovu trenutnog vremena i indeksa.
		binary.BigEndian.PutUint32(seed, seconds+i)

		// Kreiranje strukture HashWithSeed sa trenutnim seed-om i dodavanje u niz has funkcija.
		hash_array[i] = HashWithSeed{Seed: seed}
	}

	return hash_array, nil // Vraćanje niza has funkcija i praznog niza seed-ova.
}

func InitializeHashFunctionsFromSeeds(k uint32, seeds [][]byte) []HashWithSeed {
	hash_array := make([]HashWithSeed, k)

	for i := uint32(0); i < k; i++ {
		hash_array[i] = HashWithSeed{Seed: seeds[i]}
	}

	return hash_array
}

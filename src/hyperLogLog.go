package src

import (
	"errors"
	"hash/fnv"
	"math"
	"math/bits"
)

var (
	exp32 = math.Pow(2, 32)
)

type HyperLogLog struct {
	p uint32 //broj pocetnih bitova iz hesa (precision)
	m uint32 //broj baketa
	buckets []int //lista baketa
}

func NewHyperLogLog(precison uint32) *HyperLogLog {
	m, _ := GenerateM(precison)
	hll := &HyperLogLog{
		p : precison,
		m : m,
		buckets : make([]int, m),
	}
	return hll
	
}
func CreateHash(stream []byte) uint32 {
	hash_value := fnv.New32()
	hash_value.Write(stream)
	sum := hash_value.Sum32()
	hash_value.Reset()
	return sum
}

func GenerateM(p uint32) (uint32, error) {
	if p > 16 || p < 4 {
		return 0, errors.New("precision must be between 4 and 16")
	}
	return uint32(math.Pow(2.0, float64(p))), nil
}

func leftmostActiveBit(hash uint32) int {
	return 1 + bits.LeadingZeros32(hash)
 }

func (hll HyperLogLog) Add(data []byte) HyperLogLog {
	hash_value := CreateHash(data)
	leading_bits := 32 - hll.p //broj bitova kada se izuzmu vodece nule
	rest_of_hash := hash_value << hll.p //odsece vodece 0 iz binarnog zapisa hesa tako sto sve bite pomeri u levo za broj nula
	position := leftmostActiveBit(rest_of_hash) //pozicija prvog levog aktivnog bita je ustvari broj desnih nula koje mu prethode
	index := hash_value >> leading_bits //pomeri sve bitove u desno i dobije vodece nule

	if position > int(hll.buckets[index]){
		hll.buckets[index] = position
	}

	return hll
}

func (hll *HyperLogLog) CountZeros() int {
	var zeros int
	for _, value := range hll.buckets {
		if value == 0 {
			zeros++
		}
	}
	return zeros
}

func (hll *HyperLogLog) Reset() {
	hll.buckets = make([]int, hll.m)
}

//procena kardinalnosti
func (hll *HyperLogLog) CalculateEstimation() float64{
	sum := 0.0
   	m := float64(hll.m)
	for _, val := range hll.buckets {
		sum += 1.0 / math.Pow(2.0, float64(val))
	}
	alpha := 0.79402 / (1.0 + 1.079/float64(m))
	estimate := alpha * m * m / sum
	empty_buckets := hll.CountZeros()
	if estimate <= 5.0/2.0*m {
		if empty_buckets > 0 {
			estimate = m * math.Log(m/float64(empty_buckets))
		 }
	} else if estimate > 1.0/30.0*exp32 {
		estimate = -exp32 * math.Log(1-estimate/exp32)
	}
	return estimate
}




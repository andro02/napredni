package src

import (
	"math"
)

func CalculateRows(epsilon float64) uint32 {
	return uint32(math.Ceil(math.E / epsilon))
}

func CalculateCols(delta float64) uint32 {
	return uint32(math.Ceil(math.Log(math.E / delta)))
}

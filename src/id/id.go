package id

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

const base36Radix = 36

// toBase36 converts a number to base36 string representation.
func toBase36(x uint32) string {
	var result []rune

	for {
		m := x % base36Radix
		x /= base36Radix
		result = append(result, rune(strconv.FormatInt(int64(m), base36Radix)[0]))
		if x == 0 {
			break
		}
	}

	// Reverse the result
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return string(result)
}

// generateSegmentID generates a unique segment ID similar to Cassandra SSTable identifiers.
func generateSegmentID() string {
	now := time.Now().UTC()

	year := now.Year()
	month := int(now.Month())
	day := now.Day() - 1

	hour := now.Hour()
	min := now.Minute()

	sec := now.Second()
	nano := now.Nanosecond()

	// Generate random number
	random := rand.Uint32() & 0xFFFF // limiting to 16-bit as in the original u16

	return fmt.Sprintf(
		"%04s_%s%s%02s%02s_%02s%08s_%04s",
		toBase36(uint32(year)),
		toBase36(uint32(month)),
		toBase36(uint32(day)),
		toBase36(uint32(hour)),
		toBase36(uint32(min)),
		toBase36(uint32(sec)),
		toBase36(uint32(nano)),
		toBase36(random),
	)
}

// func main() {
// 	fmt.Println(generateSegmentID())
// }

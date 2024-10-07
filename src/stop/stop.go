package stop

import (
	"fmt"
	"sync/atomic"
)

type StopSignal struct {
	flag *atomic.Bool
}

func NewStopSignal() *StopSignal {
	fg := new(atomic.Bool)
	fg.Store(false)
	return &StopSignal{
		flag: fg,
	}
}

func (s StopSignal) Send() {
	s.flag.Store(true)
}

func (s StopSignal) IsStopped() bool {
	return s.flag.Load()
}

// Clone method to replicate Rust's Clone trait
func (s StopSignal) Clone() StopSignal {
	return StopSignal{
		flag: s.flag,
	}
}

// String method to replicate Rust's Debug trait
func (s StopSignal) String() string {
	return fmt.Sprintf("StopSignal(%v)", s.flag.Load())
}

// Default method to replicate Rust's Default trait
func DefaultStopSignal() *StopSignal {
	return NewStopSignal()
}

// func main() {
// 	// Example usage
// 	signal := NewStopSignal()
// 	fmt.Println(signal)  // Output: StopSignal(false)

// 	signal.Send()
// 	fmt.Println(signal.IsStopped())  // Output: true

// 	cloned := signal.Clone()
// 	fmt.Println(cloned.IsStopped())  // Output: true
// }

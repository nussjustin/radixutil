package radixutil

import (
	"bufio"
	"fmt"
	"io"
	"time"

	"github.com/mediocregopher/radix/v3/resp"
	"github.com/mediocregopher/radix/v3/resp/resp2"
)

// Duration wraps a time.Duration with a given Precision for marshalling and unmarshalling for Redis.
type Duration struct {
	time.Duration

	// Precision that will be used when marshalling to and unmarshalling from RESP format.
	//
	// If Precision is 0, time.Millisecond is used.
	Precision time.Duration
}

var _ resp.Marshaler = (*Duration)(nil)
var _ resp.Unmarshaler = (*Duration)(nil)

// MarshalRESP implements the resp.Marshaler interface.
func (d Duration) MarshalRESP(w io.Writer) error {
	p := d.Precision
	if p == 0 {
		p = time.Millisecond
	}
	return resp2.Int{I: int64(d.Duration / p)}.MarshalRESP(w)
}

// UnmarshalRESP implements the resp.Unmarshaler interface.
func (d *Duration) UnmarshalRESP(br *bufio.Reader) error {
	var i resp2.Int
	if err := i.UnmarshalRESP(br); err != nil {
		return err
	}
	p := d.Precision
	if p == 0 {
		p = time.Millisecond
	}
	d.Duration = time.Duration(i.I) * p
	return nil
}

// UnmarshalArray unmarshals an array with length len(targets) from br into the values in targets.
func UnmarshalArray(br *bufio.Reader, targets ...interface{}) error {
	var ah resp2.ArrayHeader
	if err := ah.UnmarshalRESP(br); err != nil {
		return err
	}

	if ah.N != len(targets) {
		return fmt.Errorf("expected array of length %d, got %d", len(targets), ah.N)
	}

	for _, t := range targets {
		if err := (resp2.Any{I: t}).UnmarshalRESP(br); err != nil {
			return err
		}
	}

	return nil
}

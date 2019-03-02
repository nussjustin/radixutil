package stream_test

import (
	"bufio"
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/mediocregopher/radix/v3"
	"github.com/mediocregopher/radix/v3/resp"
	"github.com/nussjustin/radixutil"

	"github.com/mediocregopher/radix/v3/resp/resp2"

	"github.com/google/go-cmp/cmp"
	"github.com/nussjustin/radixutil/stream"
)

func marshalArray(t *testing.T, vals ...interface{}) io.Reader {
	var buf bytes.Buffer
	// resp2.Any does not currently handle types implementing resp.Marshaler correctly so we need to do the
	// whole marshalling ourselves just so that we can handle types implementing resp.Marshaler.
	if err := (resp2.ArrayHeader{N: len(vals)}).MarshalRESP(&buf); err != nil {
		t.Fatalf("failed to marshal arryay header: %s", err)
	}
	for i, val := range vals {
		var m resp.Marshaler
		if valm, ok := val.(resp.Marshaler); ok {
			m = valm
		} else {
			m = resp2.Any{I: val}
		}
		if err := m.MarshalRESP(&buf); err != nil {
			t.Fatalf("failed to marshal value at index %d: %s", i, err)
		}
	}
	return &buf
}

func TestPendingMessage(t *testing.T) {
	t.Run("UnmarshalRESP", func(t *testing.T) {
		want := stream.PendingMessage{
			ID: radix.StreamEntryID{
				Time: 1,
				Seq:  2,
			},
			Consumer:              "antirez",
			TimeSinceLastDelivery: radixutil.Duration{Duration: 3 * time.Second},
			DeliveryCount:         1337,
		}

		input := marshalArray(t,
			&want.ID,
			&want.Consumer,
			&want.TimeSinceLastDelivery,
			&want.DeliveryCount)

		br := bufio.NewReader(input)

		var got stream.PendingMessage
		if err := got.UnmarshalRESP(br); err != nil {
			t.Fatalf("unexpected error. want %q, got %q", (error)(nil), err)
		}

		if !cmp.Equal(want, got) {
			t.Errorf("got wrong result. diff:\n%s", cmp.Diff(want, got))
		}
	})
}

func TestPendingMessages(t *testing.T) {
	t.Run("ByConsumer", func(t *testing.T) {
		got := stream.PendingMessages{
			{
				Consumer:      "antirez",
				DeliveryCount: 1337,
			},
			{
				Consumer:      "mediocregopher",
				DeliveryCount: 814,
			},
			{
				Consumer:      "mediocregopher",
				DeliveryCount: 0,
			},
			{
				Consumer:      "nussjustin",
				DeliveryCount: 21,
			},
			{
				Consumer:      "mediocregopher",
				DeliveryCount: 1,
			},
			{
				Consumer:      "",
				DeliveryCount: 1,
			},
		}.ByConsumer()

		want := map[string]stream.PendingMessages{
			"": {
				stream.PendingMessage{Consumer: "", DeliveryCount: 1},
			},
			"antirez": {
				stream.PendingMessage{Consumer: "antirez", DeliveryCount: 1337},
			},
			"mediocregopher": {
				stream.PendingMessage{Consumer: "mediocregopher", DeliveryCount: 814},
				stream.PendingMessage{Consumer: "mediocregopher", DeliveryCount: 0},
				stream.PendingMessage{Consumer: "mediocregopher", DeliveryCount: 1},
			},
			"nussjustin": {
				stream.PendingMessage{Consumer: "nussjustin", DeliveryCount: 21},
			},
		}

		if !cmp.Equal(want, got) {
			t.Errorf("got wrong result. diff:\n%s", cmp.Diff(want, got))
		}
	})
}

func TestPendingSummary(t *testing.T) {
	t.Run("UnmarshalRESP", func(t *testing.T) {
		want := stream.PendingSummary{
			Pending:           1340,
			SmallestPendingID: radix.StreamEntryID{Time: 123, Seq: 456},
			LargestPendingID:  radix.StreamEntryID{Time: 789, Seq: 0},
			PendingByConsumer: map[string]int{
				"antirez":        1337,
				"mediocregopher": 3,
				"nussjustin":     0,
			},
		}

		input := marshalArray(t,
			&want.Pending,
			&want.SmallestPendingID,
			&want.LargestPendingID,
			&want.PendingByConsumer)

		br := bufio.NewReader(input)

		var got stream.PendingSummary
		if err := got.UnmarshalRESP(br); err != nil {
			t.Fatalf("unexpected error. want %q, got %q", (error)(nil), err)
		}

		if !cmp.Equal(want, got) {
			t.Errorf("got wrong result. diff:\n%s", cmp.Diff(want, got))
		}
	})
}

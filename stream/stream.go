package stream

import (
	"bufio"

	"github.com/mediocregopher/radix/v3"
	"github.com/mediocregopher/radix/v3/resp"
	"github.com/nussjustin/radixutil"
)

// Consumer contains information on a stream group consumer as returned by XINFO CONSUMERS.
type Consumer struct {
	Name    string             `redis:"name"`
	Pending int                `redis:"pending"`
	Idle    radixutil.Duration `redis:"idle"`
}

// Group contains information on a stream group as returned by XINFO GROUPS.
type Group struct {
	Name      string `redis:"name"`
	Consumers int    `redis:"consumers"`
	Pending   int    `redis:"pending"`
}

// Info contains information on a stream as returned by XINFO STREAM.
type Info struct {
	Length          int                 `redis:"length"`
	RadixTreeKeys   int                 `redis:"radix-tree-keys"`
	RadixTreeNodes  int                 `redis:"radix-tree-nodes"`
	Groups          int                 `redis:"groups"`
	LastGeneratedID radix.StreamEntryID `redis:"last-generated-id"`
	FirstEntry      radix.StreamEntry   `redis:"first-entry"`
	LastEntry       radix.StreamEntry   `redis:"last-entry"`
}

// PendingMessage contains information about a pending message as returned by the extended form of XPENDING.
type PendingMessage struct {
	ID                    radix.StreamEntryID
	Consumer              string
	TimeSinceLastDelivery radixutil.Duration
	DeliveryCount         int
}

var _ resp.Unmarshaler = (*PendingMessage)(nil)

// UnmarshalRESP implements the resp.Unmarshaler interface.
func (pm *PendingMessage) UnmarshalRESP(br *bufio.Reader) error {
	return radixutil.UnmarshalArray(br,
		&pm.ID,
		&pm.Consumer,
		&pm.TimeSinceLastDelivery,
		&pm.DeliveryCount)
}

// PendingMessage contains information about multiple pending messages as returned by the extended form of XPENDING.
type PendingMessages []PendingMessage

// ByConsumer returns a map of PendingMessage's grouped by Consumer.
func (pms PendingMessages) ByConsumer() map[string]PendingMessages {
	m := make(map[string]PendingMessages)
	for _, pm := range pms {
		m[pm.Consumer] = append(m[pm.Consumer], pm)
	}
	return m
}

// PendingMessage contains information about pending consumers and messages as returned by the simple form of XPENDING.
type PendingSummary struct {
	Pending           int
	SmallestPendingID radix.StreamEntryID
	LargestPendingID  radix.StreamEntryID
	PendingByConsumer map[string]int
}

// UnmarshalRESP implements the resp.Unmarshaler interface.
func (ps *PendingSummary) UnmarshalRESP(br *bufio.Reader) error {
	return radixutil.UnmarshalArray(br,
		&ps.Pending,
		&ps.SmallestPendingID,
		&ps.LargestPendingID,
		&ps.PendingByConsumer)
}

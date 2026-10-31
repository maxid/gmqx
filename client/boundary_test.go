package client_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
	"github.com/maxid/gnety/codec/mqtt"
)

// TestClientServerMaximumPacketSize checks the documented boundary: the maximum
// packet size the broker advertises is enforced by the client, which returns
// ErrPacketTooLarge instead of sending (or silently dropping) the packet, and the
// connection stays usable.
func TestClientServerMaximumPacketSize(t *testing.T) {
	b := startBroker(t, gmqx.Config{MaxPacketSize: 128})
	ctx := testContext(t)
	c := dialClient(t, client.Config{
		Addr:       b.Addr(),
		ClientID:   "max-packet",
		Version:    mqtt.Version5,
		CleanStart: true,
	})
	if got := c.ServerInfo().MaximumPacketSize; got != 128 {
		t.Fatalf("ServerInfo().MaximumPacketSize = %d, want 128", got)
	}

	big := bytes.Repeat([]byte("x"), 512)
	err := c.Publish(ctx, "max/topic", big, 1, false)
	if !errors.Is(err, client.ErrPacketTooLarge) {
		t.Fatalf("Publish of a 512-byte payload = %v, want ErrPacketTooLarge", err)
	}
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("128")) {
		t.Fatalf("the error does not name the server limit: %v", err)
	}
	if !c.Connected() {
		t.Fatal("a locally refused packet must not end the connection")
	}

	// The same connection still works for a packet that fits.
	if err := c.Publish(ctx, "max/topic", []byte("ok"), 1, false); err != nil {
		t.Fatalf("Publish of a small payload: %v", err)
	}
	if _, err := c.Subscribe(ctx, client.Subscription{Filter: "max/#", QoS: 1}); err != nil {
		t.Fatalf("Subscribe after a refused packet: %v", err)
	}
}

// TestClientCloseWhilePublishing cycles a client that is being closed while
// several goroutines publish at every QoS. It fails on a panic, on an unexpected
// error and on unbounded goroutine growth; -race makes it a data-race test too.
func TestClientCloseWhilePublishing(t *testing.T) {
	b := startBroker(t, gmqx.Config{})
	// Give the broker's listeners and accept loops a moment to settle before the
	// goroutine baseline is taken.
	time.Sleep(20 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	unexpected := make(chan error, 32)
	for cycle := 0; cycle < 20; cycle++ {
		c := dialClient(t, client.Config{
			Addr: b.Addr(),
			// A unique id per cycle: the broker may not have reaped the previous
			// connection yet, and UNIQUE mode would rightly refuse a duplicate id.
			ClientID:   fmt.Sprintf("close-race-%d", cycle),
			Version:    mqtt.Version5,
			CleanStart: true,
		})

		var wg sync.WaitGroup
		for worker := 0; worker < 3; worker++ {
			worker := worker
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 20; i++ {
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
					err := c.Publish(ctx, "close/race", []byte("payload"), byte(worker%3), false)
					cancel()
					if err == nil {
						continue
					}
					switch {
					case errors.Is(err, client.ErrClosed),
						errors.Is(err, client.ErrNotConnected),
						errors.Is(err, client.ErrDisconnected),
						errors.Is(err, client.ErrRetryLimit),
						errors.Is(err, context.Canceled),
						errors.Is(err, context.DeadlineExceeded):
					default:
						select {
						case unexpected <- err:
						default:
						}
						return
					}
				}
			}()
		}
		time.Sleep(time.Millisecond)
		// Two concurrent Close calls must be safe; one of them does the work.
		go func() { _ = c.Close() }()
		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		wg.Wait()
	}
	select {
	case err := <-unexpected:
		t.Fatalf("Publish returned an unexpected error while closing: %v", err)
	default:
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		settled := runtime.NumGoroutine()
		if settled <= baseline+8 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines grew from %d to %d over 20 close cycles", baseline, settled)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

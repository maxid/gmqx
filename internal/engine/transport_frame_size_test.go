package engine

// The WebSocket message limit is Config.MaxPacketSize, like every other transport.
//
// gnety caps a reassembled WebSocket message at 4 MiB by default and the broker
// never overrode it, so MaxPacketSize above 4 MiB was silently unreachable over ws
// and wss: the listener closed the connection with 1009 while tcp and tls served
// the same packet. 4 MiB is a library default, not a protocol fact — RFC 6455 has no
// message size limit and the MQTT spec lets one frame carry several or partial
// packets — so the limit is derived from the broker's own packet limit instead.

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"io"
	"testing"
	"time"
)

// readPacketExactly reads one packet whose body may be larger than the test
// client's read buffer. testClient.readPacket issues a single Read, which is fine
// for the control packets the other cases exchange but returns short here.
func readPacketExactly(c *testClient) (byte, []byte, error) {
	header, err := c.r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	n, err := readRemainingLength(c.r)
	if err != nil {
		return 0, nil, err
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(c.r, body); err != nil {
		return 0, nil, err
	}
	return header, body, nil
}

func TestWebSocketMessageLimitFollowsMaxPacketSize(t *testing.T) {
	const (
		maxPacketSize = 8 << 20
		payloadSize   = 5 << 20 // above gnety's 4 MiB WebSocket default
	)
	certFile, keyFile := selfSignedCert(t)
	b := startBroker(t, Config{
		Addr:                "127.0.0.1:0",
		SecureAddr:          "127.0.0.1:0",
		TLS:                 TLSConfig{Enabled: true, Cert: certFile, Key: keyFile},
		WebsocketAddr:       "127.0.0.1:0",
		WebsocketSecureAddr: "127.0.0.1:0",
		WebsocketPath:       "/mqtt",
		MaxPacketSize:       maxPacketSize,
	})
	pool := certPool(t, certFile)
	payload := bytes.Repeat([]byte("x"), payloadSize)

	for _, tc := range []struct {
		network string
		addr    string
		tlsCfg  *tls.Config
	}{
		{"ws", b.WebsocketAddr(), nil},
		{"wss", b.WebsocketSecureAddr(), &tls.Config{RootCAs: pool}},
	} {
		t.Run(tc.network, func(t *testing.T) {
			topic := "big/" + tc.network
			subConn := dialTestWebSocket(t, tc.network, tc.addr, "/mqtt", tc.tlsCfg)
			sub := connectOverConn(t, subConn, "big-sub-"+tc.network)
			body := []byte{0, 1} // SUBSCRIBE, packet id 1
			body = appendString(body, topic)
			body = append(body, 0) // requested QoS 0
			sub.writePacket(0x82, body)
			if header, _, err := readPacketExactly(sub); err != nil || header>>4 != 9 {
				t.Fatalf("subscribe over %s: header %x, err %v", tc.network, header, err)
			}

			pubConn := dialTestWebSocket(t, tc.network, tc.addr, "/mqtt", tc.tlsCfg)
			pub := connectOverConn(t, pubConn, "big-pub-"+tc.network)
			pubBody := appendString(nil, topic)
			pubBody = append(pubBody, payload...)
			pub.writePacket(0x30, pubBody) // PUBLISH, QoS 0

			sub.armDeadline(testClientIOTimeout)
			header, delivered, err := readPacketExactly(sub)
			if err != nil {
				t.Fatalf("the %s listener dropped a %d byte PUBLISH that MaxPacketSize (%d) allows: %v",
					tc.network, len(pubBody), maxPacketSize, err)
			}
			if header>>4 != 3 {
				t.Fatalf("delivered packet type = %d, want a PUBLISH", header>>4)
			}
			if want := 2 + len(topic) + payloadSize; len(delivered) != want {
				t.Fatalf("delivered body = %d bytes, want %d", len(delivered), want)
			}
		})
	}
}

// TestWebSocketMessageAboveMaxPacketSizeCloses is the other half of the same
// boundary: the limit is MaxPacketSize, not "unlimited". A message above it is
// refused and the connection is closed, so the number a caller configures is the
// number the ws listener enforces.
func TestWebSocketMessageAboveMaxPacketSizeCloses(t *testing.T) {
	b := startBroker(t, Config{
		Addr:          "127.0.0.1:0",
		WebsocketAddr: "127.0.0.1:0",
		WebsocketPath: "/mqtt",
		MaxPacketSize: 1 << 20,
	})
	conn := dialTestWebSocket(t, "ws", b.WebsocketAddr(), "/mqtt", nil)
	c := &testClient{conn: conn, r: bufio.NewReader(conn)}
	c.writePacket(0x10, connectPayload("over-ws"))
	if header, _, err := readPacketExactly(c); err != nil || header>>4 != 2 {
		t.Fatalf("connect: header %x, err %v", header, err)
	}

	body := appendString(nil, "over/topic")
	body = append(body, make([]byte, 2<<20)...)
	// The write may fail with the close the server sends; the write's outcome is
	// not the assertion.
	c.writePacket(0x30, body)
	c.writePacket(0xc0, nil) // PINGREQ
	c.armDeadline(testClientIOTimeout)
	if header, _, err := readPacketExactly(c); err == nil {
		t.Fatalf("the ws listener answered header %x after a %d byte message with MaxPacketSize at 1 MiB",
			header, len(body))
	}
	_ = conn.SetDeadline(time.Now())
}

// Command plugin-codec shows a data-protocol extension as an application-side
// plugin: the broker itself has no idea what JSON, CSV or a binary frame is, and
// the plugin supplies both the codecs and the decision to transcode.
//
// By default it starts an in-process gmqx broker, loads the plugin, publishes a
// JSON document and prints the payload a subscriber actually receives — binary
// bytes, decoded again through the same codec set. With -format csv the same round
// trip runs in the other direction (CSV in, JSON out). The last step unloads the
// plugin and publishes once more, so the demo also shows that passthrough is back.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/maxid/gplugin"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
	"github.com/maxid/gmqx/hook"
)

const steps = 6

func main() {
	format := flag.String("format", "json", `input format: "json" (JSON in, binary out) or "csv" (CSV in, JSON out)`)
	topic := flag.String("topic", "demo/telemetry", "topic the plugin transcodes")
	timeout := flag.Duration("timeout", 10*time.Second, "overall deadline of the demo")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `plugin-codec loads a data-protocol plugin into an in-process broker and
prints what a subscriber actually receives after the plugin transcoded it.

Examples:
  go run ./examples/plugin-codec
  go run ./examples/plugin-codec -format csv
  go run ./examples/plugin-codec -topic demo/telemetry -format json

Flags:
`)
		flag.PrintDefaults()
	}
	flag.Parse()

	if *format != "json" && *format != "csv" {
		log.Fatalf("plugin-codec: -format %q is not json or csv", *format)
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(sigCtx, *timeout)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	broker, err := gmqx.New(gmqx.Config{Addr: "127.0.0.1:0", Logger: logger})
	if err != nil {
		log.Fatalf("plugin-codec: create broker: %v", err)
	}
	if err := broker.Start(ctx); err != nil {
		log.Fatalf("plugin-codec: start broker: %v", err)
	}
	// Stop takes the cancelled ctx off the table, so Ctrl-C still closes it.
	defer func() { _ = broker.Stop(context.Background()) }()
	fmt.Printf("[1/%d] broker listening on %s\n", steps, broker.Addr())

	// Load the plugin the way a deployment does: add it, then start it. The host is
	// the broker, and the plugin installs its codecs and its transcoder through it.
	plugin := &codecPlugin{plan: map[string][2]string{*topic: plan(*format)}}
	if err := broker.Plugins().Add(plugin); err != nil {
		log.Fatalf("plugin-codec: add plugin: %v", err)
	}
	if err := broker.Plugins().Start(plugin.Name()); err != nil {
		log.Fatalf("plugin-codec: start plugin: %v", err)
	}
	defer func() { _ = broker.Plugins().Stop(plugin.Name()) }()

	for _, status := range mustSnapshot(broker) {
		fmt.Printf("[2/%d] plugin %s is %s (codecs %v)\n", steps, status.ID, status.State, broker.Codecs().MediaTypes())
	}

	delivered := make(chan []byte, 4)
	c, err := client.Dial(ctx, client.Config{
		Addr:       broker.Addr(),
		ClientID:   "plugin-codec",
		Version:    5,
		KeepAlive:  30 * time.Second,
		CleanStart: true,
	})
	if err != nil {
		log.Fatalf("plugin-codec: dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	// The handler is bound to the filter: every publication of this topic that the
	// plugin transcodes arrives here, and no fallback handler is needed.
	if _, err := c.Subscribe(ctx, client.Subscription{Filter: *topic, QoS: 0, Handler: func(m *client.Message) {
		// The payload is only valid during the callback; the demo copies it.
		select {
		case delivered <- append([]byte(nil), m.Payload...):
		default:
		}
	}}); err != nil {
		log.Fatalf("plugin-codec: subscribe: %v", err)
	}
	fmt.Printf("[3/%d] client %q subscribed to %s\n", steps, c.ClientID(), *topic)

	payload, outMediaType := input(*format)
	if err := c.Publish(ctx, *topic, payload, 0, false); err != nil {
		log.Fatalf("plugin-codec: publish: %v", err)
	}
	got, ok := receive(ctx, delivered, "the transcoded publication")
	if !ok {
		fmt.Println("[stop] interrupted")
		return
	}
	fmt.Printf("[4/%d] published %s, received as %s: %s\n", steps, *format, outMediaType, got)

	out, ok := broker.Codecs().Lookup(outMediaType)
	if !ok {
		log.Fatalf("plugin-codec: no codec for %s", outMediaType)
	}
	decoded, err := out.Decode(got)
	if err != nil {
		log.Fatalf("plugin-codec: decode %s: %v", outMediaType, err)
	}
	fmt.Printf("[5/%d] decoded with the same codec set: %v\n", steps, decoded)

	// Unload: the transcoder and the codecs go away, and the broker is back to
	// moving opaque bytes.
	if err := broker.Plugins().Stop(plugin.Name()); err != nil {
		log.Fatalf("plugin-codec: stop plugin: %v", err)
	}
	if err := c.Publish(ctx, *topic, payload, 0, false); err != nil {
		log.Fatalf("plugin-codec: publish after unload: %v", err)
	}
	again, ok := receive(ctx, delivered, "the passthrough publication")
	if !ok {
		fmt.Println("[stop] interrupted")
		return
	}
	if !bytes.Equal(again, payload) {
		log.Fatalf("plugin-codec: passthrough payload = %q, want %q", again, payload)
	}
	fmt.Printf("[6/%d] plugin unloaded; the payload passes through unchanged (%d bytes)\n", steps, len(again))
	fmt.Println("[done] codecs registered, transcoded and removed by an application-side plugin")
}

// plan maps the input format to the codec pair the transcoder uses.
func plan(format string) [2]string {
	if format == "csv" {
		return [2]string{"text/csv", "application/json"}
	}
	return [2]string{"application/json", "application/octet-stream"}
}

// input is the document the demo publishes, and the media type it arrives as.
func input(format string) ([]byte, string) {
	if format == "csv" {
		return []byte("id,state\n9,off\n"), "application/json"
	}
	return []byte(`{"id":7,"state":"on"}`), "application/octet-stream"
}

func mustSnapshot(broker *gmqx.Broker) []gplugin.Status {
	statuses, err := broker.Plugins().Snapshot()
	if err != nil {
		log.Fatalf("plugin-codec: snapshot: %v", err)
	}
	return statuses
}

// receive waits for one delivery. A false result means the demo was interrupted:
// the caller prints the [stop] line and returns, so the deferred closes still run.
func receive(ctx context.Context, ch chan []byte, what string) ([]byte, bool) {
	select {
	case payload := <-ch:
		return payload, true
	case <-ctx.Done():
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil, false
	}
	log.Fatalf("plugin-codec: wait for %s: %v", what, ctx.Err())
	return nil, false
}

// codecPlugin is the application-side plugin: it contributes three encodings and one
// transcoder, and removes both in Stop.
type codecPlugin struct {
	plan   map[string][2]string
	codecs *hook.CodecSet
	remove func()
}

func (p *codecPlugin) Name() string { return "codec-demo" }

func (p *codecPlugin) Start(host hook.Host) error {
	for _, codec := range []hook.Codec{jsonCodec{}, csvCodec{}, binaryCodec{}} {
		if err := host.Codecs().Register(codec); err != nil {
			return err
		}
	}
	p.codecs = host.Codecs()
	p.remove = host.AddTranscoder(&transcoder{codecs: p.codecs, plan: p.plan})
	return nil
}

func (p *codecPlugin) Stop() error {
	if p.remove != nil {
		p.remove()
		p.remove = nil
	}
	for _, mediaType := range p.codecs.MediaTypes() {
		p.codecs.Unregister(mediaType)
	}
	return nil
}

// transcoder applies the plan: the payload of a listed topic is decoded with one
// codec and encoded with the other. Every other publication passes through.
type transcoder struct {
	codecs *hook.CodecSet
	plan   map[string][2]string
}

func (t *transcoder) Transcode(_ context.Context, ev hook.PublishEvent) ([]byte, bool) {
	pair, ok := t.plan[ev.Topic]
	if !ok {
		return ev.Payload, true
	}
	from, ok := t.codecs.Lookup(pair[0])
	if !ok {
		return nil, false
	}
	to, ok := t.codecs.Lookup(pair[1])
	if !ok {
		return nil, false
	}
	value, err := from.Decode(ev.Payload)
	if err != nil {
		return nil, false
	}
	out, err := to.Encode(value)
	if err != nil {
		return nil, false
	}
	return out, true
}

// jsonCodec is RFC 8259 JSON.
type jsonCodec struct{}

func (jsonCodec) MediaType() string { return "application/json" }

func (jsonCodec) Decode(payload []byte) (any, error) {
	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func (jsonCodec) Encode(value any) ([]byte, error) { return json.Marshal(value) }

// csvCodec decodes the header and first row of a CSV document into a map.
type csvCodec struct{}

func (csvCodec) MediaType() string { return "text/csv" }

func (csvCodec) Decode(payload []byte) (any, error) {
	records, err := csv.NewReader(bytes.NewReader(payload)).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 2 {
		return nil, errors.New("csv: a header and one row are required")
	}
	row := make(map[string]string, len(records[0]))
	for i, key := range records[0] {
		if i < len(records[1]) {
			row[key] = records[1][i]
		}
	}
	return row, nil
}

func (csvCodec) Encode(value any) ([]byte, error) {
	row, ok := value.(map[string]string)
	if !ok {
		return nil, fmt.Errorf("csv: %T is not a CSV row", value)
	}
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]string, len(keys))
	for i, key := range keys {
		values[i] = row[key]
	}
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.WriteAll([][]string{keys, values}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// binaryCodec is a deliberately simple frame: a uvarint count of fields followed by
// length-prefixed key/value strings in sorted key order.
type binaryCodec struct{}

func (binaryCodec) MediaType() string { return "application/octet-stream" }

func (binaryCodec) Encode(value any) ([]byte, error) {
	fields, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("binary: %T is not a JSON object", value)
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := binary.AppendUvarint(nil, uint64(len(keys)))
	for _, key := range keys {
		out = appendString(out, key)
		out = appendString(out, fmt.Sprint(fields[key]))
	}
	return out, nil
}

func (binaryCodec) Decode(payload []byte) (any, error) {
	count, n := binary.Uvarint(payload)
	if n <= 0 {
		return nil, errors.New("binary: missing field count")
	}
	rest := payload[n:]
	fields := make(map[string]any, count)
	for i := uint64(0); i < count; i++ {
		key, after, err := readString(rest)
		if err != nil {
			return nil, err
		}
		value, after, err := readString(after)
		if err != nil {
			return nil, err
		}
		fields[key] = value
		rest = after
	}
	return fields, nil
}

// appendString and readString are the MQTT wire helpers, reused here so the demo
// binary frame is length-prefixed the same way the broker's own format is.
func appendString(dst []byte, s string) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(s)))
	return append(dst, s...)
}

func readString(b []byte) (string, []byte, error) {
	if len(b) < 2 {
		return "", nil, errors.New("binary: truncated length prefix")
	}
	n := int(binary.BigEndian.Uint16(b))
	if len(b) < 2+n {
		return "", nil, errors.New("binary: truncated string")
	}
	return string(b[2 : 2+n]), b[2+n:], nil
}

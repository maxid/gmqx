package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/maxid/gnety/codec/mqtt"
	"github.com/maxid/gplugin"

	"github.com/maxid/gmqx/hook"
)

// TestPluginCodecTranscodesPayload is the acceptance case for the codec extension:
// an application-side plugin contributes two payload encodings (JSON and a binary
// format) and a transcoder, and a publication that enters as JSON leaves as binary
// on the subscriber's side. Nothing in the broker knows either encoding: the plugin
// brings the codecs, the transcoder decides what to do with them, and removing the
// plugin restores byte-for-byte passthrough.
func TestPluginCodecTranscodesPayload(t *testing.T) {
	b := startBroker(t, Config{})
	plugin := &codecPlugin{}

	if err := b.Plugins().Add(plugin); err != nil {
		t.Fatalf("add plugin: %v", err)
	}
	if err := b.Plugins().Start(plugin.Name()); err != nil {
		t.Fatalf("start plugin: %v", err)
	}
	if got, want := b.Codecs().MediaTypes(), []string{"application/json", "application/octet-stream", "text/csv"}; !equalStrings(got, want) {
		t.Fatalf("codecs = %v, want %v", got, want)
	}
	status, err := b.Plugins().Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(status) != 1 || status[0].ID != plugin.Name() || status[0].State != gplugin.StateEnabled {
		t.Fatalf("plugin snapshot = %+v", status)
	}

	subscriber, _ := mustConnect(t, b.Addr(), confConnect{clientID: "codec-sub", clean: true})
	subscribe(t, subscriber, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "codec/#", QoS: 0})
	publisher, _ := mustConnect(t, b.Addr(), confConnect{clientID: "codec-pub", clean: true})

	publisher.publish(confPublish{topic: "codec/json", payload: `{"id":7,"state":"on"}`})
	transcoded := expectPublish(t, subscriber)
	if json.Valid(transcoded.Payload) {
		t.Fatalf("payload stayed JSON: %q", transcoded.Payload)
	}
	decoded, err := (binaryCodec{}).Decode(transcoded.Payload)
	if err != nil {
		t.Fatalf("decode binary payload %q: %v", transcoded.Payload, err)
	}
	if got := fmt.Sprint(decoded); got != "map[id:7 state:on]" {
		t.Fatalf("decoded payload = %s", got)
	}

	// The same seam carries a text format in the other direction: CSV in, JSON out.
	publisher.publish(confPublish{topic: "codec/csv", payload: "id,state\n9,off\n"})
	asJSON := expectPublish(t, subscriber)
	var csvRow map[string]any
	if err := json.Unmarshal(asJSON.Payload, &csvRow); err != nil {
		t.Fatalf("csv->json payload %q: %v", asJSON.Payload, err)
	}
	if csvRow["id"] != "9" || csvRow["state"] != "off" {
		t.Fatalf("csv->json row = %v", csvRow)
	}

	// A topic the transcoder does not claim crosses untouched: the seam is opt-in
	// per publication, not a broker-wide conversion.
	publisher.publish(confPublish{topic: "codec/raw", payload: `{"id":7}`})
	if raw := expectPublish(t, subscriber); string(raw.Payload) != `{"id":7}` {
		t.Fatalf("unclaimed topic payload = %q", raw.Payload)
	}

	// Stop removes the transcoder and every codec the plugin registered: MQTT
	// passthrough is back, byte for byte.
	if err := b.Plugins().Stop(plugin.Name()); err != nil {
		t.Fatalf("stop plugin: %v", err)
	}
	if got := b.Codecs().MediaTypes(); len(got) != 0 {
		t.Fatalf("codecs after stop = %v, want none", got)
	}
	publisher.publish(confPublish{topic: "codec/json", payload: `{"id":7}`})
	if plain := expectPublish(t, subscriber); string(plain.Payload) != `{"id":7}` {
		t.Fatalf("payload after unloading the plugin = %q", plain.Payload)
	}
}

// TestPluginRuntimeHookSlots pins the other half of the host layer: the hooks a
// plugin replaces take effect on a broker that is already serving, and swapping the
// metrics hook routes new observations to the new manager.
func TestPluginRuntimeHookSlots(t *testing.T) {
	first := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: first})
	publisher, _ := mustConnect(t, b.Addr(), confConnect{clientID: "slot-pub", clean: true})

	remove := b.AddInterceptor(hook.InterceptFunc(func(context.Context, string, string, []byte, byte) bool {
		return false
	}))
	publisher.publish(confPublish{topic: "slot/x", payload: "drop"})
	waitFor(t, "the interceptor drop to be counted", func() bool { return first.count("interceptor") == 1 })

	second := newDeliveryMetrics()
	b.SetMetricsManager(second)
	publisher.publish(confPublish{topic: "slot/x", payload: "drop again"})
	waitFor(t, "the swapped metrics hook to see the drop", func() bool { return second.count("interceptor") == 1 })
	if got := first.count("interceptor"); got != 1 {
		t.Fatalf("old metrics saw %d interceptor drops, want it to stay at 1", got)
	}

	remove()
	publisher.publish(confPublish{topic: "slot/x", payload: "kept"})
	if got := second.count("interceptor"); got != 1 {
		t.Fatalf("a removed interceptor still dropped: %d", got)
	}
}

// codecPlugin is the application-side plugin the tests above load: two codecs and a
// transcoder for one topic. It is what a real deployment writes once, outside the
// library.
type codecPlugin struct {
	codecs  *hook.CodecSet
	remove  func()
	started bool
}

func (p *codecPlugin) Name() string { return "json-to-binary" }

func (p *codecPlugin) Start(host hook.Host) error {
	for _, codec := range []hook.Codec{jsonCodec{}, binaryCodec{}, csvCodec{}} {
		if err := host.Codecs().Register(codec); err != nil {
			return err
		}
	}
	p.codecs = host.Codecs()
	p.remove = host.AddTranscoder(&topicTranscoder{codecs: p.codecs})
	p.started = true
	return nil
}

func (p *codecPlugin) Stop() error {
	if p.remove != nil {
		p.remove()
	}
	p.codecs.Unregister("application/json")
	p.codecs.Unregister("application/octet-stream")
	p.codecs.Unregister("text/csv")
	p.started = false
	return nil
}

// topicTranscoder maps one topic to a pair of codecs: what a payload arrives as and
// what it must leave as. Every other publication passes through untouched.
type topicTranscoder struct {
	codecs *hook.CodecSet
}

// transcodeMap is the plugin's routing of topics to encodings. It is application
// policy, which is exactly why it lives here and not in the broker.
var transcodeMap = map[string][2]string{
	"codec/json": {"application/json", "application/octet-stream"},
	"codec/csv":  {"text/csv", "application/json"},
}

func (t *topicTranscoder) Transcode(_ context.Context, ev hook.PublishEvent) ([]byte, bool) {
	pair, ok := transcodeMap[ev.Topic]
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
	payload, err := to.Encode(value)
	if err != nil {
		return nil, false
	}
	return payload, true
}

// jsonCodec and binaryCodec are the two encodings the test plugin contributes. The
// binary one is deliberately not JSON: a count of entries followed by length-prefixed
// key/value strings, in sorted key order, so the test can prove the bytes changed and
// the payload round-trips.
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

// csvCodec decodes the first row of a CSV document into a map. It exists so the
// acceptance case covers a text format the broker has never heard of.
type csvCodec struct{}

func (csvCodec) MediaType() string { return "text/csv" }

func (csvCodec) Decode(payload []byte) (any, error) {
	records, err := csv.NewReader(bytes.NewReader(payload)).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 2 {
		return nil, errors.New("csv codec: a header and one row are required")
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
		return nil, fmt.Errorf("csv codec: %T is not a CSV row", value)
	}
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([][]string, 0, 2)
	out = append(out, keys)
	values := make([]string, len(keys))
	for i, key := range keys {
		values[i] = row[key]
	}
	out = append(out, values)
	var buf bytes.Buffer
	if err := csv.NewWriter(&buf).WriteAll(out); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type binaryCodec struct{}

func (binaryCodec) MediaType() string { return "application/octet-stream" }

func (binaryCodec) Encode(value any) ([]byte, error) {
	fields, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("binary codec: %T is not a JSON object", value)
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
		return nil, errors.New("binary codec: missing field count")
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

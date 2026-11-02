package hook

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Codec is one payload encoding an application contributes to a running broker.
//
// The broker itself never interprets a payload: MQTT is opaque bytes, and a codec
// changes nothing about delivery. A codec is what an application-side plugin — a
// bridge, a rule, a transcoder — uses to cross an encoding boundary, and keeping
// that business here (rather than in the broker) is what lets gmqx grow encodings
// without growing its own protocol surface.
//
// A codec is registered by a plugin through Host.Codecs, so the set of encodings a
// process understands is a property of the plugins it loaded, not of the library.
type Codec interface {
	// MediaType identifies the encoding ("application/json", "text/csv",
	// "application/octet-stream", ...). It is the key a lookup uses, so it must be
	// unique within a CodecSet.
	MediaType() string
	// Decode turns a payload into a structured value. The type of that value is the
	// codec's business; a transcoder passes it straight to another codec's Encode.
	Decode(payload []byte) (any, error)
	// Encode turns a structured value back into a payload.
	Encode(value any) ([]byte, error)
}

// CodecSet is the registry of payload encodings a process can use.
//
// It is safe for concurrent use, because a plugin may register a codec while the
// broker is serving. A duplicate media type is an error rather than a silent
// replacement: two plugins claiming one encoding is a wiring mistake, and the
// process would otherwise differ from what its plugins believe they installed.
type CodecSet struct {
	mu     sync.RWMutex
	codecs map[string]Codec
}

// NewCodecSet returns an empty registry.
func NewCodecSet() *CodecSet { return &CodecSet{codecs: map[string]Codec{}} }

// Register adds a codec.
func (s *CodecSet) Register(codec Codec) error {
	if codec == nil {
		return fmt.Errorf("gmqx/hook: nil codec")
	}
	mediaType := codec.MediaType()
	if mediaType == "" {
		return fmt.Errorf("gmqx/hook: codec media type must not be empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.codecs == nil {
		s.codecs = map[string]Codec{}
	}
	if _, exists := s.codecs[mediaType]; exists {
		return fmt.Errorf("gmqx/hook: codec %q is already registered", mediaType)
	}
	s.codecs[mediaType] = codec
	return nil
}

// Unregister removes a codec, which is what a plugin's Stop does. Removing a codec
// that is not registered is a no-op so unloading twice cannot fail.
func (s *CodecSet) Unregister(mediaType string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.codecs, mediaType)
}

// Lookup returns the codec registered for a media type.
func (s *CodecSet) Lookup(mediaType string) (Codec, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	codec, ok := s.codecs[mediaType]
	return codec, ok
}

// MediaTypes lists the registered encodings, sorted.
func (s *CodecSet) MediaTypes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.codecs))
	for mediaType := range s.codecs {
		out = append(out, mediaType)
	}
	sort.Strings(out)
	return out
}

// Transcoder rewrites a payload on its way from the sink to routing.
//
// It is the encoding seam a codec plugin installs: the application decodes the
// payload with one codec and encodes it with another (JSON in, binary out), and the
// broker applies the result to the publication every local target and the cluster
// see. Two properties keep MQTT semantics intact: the payload is the only thing a
// transcoder may change (topic, QoS, retain and properties stay what the client
// sent), and a broker without transcoders behaves exactly as before.
//
// Transcoders run in registration order, after the sink accepted the publication,
// so a rejected or retryable message is never transformed; a transcoder that
// returns ok=false rejects the publication and it is counted as a drop.
type Transcoder interface {
	Transcode(ctx context.Context, ev PublishEvent) (payload []byte, ok bool)
}

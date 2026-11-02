package gmqx

// The broker engine lives in gmqx/server (docs/design/source-layout.md §4.1): the
// connection/session state machine, the routing index, the retained store usage and
// the configuration are one unit, so they moved together. The root package is the
// facade the existing importers see — every type below is an alias and every
// constructor a forwarder (rule L3), so `gmqx.Broker` and `server.Broker` are the
// same type and no consumer changes.

import "github.com/maxid/gmqx/server"

type (
	// Broker is a single-node MQTT 3.1.1/5 broker.
	Broker = server.Broker
	// Publication is one accepted publication the broker routes.
	Publication = server.Publication
	// LocalTarget is one local session a publication matches.
	LocalTarget = server.LocalTarget
	// Dispatcher is the routing policy of an accepted publication.
	Dispatcher = server.Dispatcher
	// DispatcherBinder is implemented by a Dispatcher that needs the routing seam.
	DispatcherBinder = server.DispatcherBinder
	// DefaultDispatcher is the built-in routing policy.
	DefaultDispatcher = server.DefaultDispatcher
	// ByteRateMetrics is the optional byte-rate extension of MetricsManager.
	ByteRateMetrics = server.ByteRateMetrics
	// ConnectionID identifies one physical connection.
	ConnectionID = server.ConnectionID
)

// New builds a broker from cfg.
func New(cfg Config) (*Broker, error) { return server.New(cfg) }

// NewDefaultDispatcher builds the built-in routing policy.
func NewDefaultDispatcher() *DefaultDispatcher { return server.NewDefaultDispatcher() }

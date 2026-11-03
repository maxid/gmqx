English | [简体中文](CLIENT.zh-CN.md)

# The MQTT client

`github.com/maxid/gmqx/client` speaks MQTT 3.1, 3.1.1 and 5 over TCP or TLS, with
the QoS 1/2 state machines, reconnection and subscription restore implemented
in-house. This page is the API in one screen plus the mapping from
[paho.mqtt.golang](https://github.com/eclipse/paho.mqtt.golang), so a project that
already uses paho can move without rethinking its dispatch model.

## Dispatch: handlers are bound to filters

A broker does not tell a client which subscription a publication belongs to — a
PUBLISH carries a topic name, and matching it against the filters the client asked
for is the client's job. Both libraries therefore keep a **route table**: filter →
handler.

```go
c, err := client.Dial(ctx, client.Config{Addr: addr, ClientID: "dev-1", Version: mqtt.Version5})

// 1. The handler travels with the subscription (paho: Subscribe's callback).
codes, err := c.Subscribe(ctx, client.Subscription{
    Filter:  "sensors/+/temp",
    QoS:     1,
    Handler: func(m *client.Message) { fmt.Println(m.Topic, string(m.Payload)) },
})

// 2. A route can exist without a subscription of its own (paho: AddRoute).
c.AddRoute("alerts/#", func(m *client.Message) { log.Println(m.Topic) })

// 3. Anything no route claimed goes to the configured fallback
//    (paho: DefaultPublishHandler), or is dropped when it is nil.
OnMessage: func(m *client.Message) { ... }
```

Rules, in the order they bite:

- **Every matching route runs.** One PUBLISH that two routes cover (say `a/#` and
  `a/+`) calls both handlers, in registration order. The broker sends one PUBLISH;
  the duplicate is the client's, exactly as in paho.
- **Handlers run one at a time**, on the client's single delivery goroutine, in the
  order the server sent the messages. A slow handler backpressures the connection
  (bounded queue, then TCP); real work belongs on your own goroutine.
- **The payload is only valid during the call.** A handler that keeps it copies it.
- **A route outlives the subscription.** It survives a reconnect (the client
  restores the subscription, the handler never moved) and an `Unsubscribe`; there is
  no `RemoveRoute`, as in paho.
- **A shared subscription registers under the inner filter.** `$share/g/a/#` is
  routed as `a/#`, because that is the topic an arriving publication carries.

## paho → gmqx/client

| paho.mqtt.golang | gmqx/client | Note |
|---|---|---|
| `paho.NewClient(opts)` + `Connect()` | `client.Dial(ctx, client.Config{...})` | one call; `Dial` waits for CONNACK |
| `Options.AddBroker(addr)` | `Config.Addr` | one address per client |
| `Options.SetClientID(id)` | `Config.ClientID` | |
| `Options.SetKeepAlive(d)` | `Config.KeepAlive` | |
| `Options.SetCleanSession(b)` | `Config.CleanStart` | MQTT 3.1.1 Clean Session / MQTT 5 Clean Start |
| `Options.SetAutoReconnect(b)` | `Config.Reconnect.Enabled` | backoff is exponential with jitter |
| `Options.SetConnectRetry(b)` | `Config.Reconnect.MaxAttempts` | |
| `Options.SetDefaultPublishHandler(h)` | `Config.OnMessage` | **fallback only** — routes win |
| `Options.SetOnConnectHandler(f)` | `Config.OnConnect` | |
| `Options.SetConnectionLostHandler(f)` | `Config.OnDisconnect` | |
| `Subscribe(topic, qos, cb)` | `Subscribe(ctx, Subscription{Filter, QoS, Handler})` | **blocks** until SUBACK; returns the granted codes |
| `SubscribeMultiple(map, cb)` | `Subscribe(ctx, subs...)` | one call, one handler per subscription |
| `AddRoute(filter, cb)` | `AddRoute(filter, handler)` | same table, same lifetime |
| `Unsubscribe(topics...)` | `Unsubscribe(ctx, filters...)` | routes stay |
| `Publish(topic, qos, retain, payload)` | `PublishMessage(ctx, PublishOptions{...})` | returns after PUBACK/PUBCOMP, not a Token |
| `Token.Wait()` / `Token.Error()` | the `error` return | every call is synchronous except handlers |
| `Options.SetOrderMatters(b)` | — | handlers always run in order, one at a time |
| `LoadMessage`/`StoreMessage` | — | no offline store; `Reconnect` restores subscriptions |
| `NewClientOptions()` defaults | `Config` zero value | zero value is 3.1.1, clean session, no reconnect |

Two differences worth planning for:

- **No `Token`.** `Subscribe` and `Publish` block until the acknowledgement, so a
  caller that wants fire-and-forget runs the call on its own goroutine. That is what
  keeps the API one value in and one value out, and it removes the "did my token
  ever complete" question.
- **No offline message store.** paho can persist messages across a process restart;
  gmqx's client keeps state in memory and relies on the session on the server side
  (`CleanStart: false` + `SessionExpiry`) for anything that must survive.

## Errors

Every failure is an `error` the caller can test with `errors.Is`:
`ErrNotConnected`, `ErrDisconnected`, `ErrClosed`, `ErrBadQoS`, `ErrBadTopic`,
`ErrPacketTooLarge`, `ErrRetryLimit`, `ErrNoPacketID`, `ErrInvalidConfig`,
`ErrProtocol`. A negative MQTT 5 acknowledgement is a `*ReasonError` carrying the
packet, the code and the server's reason string.

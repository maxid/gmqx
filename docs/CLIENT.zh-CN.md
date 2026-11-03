[English](CLIENT.md) | 简体中文

# MQTT 客户端

`github.com/maxid/gmqx/client` 支持 MQTT 3.1、3.1.1 与 5（TCP 或 TLS），QoS 1/2 状态机、重连与
订阅恢复都是自己实现的。这一页是一屏 API，加上
[paho.mqtt.golang](https://github.com/eclipse/paho.mqtt.golang) 的映射表——已经在用 paho 的项目
不用重新设计分发模型就能迁过来。

## 分发：handler 绑在 filter 上

broker 不会告诉客户端一条发布属于哪个订阅——PUBLISH 只带主题名，把它与客户端申请过的 filter 匹配是
客户端自己的事。所以两个库都维护一张**路由表**：filter → handler。

```go
c, err := client.Dial(ctx, client.Config{Addr: addr, ClientID: "dev-1", Version: mqtt.Version5})

// 1. handler 跟订阅一起给（paho：Subscribe 的回调）
codes, err := c.Subscribe(ctx, client.Subscription{
    Filter:  "sensors/+/temp",
    QoS:     1,
    Handler: func(m *client.Message) { fmt.Println(m.Topic, string(m.Payload)) },
})

// 2. 路由可以独立于订阅存在（paho：AddRoute）
c.AddRoute("alerts/#", func(m *client.Message) { log.Println(m.Topic) })

// 3. 没有任何路由认领的消息走兜底（paho：DefaultPublishHandler）；没配就丢弃
OnMessage: func(m *client.Message) { ... }
```

规则，按会咬人的顺序：

- **每个匹配的路由都会跑。** 一条 PUBLISH 命中两条路由（比如 `a/#` 与 `a/+`），两个 handler 都会
  被调用，按注册顺序。broker 只发一条 PUBLISH，重复发生在客户端——与 paho 一致。
- **handler 一次跑一个**，在客户端唯一的投递 goroutine 上，按服务端发送顺序。慢 handler 会反压
  连接（有界队列，然后 TCP）；真正的活儿应该丢给自己的 goroutine。
- **payload 只在回调期间有效**，要留就自己 copy。
- **路由活得比订阅长。** 重连不丢（客户端恢复订阅，handler 从没动过），`Unsubscribe` 也不删；与
  paho 一样没有 `RemoveRoute`。
- **共享订阅按内层 filter 注册。** `$share/g/a/#` 按 `a/#` 路由，因为到达的发布带的就是那个主题。

## paho → gmqx/client

| paho.mqtt.golang | gmqx/client | 说明 |
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

两处需要提前规划的差异：

- **没有 `Token`。** `Subscribe` 与 `Publish` 阻塞到确认，想要 fire-and-forget 的调用方自己起
  goroutine。这让 API 一进一出，也消掉了"我的 token 到底完成没完成"这个问题。
- **没有离线消息存储。** paho 能跨进程重启持久化消息；gmqx 的客户端只在内存里保存状态，要跨重启活
  的东西依赖服务端会话（`CleanStart: false` + `SessionExpiry`）。

## 错误

每个失败都是一个可以用 `errors.Is` 判断的 `error`：
`ErrNotConnected`, `ErrDisconnected`, `ErrClosed`, `ErrBadQoS`, `ErrBadTopic`,
`ErrPacketTooLarge`, `ErrRetryLimit`, `ErrNoPacketID`, `ErrInvalidConfig`,
`ErrProtocol`. A negative MQTT 5 acknowledgement is a `*ReasonError` carrying the
packet, the code and the server's reason string.

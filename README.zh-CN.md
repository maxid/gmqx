[English](README.md) | 简体中文

# gmqx

用 Go 编写的轻量级、可嵌入的 **MQTT 3.1.1 broker**。

> 状态：**开发快照（未打标签）** — MQTT 3.1.1/5 broker；经 gnety 提供 TCP/TLS/WebSocket/WSS 传输，
> 可插拔的认证/ACL/事件/指标钩子，连接模式与限制。
> API 在 v1.0 之前可能变化。

## 特性

- TCP、TLS、WebSocket 与 WSS 上的 MQTT 3.1、3.1.1 及基础 MQTT 5
- CONNECT/CONNACK、SUBSCRIBE/SUBACK、PUBLISH、PING、DISCONNECT
- 持久会话（Clean Session / Clean Start、Session Present）；持久会话的订阅在断连后保留，
  clean 连接丢弃它们
- MQTT 5：CONNACK 能力属性、Topic Alias、User Property 转发、
  订阅选项（no-local / retain-as-published / retain-handling）、原因码，
  以及协议错误时的 DISCONNECT
- 持久会话存储、有界离线队列与 MQTT 5 Session Expiry Interval：没有活动连接的会话
  保留其订阅匹配到的消息（FIFO，按会话与全局双重上限），在下次 CONNACK 之后立即按序
  投递，并在过期间隔到达后由有界清扫器删除。状态位于 `SessionStore` 接缝之后
  （`Load`/`Save`/`Delete`/`Keys`），默认即为早期版本的内存存储。
- QoS 0/1/2 流程（按订阅 QoS 降级）、broker 侧出站 QoS 1/2 重传（PUBLISH 重传带 DUP、
  PUBREL 重传到 PUBCOMP、每连接一个定时器、有上限的指数退避）以及由每连接有界队列支撑的
  inflight 窗口：窗口满时按 FIFO 排队、腾出位置后再投递，既不阻塞也不丢弃。入站 QoS 2
  握手抑制重复，并受 `MaxInflightQoS2` 约束（PUBREC 0x97）。
- 每连接一个写协程，配上有界出站队列与每包写超时（`MaxOutboundQueueSize`、`WriteTimeout`），
  因此停止读取的对端无法拖住发布方、遗嘱发布或集群接收协程。
- 保留消息（含 MQTT 5 Message Expiry Interval：回放时携带剩余间隔，已过期的不再回放并删除）、
  通配符订阅（`+`/`#`）、带 QoS/retain/属性的遗嘱消息、MQTT 5 Will Delay Interval
  （延迟到期发布、被重连取消、被会话过期提前触发）、keepalive
- CONNECT 阶段与鉴权时限：`ConnectTimeout` 关闭永远不完成 CONNECT 的连接，
  `AuthTimeout` 限制 `Authenticate` 与 `ACL.Check`，超时视为拒绝
- 线级 MQTT 3.1.1/5 一致性测试套件（`TestConformance`）
- 可插拔钩子：`AuthManager`、`AclManager`、`EventSink`（生命周期/消息）、
  `MetricsManager`、`Interceptor`
- 可插拔路由：`Dispatcher` 接收每个被接受 publish 的 `Publication` 并决定去向；
  未配置时保持内置的本地投递 + 集群传播行为（`DefaultDispatcher`）；broker 提供只读接缝
  （`LocalTargets`、`SendLocal`、`SpreadPublication`），dispatcher 无需自行重写 topic 匹配
- 每个钩子都收到连接作用域的 context：通过 `ContextWithConn`/`ConnFromContext`
  读取 `ConnInfo`（client id、connection id、namespace、远端地址、协议级别）；
  context 在 CONNECT 时由 `Config.ConnContext` 增强一次，并在连接结束时取消
- 命名空间隔离：`Config.Namespace` 划分会话表、订阅匹配、保留状态以及集群订阅摘要/事件，
  一个命名空间的 publish 永远不会到达另一命名空间的会话；默认空命名空间
  保持单命名空间部署的行为不变
- 连接模式：`UNIQUE` / `KICK`（client id 接管；MQTT 5 上被顶掉的连接收到 DISCONNECT 0x8E；
  `NotKickSeconds` 防抖窗口在 CONNECT 端与收到 kick 的对端都生效；CONNECT 时 kick 携带产生
  该事件的连接的建立时间，因此它仍会关闭其他节点上的重复连接，而迟到的 kick 无法关闭更新的连接）
- 集群内共享持久会话：可选的 `RedisSessionStore` 把订阅与离线队列放进集群后端所用的同一个
  Redis，客户端重连到其他节点时会话得以恢复
- 共享订阅（MQTT 5 §4.8.2）：`$share/{ShareName}/{filter}` 按 ShareName 规则校验
  （畸形的按过滤器回 SUBACK 0x8F；只以 `$share` 开头的普通过滤器仍是字面量过滤器；
  共享订阅上的 No Local 被拒绝），单节点按 round-robin 只投递给组内一个活成员；
  集群内每个组只选一个节点投递，因此组不会在每个节点各收一份。离线成员不收共享消息
  （共享状态不入队），订阅共享过滤器时也不重放保留消息
- 订阅标识符（MQTT 5 属性 0x0B）：每个 SUBSCRIBE 携带的标识符作为订阅状态保存，
  持久会话恢复后依然有效，并附加到该订阅产生的每条 PUBLISH（实时投递、保留消息重放、
  离线队列排空、共享组投递），跨多个匹配订阅去重并受每次投递上限约束
- CONNACK 宣告 Retain、Wildcard Subscription、Subscription Identifier 与 Shared
  Subscription 均可用，且四者都已实现
- 限制：最大连接数、最大订阅数、最大保留消息数、每个方向的 inflight 窗口、
  每会话与全局离线队列长度、连接速率限制
- 优雅关闭与 client-id 会话接管
- 路由索引：本地匹配对字面量过滤器只需一次 map 查找，每个不同的通配过滤器只匹配一次，
  publish 不再遍历连接或已存储会话，发布路径也不会调用 `SessionStore`
- 定向发布：`Broker.PublishToClient(ctx, clientID, Publication)` 投递到指定 client id
  （本地活连接、本地持久会话的离线队列，或持有该 id 的集群节点）；Redis 后端通过带 TTL 的
  client-id presence 路由，TCP 网状后端广播、各节点仅在持有该 id 时应用
- 可插拔的 `Cluster` 接口，提供内置 TCP 后端（静态 seeds、
  带心跳与驱逐的握手式成员关系、事件去重与订阅感知的定向路由、
  跨节点 publish/kick、重连）以及
  Redis 后端（按节点划分通道而非网状互联、Redis 中以 TTL 维护的成员关系、
  相同的去重与路由规则）

## 安装

```bash
go get github.com/maxid/gmqx
```

## 快速开始

```go
broker, _ := gmqx.New(gmqx.Config{Addr: ":1883"})
_ = broker.Start(context.Background())
defer broker.Stop(context.Background())
```

带钩子：

```go
broker, _ := gmqx.New(gmqx.Config{
    Addr: ":1883",
    Auth: myAuth{},          // implements gmqx.AuthManager
    ACL:  myACL{},           // implements gmqx.AclManager
    Sink: myDispatcher{},    // implements gmqx.EventSink
    Metrics: gmqx.NoopMetrics{},
    ConnectMode: gmqx.ConnectModeKick,
})
```

## 示例

[`examples/broker-demo`](examples/broker-demo/main.go) 在回环地址的临时端口上启动 broker，
连接一个客户端并完成一次发布/订阅往返。gmqx 不提供客户端 API，因此示例借助
gnety 的 MQTT 编解码器（broker 自身使用的线格式层）充当 MQTT 3.1.1 客户端，
并打印 broker 回送的 PUBLISH：

```sh
go run ./examples/broker-demo
go run ./examples/broker-demo -addr 127.0.0.1:1883 -topic demo/hello
```

broker 与客户端运行在同一进程内，无需第三方 MQTT 客户端。

## 配置（部分）

| 字段 | 默认值 | 说明 |
|---|---|---|
| `Addr` / `Port` | `:1883` / `1883` | TCP 监听地址 |
| `SecureAddr` / `SecurePort` | — / 8883 | TLS 监听器（需要 `TLS`） |
| `WebsocketAddr` / `WebsocketPort` | — / 1884 | WebSocket 监听器 |
| `WebsocketSecureAddr` / `WebsocketSecurePort` | — / 8884 | WSS 监听器 |
| `WebsocketPath` | `/mqtt` | WebSocket 路径 |
| `TLS` | 禁用 | `Enabled`、`Cert`、`Key`、`CA`、`RequireClientCert` |
| `MaxPacketSize` | 4 MiB | 入站数据包上限 |
| `ConnectMode` | `UNIQUE` | 拒绝或踢掉已存在的 client id |
| `NotKickSeconds` | 30 | 接管的防抖窗口，本地 CONNECT 与收到的对端 kick 都适用（负值：不设窗口） |
| `ConnectTimeout` | 10 s | CONNECT 阶段读超时；只建连不发 CONNECT 的连接会被关闭 |
| `AuthTimeout` | 5 s | `Authenticate` 与 `ACL.Check` 的时限；超时即拒绝（负值：不限） |
| `MaxWillDelayInterval` | 5 min | 允许的最长 MQTT 5 Will Delay Interval（负值：不限） |
| `MaxConnections` | 0（不限） | 并发连接数限制 |
| `MaxTopicSubscriptions` | 0 | 每会话订阅数限制 |
| `MaxRetainMessages` | 0 | 保留消息数量限制 |
| `ConnectionRateLimit` | 0 | 连接数/秒的令牌桶 |
| `MaxInflightQoS1` | 64 | 每连接未确认的 QoS 1 PUBLISH 数（负值：不限） |
| `MaxInflightQoS2` | 64 | 每连接每个方向的 QoS 2 交换数（负值：不限） |
| `MaxInflightQueueSize` | 256 | 每连接等待空闲 inflight 位置的 QoS 1/2 投递数 |
| `MaxOutboundQueueSize` | 256 | 每连接写协程的出站队列长度（包数） |
| `WriteTimeout` | 5 s | 单个数据包的写超时；超时即关闭卡住的连接 |
| `QoSRetryInterval` | 5 s | 重传基准间隔，每次尝试翻倍 |
| `QoSMaxRetries` | 5 | 每个交换放弃前的重传次数（负值：不重传） |
| `MaxOfflineQueueSize` | 1000 | 每个离线会话的队列上限；满时丢弃最旧的 |
| `MaxTotalOfflineMessages` | 100000 | 所有离线会话的队列上限；满时拒绝最新的 |
| `SessionSweepInterval` | 30 s | 过期会话的清理间隔（负值：不清理） |
| `SessionStore` | `MemorySessionStore` | 离线会话状态的持久化接缝；`NewRedisSessionStore` 使其在 Redis 集群各节点间共享 |
| `Auth` / `ACL` / `Sink` / `Metrics` | 全部放行 / 空实现 | 可插拔 SPI |
| `Namespace` | `""`（默认） | 会话、保留状态与集群流量的隔离作用域 |
| `Dispatcher` | `DefaultDispatcher` | 路由 SPI：默认本地投递 + 集群传播 |
| `ConnContext` | nil | 在 CONNECT 时增强每连接 context |

随离线队列与 inflight 窗口新增的投递限制刻意有界：未设置时取文档中的默认值，因为无界
队列与无界窗口都会把慢速客户端变成无界的 broker 内存。负值表示不限
（`MaxInflightQoS1`、`MaxInflightQoS2`、`MaxOfflineQueueSize`、`MaxTotalOfflineMessages`）、
不重传（`QoSMaxRetries`）或不做清理（`SessionSweepInterval`）。`MaxInflightQueueSize`、
`MaxOutboundQueueSize`、`WriteTimeout` 与 `ConnectTimeout` 没有“不限”设置：非正值一律
取默认值，因为这四项正是防止单个慢速或沉默客户端占掉无界内存、卡住协程、或长期占用一个
永远不完成 CONNECT 的连接槽的护栏。

其余 MQTT 5 属性/语义（receive maximum、数据包上限）、跨重连的 QoS 2 状态与客户端库
都在路线图中（见 [ROADMAP.md](ROADMAP.md)）。

## Dispatcher、连接 context 与命名空间

`Config.Dispatcher` 是被接受 publish 去向的唯一决策点。broker 先完成鉴权、拦截器与 sink；
保留状态在路由之前写入，因此 dispatcher 决定的是实时消息的去向，而不是保留什么。
`DefaultDispatcher`（nil 默认）投递给本地匹配会话并传播到集群后端。自定义 dispatcher
通过 broker 的只读接缝路由，而不是自行实现 topic 匹配：

```go
type myDispatcher struct{ broker *gmqx.Broker }

func (d *myDispatcher) Bind(b *gmqx.Broker) { d.broker = b }

func (d *myDispatcher) Dispatch(ctx context.Context, pub gmqx.Publication) error {
    for _, target := range d.broker.LocalTargets(pub) {
        _ = d.broker.SendLocal(ctx, target, pub)
    }
    return d.broker.SpreadPublication(ctx, pub)
}
```

每个钩子都收到连接作用域的 context。Go 没有 thread-local 状态，因此连接身份通过
context value 传递：`gmqx.ConnFromContext` 返回该钩子所属连接的 `gmqx.ConnInfo`。
context 在连接被接受时创建，在 CONNECT 时由 `Config.ConnContext` 增强一次
（写入其中的值/截止时间会传达到该连接的每个钩子），整个交换过程复用同一个 context，
并在连接结束时取消。

`Config.Namespace` 是 broker 的隔离作用域。会话、订阅匹配、保留存储与对外通告的订阅摘要
都按它划分，两个内置集群后端会丢弃属于其他命名空间的远端事件。空字符串是默认命名空间
（零值），因此未设置它的部署行为完全不变，包括文档化的 Redis 键布局。

## 定向发布

`PublishToClient` 将一条发布投递到一个 client id，无论它在哪里：

```go
err := broker.PublishToClient(ctx, "device-42", gmqx.Publication{
    Topic:   "cmd/reboot",
    Payload: []byte("now"),
    QoS:     1,
})
```

- 本地活连接通过与本地扇出相同的 `SendLocal` 路径接收，QoS 取发布本身的 QoS（没有订阅可用来
  封顶），并遵守该连接的 QoS 1/2 inflight 窗口；
- 本地无连接的持久会话按普通发布的方式入队：在下次 CONNACK 之后排空，并受
  `MaxOfflineQueueSize` 与 `MaxTotalOfflineMessages` 约束；
- 否则变成 `ClusterTarget` 事件，集群后端接受后返回 nil。Redis 后端只发给持有该 client id
  的节点；TCP 网状后端广播，各节点仅在持有该 id 时应用。

没有任何节点持有该 client id 时调用仍然成功：投递是 fire-and-forget（与集群传输一样
at-most-once），未命中通过 `RecordDroppedMessage("target_unknown")` 计数。只有调用方错误
（空 client id 的 `ErrClientIDRequired`、非法 topic 或 QoS、其他 namespace 的
`ErrNamespaceMismatch`）或集群后端拒绝事件（例如 Redis 不可达，可重试）时才返回错误。

定向发布是服务端投递而非客户端 PUBLISH：不经过 ACL、interceptor、`EventSink`（鉴权由调用方
负责），不写入保留状态；对端收到 target 事件后也不会再交给它的 `Dispatcher`。

## 共享订阅与订阅标识符

两项 MQTT 5 能力均已实现，并在 CONNACK 中宣告为可用（`SharedSubscriptionAvailable`
与 `SubscriptionIdentifierAvailable`）。

### 共享订阅

共享订阅是 `$share/{ShareName}/{filter}` 形式的过滤器（MQTT 5 §4.8.2）。SUBSCRIBE 到达时校验：

- ShareName 不能为空，且不能包含 `/`、`+`、`#`，其后必须是合法的 topic filter；
  违反任一条的 `$share/...` 过滤器按过滤器返回 SUBACK `0x8F`，连接保持可用
  （MQTT 3.1.1 只有 `0x80`，与其它畸形过滤器一样关闭连接）；
- 仅以 `$share` 开头的过滤器（单独的 `$share`、`$sharex/g/t`）仍是普通字面量过滤器；
- 共享订阅上的 No Local 被拒绝（MQTT-3.8.3-4），同样按过滤器回 `0x8F`。

单节点路由：匹配某个组的消息只投递给该组**恰好一个**活成员，按会话 key 排序后 round-robin
选择。同一过滤器的不同组互相独立、各收一份；同一过滤器上的普通订阅者另外再收一份。
组的 round-robin 游标保存在路由索引里，随最后一个成员离开而消失，因此客户端无法通过
不断更换组名让索引膨胀。要点：

- 只有活会话是成员。持久会话订阅了某个组后，订阅会被保存（恢复时还原），但离线期间
  不是成员，**共享状态也不会入队**：其余成员接手这些消息；会话恢复后重新加入组；
- 订阅共享订阅时不重放保留消息（§4.8.2），之后匹配的 publish 正常投递；
- 共享订阅携带的订阅标识符会随被选中成员的投递一起发出。

集群中若“每个节点各自投递”，一个组会每个节点各收一份。因此由**产生该 publish 的节点**为每个组
恰好选出一个节点：

```
candidates = 通告持有该组活成员的其它节点 + 本节点（当索引中有成员时），排序
chosen     = candidates[(fnv64(groupKey) + seat) % len(candidates)]
```

`seat` 是产生节点上按组递增的计数器，使同一组的连续 publish 轮流落到每个持有成员的节点；
哈希决定组的亲和性，不同组无需协调即可分散。选举不需要共享状态、也不需要额外往返，
因此 TCP 网状与 Redis 后端共用同一规则：被选中的节点写进 `ClusterMessage.Shared`
（每个节点一条 `SharedDelivery`），未被点名的节点对该组不做任何投递；被选中节点再在本地
按 round-robin 选成员。

据此确定的行为：

- 一个组在集群内**至多投递一次**；
- 若被选中节点在此期间失去了最后一个成员（断连，或摘要晚了一个刷新周期），它会**丢弃**
  并计入 `Metrics.RecordDroppedMessage("shared_no_member")`；**不会**转交给其它节点的成员：
  集群投递是至多一次，转交可能造成重复。成员全部离开而仍有对端通告该组时同样计数；
- 已经没有任何节点通告的组不再算共享发布：消息只是没有匹配的订阅者，不计数；
- 两个内置后端都在订阅摘要中通告组 key（`subscriptionFilters` 是普通过滤器，
  `sharedSubscriptionGroups` 是共享组）；本包之外的 `Cluster` 实现不实现该通告，
  共享订阅因此只在本节点内投递。旧版本的对端会忽略 `ClusterMessage.Shared`，
  降级为“跨节点收不到共享消息”而不是重复投递。

### 订阅标识符

SUBSCRIBE 可以携带 Subscription Identifier（属性 `0x0B`，取值 1..268435455）。它属于**订阅**
而不是连接：随过滤器一起保存、写入 `SessionStore`（`SessionSubscription.SubscriptionIdentifiers`）、
会话恢复时还原，并附加到该订阅产生的每条 PUBLISH：

- 实时投递；一个会话有多个匹配订阅时，一条 PUBLISH 携带它们全部的标识符并去重（§3.3.2.3.8）；
- 保留消息重放；
- 下次 CONNACK 之后从离线队列排空的消息；
- 共享组投递给被选中成员的投递。

取值 0（以及超过 268435455 的值）是 Protocol Error，以 DISCONNECT `0x82` 结束连接。
gnety codec 在解析阶段就拒绝 0（这是该值唯一可见的位置——gmqx 不维护属性表，见 `props.go`），
因此 broker 仅把这一种拒绝映射为 `0x82`，其余无法解析的报文仍为 `0x81`（Malformed Packet）。

上限：`maxSubscriptionIdentifiersPerSubscription`（8）限制单个订阅保存的数量，
`maxSubscriptionIdentifiersPerDelivery`（8）限制一条出站 PUBLISH 的属性块长度，未能附加的
标识符计入 `Metrics.RecordDroppedMessage("subscription_identifier_limit")`。该属性是逐跳的：
客户端 PUBLISH 中携带的标识符会被剥离，投递的标识符由写出 PUBLISH 的节点附加，
不会经由 `Publication.Props` 或集群传输。


## 持久会话、离线队列与 QoS 1/2 重传

持久会话（Clean Session 清零；MQTT 5 上为 Clean Start 清零且 Session Expiry Interval
非零）在没有活动连接时保留状态：订阅、投递给这些订阅的消息，以及状态结束的截止时间。
这些状态都位于 `SessionStore` 之后，并且存储中保存的正是“没有归属连接”的会话——活动连接
拥有自己的会话，因此一条 publish 只会到达一个会话一次。

`SessionStore`（`Load`、`Save`、`Delete`、`Keys`）存在的目的是让部署可以把会话状态移出
进程。当 `Config.SessionStore` 为 nil 时 `New` 安装 `MemorySessionStore`，即早期版本的
内存行为完全不变，且它仍是默认值；本包还提供另一个实现 `NewRedisSessionStore`（见下面的
**共享会话**）。broker 在持有自身会话锁的情况下调用存储，
因此实现不得回调 broker。路由完全不读取存储：broker 在保存会话时把其订阅写入路由索引，
在接管或过期时移除。`Keys` 仍然必需——`New` 用它把部署在 broker 创建之前恢复进存储的会话
索引一次，过期清扫器用它枚举需要执行的截止时间；运行中的 broker 不会重读被外部进程写入的
存储。

**共享会话。** 默认情况下会话只存在于持有它的节点上，因此重连到另一个节点的客户端会从零
开始。`NewRedisSessionStore` 把这份状态放进 Redis，而
`NewRedisClusterWithSessionStore(cfg)` 用一份 `RedisClusterConfig` 同时构建 Redis 集群后端
与该存储，两者使用相同的地址、数据库与 `ChannelPrefix`：

```go
redisCluster, sessions, err := gmqx.NewRedisClusterWithSessionStore(gmqx.RedisClusterConfig{
    Addr: "redis:6379",
})
if err != nil {
    return err
}
defer sessions.Close()
broker, _ := gmqx.New(gmqx.Config{
    NodeID:         "n1",
    ClusterBackend: redisCluster,
    SessionStore:   sessions,
})
```

存储为每个已存储会话保留一个键，位于集群前缀之下：

| 键 | 内容 | TTL |
|---|---|---|
| `<prefix>:session:<namespaceKey(ns, clientID)>` | JSON `{v, client, expires, subs, queue}`：会话的订阅及其选项、排队消息（payload、QoS、retain 标志与属性块）以及会话截止时间 | 会话自身的截止时间 |

键正是 broker 请求的那个键（已带命名空间前缀），因此共用同一个 Redis 的两个命名空间不会
相撞，而 `session` 段与集群自己的 `node`、`owner`、`member`、`subs`、`client` 键没有任何
重叠。TTL 是会话本身而不是缓存寿命：带 Session Expiry Interval 的会话即使没有清扫器运行也会
在到期时从 Redis 消失，没有该间隔的会话（MQTT 3.1.1）不设 TTL，而截止时间已过的状态会被
删除而不是写入。无法解码的值会被删除并计入
（`RecordDroppedMessage("session_store_corrupt")`），一条坏记录不会让 CONNECT 失败；Redis
命令失败计入 `"session_store_error"` 并退化为“没有会话”（与集群传输一样是至多一次语义）。

两条规则避免同一个会话被两个节点同时服务：

- 取出会话是原子操作（`Take`，一次 Redis 往返完成读取与删除），因此两个节点同时对同一个
  client id 重连时不会都恢复该会话。该存储还实现了可选的 `SharedSessionStore` 接口，broker
  在对端 kick 顶掉连接时使用它：被顶掉的节点既不把自己的副本写回（那会让对端正服务的会话
  复活），也不删除该键（那会抹掉对端刚保存的状态）。使用默认内存存储时行为不变——被 kick
  的会话状态留在原处，因为只有该节点能访问它。
- clean CONNECT 无论落在哪个节点都会丢弃共享会话，而且每个节点的清扫器只清理自己命名空间
  的键。

存储搬运的是状态而不是路由：为已存储会话排队的，是把它放进自己路由索引的节点（保存它的
节点，或启动时它就存在的节点）。与节点本地存储一样，另一个节点收到的 publish 不会为这里的
离线会话排队，除非集群把它路由过来（见下面的已知限制）。`Save` 替换整条记录，而 broker
的会话锁只串行化单个节点的写入，因此两个节点同时为同一个已存储会话排队时可能丢掉其中一条
——离线会话的队列实际上由保存它的节点独占。

**过期。** MQTT 5 的 Session Expiry Interval 是一个截止时间：断连时把
`ConnectInfo.SessionExpiry` 记为 `now + interval`，有界清扫器（`SessionSweepInterval`，
每轮最多检查 512 个存储会话）在截止时间过后删除该会话及其订阅与排队消息。CONNECT 路径
也会检查截止时间，因此即使清扫器尚未到达，过期会话也绝不会被恢复。非 clean 会话上的零
间隔保持旧行为且永不过期——这是 MQTT 3.1.1 的情况（协议没有该间隔），而在 MQTT 5 上零
间隔意味着会话随连接结束，因此不会存储任何状态，也就没有可过期的东西。

**队列。** 只有持久会话会排队（本家族的参考实现对 clean 会话也排队，从而把消息投递给
一个明确表示不需要会话的客户端；gmqx 不复制该缺陷）。队列是每会话的 FIFO，元素是 PUBLISH
所需的一切——topic、QoS、retain 标志、payload、MQTT 5 属性——但不含 packet id：id 在
排空时分配。`MaxOfflineQueueSize` 限制单个会话并丢弃其最旧的消息；
`MaxTotalOfflineMessages` 限制整个 broker 并拒绝最新的消息。两种丢弃都可观测
（`RecordDroppedMessage("offline_queue_full")`、`RecordDroppedMessage("offline_queue_total")`），
`RecordQueueDepth("offline", n)` 报告当前深度。队列在下次 CONNECT 的 CONNACK 之后立即
按序排空。既拿不到 inflight 窗口、也拿不到空闲 packet id 的消息会降级为 QoS 0 投递，
而不是让会话停滞——这正是参考实现在 packet id 用尽时采用的降级方式。

**重传。** 出站 QoS 1/2 投递会以 packet id 为键保存在每连接的 inflight 表中，直到客户端
确认。每个连接一个定时器（而不是每条消息一个）驱动所有重传：首次重传在
`QoSRetryInterval`（默认 5 秒）之后，之后每次尝试等待翻倍，`QoSMaxRetries`（默认 5）
限制尝试次数，达到上限后该交换保持未确认状态且其 packet id 继续保留。PUBLISH 重传携带
DUP 标志与原始 packet id（参考实现从不设置 DUP——不复制该缺陷）。QoS 2 走
PUBLISH → PUBREC → PUBREL → PUBCOMP，PUBREL 会重传直到收到 PUBCOMP。
`MaxInflightQoS1` 限制连接的未确认 QoS 1 PUBLISH 数；`MaxInflightQoS2` 限制其每个方向的
QoS 2 交换数（入站 QoS 2 PUBLISH 超出窗口时在 MQTT 5 上回复 PUBREC 0x97 Quota exceeded）。

**inflight 队列。** 会超出窗口的 QoS 1/2 投递按到达顺序进入该连接的 inflight 队列，并在
更早消息的确认腾出位置后发送：慢速订阅者只花内存，不丢消息，发布方也不会被阻塞。队列有界
（`MaxInflightQueueSize`，默认 256）：只有队列本身已满时才丢弃，而该丢弃会被计数两次
（`RecordDroppedMessage("inflight_overflow")`、`RecordOverflow("inflight")`）并以 Warn
级别带 client id 记录日志——发布方收到的 PUBACK 已经代表 sink 接受了这条消息，丢失必须
可见。队列属于连接，随连接结束，不会像离线队列那样写入会话。

**写协程。** 每个连接的所有数据包都由同一写协程按入队顺序发出：PUBLISH、PUBACK/PUBREC/
PUBREL/PUBCOMP、SUBACK、PINGRESP。其队列有界（`MaxOutboundQueueSize`，默认 256 个包），
每次写都带超时（`WriteTimeout`，默认 5 秒），因此停止读取的对端无法拖住发布方读循环、
关闭连接时的遗嘱发布，或节点唯一的集群接收协程。队列满时 QoS 0 PUBLISH 被丢弃并计数
（`RecordDroppedMessage("outbound_queue_full")`、`RecordOverflow("outbound_queue")`）；
对 QoS 1/2 PUBLISH 或确认包则改为断开慢速消费者（`RecordDroppedMessage("slow_consumer")`
并记录 Warn 日志）——这一取舍宁可让重连可见，也不让消息无声丢失。队列按包数而非字节数
有界，因此单个连接最多占用 `MaxOutboundQueueSize` 个、每个不超过 `MaxPacketSize` 字节的
数据包。两个协程与重传定时器都随连接结束，并由 `close` 等待；会话清扫器由 `Stop` 等待。

有意保留的限制（上文均给出原因）：inflight 交换及其队列属于连接，因此重连会恢复离线队列
但不会恢复上一次连接中未确认或已排队的投递；入站 QoS 2 交换在连接关闭时丢弃（与参考
实现一致）；队列位于收到该 publish 的节点上，而集群对端通告的订阅摘要只覆盖活动会话，
因此其他节点上的 publish 不会在此处为离线会话排队（共享会话存储搬运的是状态，而不是这条
路由）。

## 接管、延迟 will 与保留消息过期

**接管（takeover）。** 用一个已在线的 client id 发 CONNECT 就是一次接管。`UNIQUE` 模式下
新连接被拒绝（MQTT 5 回 CONNACK 0x85，3.1.1 回返回码 2）。`KICK` 模式下新连接接管会话：
被顶掉的连接在 MQTT 5 上先收到服务端 `DISCONNECT` 0x8E（Session taken over）再被关闭——
MQTT 3.1.1 没有服务端 DISCONNECT，只能看到连接断开——会话状态转移到新连接。

`NotKickSeconds`（默认 30）是这次顶替的防抖窗口：如果被顶替的连接建立不足该窗口，接管被
拒绝，MQTT 5 回 CONNACK 0x9F（Connection rate exceeded），3.1.1 回返回码 3（Server
unavailable），因此同一个 client id 的两端不会互相循环顶替。0x9F 正是 MQTT 5 为“连接被限速”
保留的返回码；0x8A（Banned）会被理解为管理封禁，0x89（Server busy）是整机过载，0x9C（Use
another server）是本 broker 无法给出的重定向。`NotKickSeconds` 取负值即取消窗口，每次
CONNECT 都立即接管——这是窗口出现之前的行为，也是 MQTT 本身规定的行为。

**跨节点接管。** 每次成功 CONNECT 都会向集群广播一次 kick，两种连接模式都一样：客户端重连到
另一个节点时，旧节点上的连接否则会一直存活到 keepalive 过期，并在这段时间里重复投递每条
消息。真正顶掉连接的 kick 与本地接管一样处理（MQTT 5 上先发 DISCONNECT 0x8E），并取消本节点
为该 client id 挂着的延迟 will。事件携带的是 client id 而不是 topic，所以发往所有对等节点，
命名空间与 `(Origin, Seq)` 去重规则照旧。

*kick 的 epoch。* kick 还携带产生它的节点，以及产生它的连接的建立时间
（`ClusterMessage.ConnectTime`，该节点墙钟的 Unix 纳秒），接收端只对更旧的连接应用：

| 本节点持有的连接 | kick | 结果 |
|---|---|---|
| 早于 kick 的 epoch | 更新 | 应用（顶掉） |
| 晚于 kick 的 epoch | 更旧 | 拒绝，`RecordDroppedMessage("cluster_kick_stale")` |
| 同一时刻，本节点 id 更大 | 相同 | 拒绝（本连接胜出） |
| 同一时刻，本节点 id 更小 | 相同 | 应用（对端连接胜出） |
| 没有 epoch（`ConnectTime` 为 0） | 旧版 | 应用，`RecordDroppedMessage("cluster_kick_legacy")` |

因此迟到或乱序的 kick 无法关闭比它本应顶替的连接更新的连接，而同一时刻接受同一 client id
的两个节点会得到同样的结论，而不是两边都关闭。被拒绝的 kick 会被计数并以 Debug 级别记录，
绝不静默忽略。`Config.NotKickSeconds` 在这一侧同样生效：如果 kick 到达时它所指的连接仍年轻于
窗口，该 kick 被拒绝（`RecordDroppedMessage("cluster_kick_rate_limit")`），因此两个节点
无法以快于窗口的速度互相抢走 client id。

*时钟假设。* epoch 是墙钟读数，因为单调时钟读数在另一个进程里没有意义。跨节点排序假设集群
各节点时钟已按 NTP 同步到 `NotKickSeconds` 之内；更大的偏差不会让已被顶掉的连接复活（窗口
拒绝为其兜底），只会让对端应用或拒绝一个它本会作出不同判断的 kick。单节点内部不跨时钟：
本地窗口用 `time.Since` 比较连接的单调时钟起点。

*滚动升级。* 没有 epoch 的 kick 来自早于 kick epoch 的构建。它会被无条件应用——正是那个构建
本身的行为——并计入 `"cluster_kick_legacy"`，因此混合版本的集群退化为旧语义而不是拒绝所有
kick。新节点的 kick 到达旧对端时，只是多了一个旧构建会忽略的 JSON 字段，所以线格式在两个
方向上依然兼容。

**Will Delay Interval。** MQTT 5 允许 WILL 指定延迟。will 在两件事中较早的一件发生时发布：
延迟到期，或会话结束。

- 会话随连接一起结束（Clean Start 置位，或 Session Expiry Interval 为 0）时 will 立即发布，
  因为延迟不能长于它所依附的会话。
- 其余情况按 `min(Will Delay Interval, MaxWillDelayInterval, 剩余 Session Expiry Interval)`
  排期：在延迟之内过期的会话，其 will 在过期时刻发布。
- 恢复该会话的 CONNECT 会取消待发布的 will，本地直接取消，在其他节点上则通过 CONNECT 时
  广播的 kick 取消。带 Clean Start 的 CONNECT 不是恢复而是丢弃会话，此时上一连接的 will
  在这次 CONNECT 时立即发布。
- broker 停止时丢弃仍然挂着的 will：已经没有地方可发布，也不允许定时器活过 `Stop`。

排期是“每个待发布 will 一个定时器”（不是全 broker 的 ticker），只在 will 等待期间存在。
超过 `MaxWillDelayInterval`（默认 5 分钟）的延迟会被截断，并计入
`RecordOverflow("will_delay")`。线格式上的延迟是整秒，所以毫秒级的测试会缩短 broker 的
“协议秒”（`Config.unit`，非导出，生产环境恒为 `time.Second`）。

**保留消息过期。** 保留发布带的 MQTT 5 Message Expiry Interval 以“截止时间”（发布时间加
间隔）存储，而不是原样保存。回放时，已过截止时间的消息不再投递并从存储中删除（计入
`RecordDroppedMessage("retained_expired")`）；仍在有效期内的消息回放的是“剩下的间隔”（向上
取整，因为在线格式上取整成 0 意味着“永不过期”），而不是发布时的旧值。不带间隔的保留消息
永不过期。删除发生在 broker 真正查看该消息时（匹配的 SUBSCRIBE 回放），因此一条没人订阅的
过期保留消息会占用一个 map 条目，直到下一次匹配的订阅。从对等节点复制来的保留发布以到达
本节点的时刻计时，因为集群事件不携带源节点的发布时刻。

**CONNECT 与鉴权时限。** `ConnectTimeout`（默认 10 s）在读取第一个数据包之前设置读截止时间，
在 CONNACK 写出后清除；随后由读循环设置它自己负责的 keepalive 截止时间。因此只建连不说话、
或 CONNECT 发得太慢的客户端会被断开，而不是永久占用一个连接槽、一个协程和一个会话。
`AuthTimeout`（默认 5 s）限制 `AuthManager.Authenticate` 与 `AclManager.Check`：钩子在自己的
协程里运行，收到的 context 携带该截止时间；超时的调用一律视为拒绝——认证失败（CONNACK
0x87）或授权不通过（SUBACK 0x87 / 不返回成功 PUBACK）——绝不视为成功。内建的 `AllowAll`
走同步路径，所以保持默认配置的 broker 不会为每次发布多起一个协程；`AuthTimeout` 取负值即
取消时限，此时钩子必须自己遵守 context。

## 传输分层

TCP/TLS/WebSocket/WSS 监听器由
[`gnety`](https://github.com/maxid/gnety) 传输框架提供（`RawHandler`
模式）。MQTT 线格式位于 `gnety/codec/mqtt`：每个数据包都用其类型化的 `Decoder`
读取、用 `mqtt.Encode` 写出，因此 gmqx 拥有 broker
状态机，而不拥有任何自己的数据包布局。

## 集群后端

`gmqx.NewTCPCluster` 通过 gnety TCP 连接各 broker 节点，在带长度前缀的分帧之上
使用带版本的 JSON 帧：

```go
cluster := gmqx.NewTCPCluster(gmqx.TCPClusterConfig{
    Addr:  "10.0.0.1:7946",
    Seeds: []string{"10.0.0.2:7946", "10.0.0.3:7946"},
})
broker, _ := gmqx.New(gmqx.Config{NodeID: "n1", ClusterBackend: cluster})
```

`Addr` 必须显式指定（不能是 `:0`），并且是其他节点必须在其
`Seeds` 中使用的地址。每个节点只拨号那些通告地址排序在自己之后的节点，因此在每个节点上
于 `Seeds` 中列出所有节点会为每一对节点产生一条连接。两端都发起拨号的一对节点
（例如某个 seed 写出的地址与该节点自己的 `Addr` 不同）会由握手折叠，
因此一个 node id 永远不会有两个成员。失败的对等节点会被丢弃并重试。

### 成员关系

每条链路都以一个带版本的握手帧开始，其中携带 node id、
通告的监听地址、进程化身（incarnation，每次 `Start` 随机生成）以及
发送方当前的成员视图。该视图是参考性的——它让节点能发现某个对等节点
以不同地址认识它——而排序规则使用通告的
`(NodeID, Addr, Incarnation)` 三元组：

- 使用其他线格式版本的对等节点会在握手时被拒绝；来自不兼容版本的帧
  永远不会创建成员。
- 每个 node id 恰好存在一个逻辑成员。指向同一化身的第二条链路
  会折叠为由通告地址较低方拨出的链路
  （与挑选拨号方是同一条规则），落败的链路会被关闭并记录日志。
- 使用新化身的握手意味着对等节点已重启：它会替换
  旧链路并重置 `Since`。
- 心跳每隔 `HeartbeatInterval` 发送一次；连续错过
  `HeartbeatMissBudget` 个间隔的成员会被驱逐并关闭其链路。在该窗口内
  始终未完成握手的链路也会被关闭。
  重连仍然有效：拨号循环会重建出站链路，
  对等节点会重新打开入站链路。
- 来自未知或已被驱逐节点的事件仍会交给
  broker（可用性优先于严格性），但只有握手才能让该节点
  重新成为成员。
- `Members()`（可选的 `MemberView` 接口）暴露 `Member{NodeID, Addr,
  Incarnation, Since}`；`Cluster` 接口本身没有变化：

  ```go
  if view, ok := cluster.(gmqx.MemberView); ok {
      for _, m := range view.Members() {
          log.Printf("%s at %s since %s", m.NodeID, m.Addr, m.Since)
      }
  }
  ```

### 去重与环路

每个本地产生的事件都会打上 `Origin`（产生该事件的节点）和
`Seq`（该来源单调递增的序号）。接收方维护一张有界的、带 TTL 的
已应用 `(Origin, Seq)` 标识表，对重复事件只应用一次。
该表在 `DedupTTL` 时长内保存 `DedupSize` 个标识（最旧的先被驱逐）；过期在
插入时惰性清理，因此内存保持 O(DedupSize)。被丢弃的重复、环路
事件和转发事件都会被计数，并在设置了 `TCPClusterConfig.Metrics`
时通过它上报。

收到的事件永远不会被转发，来源为接收节点自身的事件会被丢弃，
因此重广播拓扑不会形成环路。

### 定向路由

每个节点通告其本地会话所订阅的主题过滤器集合，每隔 `SummaryInterval`
刷新一次，订阅变化时立即刷新。
一次 publish 只会写给摘要与其主题匹配的对等节点（使用
broker 常规的 `+`/`#` 匹配）。摘要未知或早于三个间隔的对等节点
仍会收到该 publish——仅该节点，绝不向所有人泛洪。保留消息的
publish 和 kick 仍然发给每个对等节点：保留状态
必须存在于每个节点上，以供未来的订阅者使用；而 kick 指定的是 client id
而不是主题，因此只有持有该会话的节点才能对其生效。

一个节点最多通告 `MaxSummaryFilters` 个过滤器；当实际数量更多时，
存在时间最长的过滤器会被排除（较新的订阅继续保持路由），
代价是无法路由仅匹配被排除过滤器的 publish。

从对等节点收到的保留消息会按本地保留消息的规则写入接收节点自己的
保留存储：空载荷删除该主题，`MaxRetainMessages` 限制存储数量，
消息携带的 QoS 与属性一并保留。这正是广播的意义所在——之后连接到
该节点的订阅者可以从其存储中收到该值。保证的是状态最终存在于
存储中：远程保留消息可能在本地订阅者已被接纳之后才到达，
broker 不会向已经连接的会话重放该值。

| 字段 | 默认值 | 说明 |
|---|---|---|
| `Addr` / `Seeds` | — | 通告的监听地址 / 要拨号的对等节点 |
| `ReconnectInterval` | 500ms | 重新拨号某个对等节点前的等待时间 |
| `HeartbeatInterval` | 1s | 每条链路的心跳周期 |
| `HeartbeatMissBudget` | 3 | 驱逐前允许错过的间隔数 |
| `DedupSize` / `DedupTTL` | 4096 / 30s | 重复抑制表的边界 |
| `SummaryInterval` | 1s | 订阅摘要刷新周期 |
| `MaxSummaryFilters` | 256 | 每个节点通告的过滤器数量 |
| `Metrics` / `Logger` | 无 / `slog.Default()` | 丢弃计数器 / 成员关系日志 |

每次成功连接都会广播一次 kick（两种连接模式都是），因此 client-id 接管即使在旧会话位于
另一个节点时也会将其断开（`UNIQUE` 只在本地裁决，因为节点看不到对端会话）。被顶掉的连接在
MQTT 5 上收到 DISCONNECT 0x8E，对端为该 client id 挂着的延迟 will 也会被取消。详见上面的
接管一节。

节点之间的投递始终保持尽力而为：某个对等节点摘要未知时到达的 publish
仍会发送给该节点，但订阅尚未对发布节点可见（最多
一个 `SummaryInterval`）的订阅者可能漏掉一条消息。集群不会跨节点
同步订阅。

## Redis 集群后端

`gmqx.NewRedisCluster` 用集中式扇出取代 TCP 网状结构。节点之间
完全不互相连接：它们共享 Redis，集群所知的一切——包括成员关系
与路由表——都存放在那里，而不是存在于 N² 条对等链路中。

```go
redisCluster := gmqx.NewRedisCluster(gmqx.RedisClusterConfig{
    Addr:     "redis:6379",
    Password: os.Getenv("REDIS_PASSWORD"), // optional
    DB:       0,                            // optional
})
broker, _ := gmqx.New(gmqx.Config{NodeID: "n1", ClusterBackend: redisCluster})
```

同一个 Redis 也可以保存集群的会话，使重连到另一个节点的客户端恢复其订阅与队列而不是从零
开始。这是可选项，并且由同一份配置构建：

```go
redisCluster, sessions, err := gmqx.NewRedisClusterWithSessionStore(cfg) // cfg: RedisClusterConfig
if err != nil {
    return err
}
defer sessions.Close()
broker, _ := gmqx.New(gmqx.Config{NodeID: "n1", ClusterBackend: redisCluster, SessionStore: sessions})
```

键布局、TTL 规则以及接管对共享会话的影响见持久会话一节的 **共享会话**。

### 键与通道

每个键与通道都位于同一个前缀之下（`ChannelPrefix`，默认
`gmqx:cluster`），该前缀在所有节点上必须一致：

| 键 / 通道 | 内容 | TTL |
|---|---|---|
| `<prefix>:node:<nodeID>` | 某个节点的 pub/sub 通道 | — |
| `<prefix>:owner:<nodeID>` | JSON `{node, inc, since}`：node id 所有权租约 | `NodeTTL` |
| `<prefix>:member:<nodeID>` | JSON `{node, inc, since}`：存活性与身份 | `NodeTTL` |
| `<prefix>:subs:<nodeID>` | JSON `{node, filters, updated}`：订阅摘要 | `SummaryTTL` |
| `<prefix>:client:<namespaceKey(ns, clientID)>` | `<nodeID>\x00<incarnation>`：client id 由哪个节点持有 | `PresenceTTL` |
| `<prefix>:session:<namespaceKey(ns, clientID)>` | JSON 会话记录，由配置的 `RedisSessionStore` 写入（见 [共享会话](#持久会话离线队列与-qos-12-重传)） | 会话的截止时间 |

一个节点只订阅自己的通道，不订阅其他任何通道。一次 publish 会为每个
感兴趣的节点各执行一次 `PUBLISH`，因此 N 个节点的代价是 O(N) 次 Redis 操作
而不是 O(N²) 条链路，也不存在每个节点都必须接收并过滤的共享通道。
只有自身通道承载该事件的节点才会应用它。

### 成员关系

成员键就是整个成员关系协议。节点每隔
`SummaryInterval` 重写它（本地订阅变化时也会重写），Redis 会在最后一次刷新之后
`NodeTTL` 将其删除，因此崩溃的节点无需握手、驱逐流程或
干净关停即可退出视图。Redis 不可达时 `Start` 会拒绝运行；节点只有在通道订阅
确认之后才注册，因此能看到成员键的对等节点已经能够到达它。

`Members()`（可选的 `MemberView` 接口）读取成员键，因此每个节点
无需共享进程内存就能报告相同的成员关系：`[]Member`
按 node id 排序、每个化身一条记录、`Member.Addr` 为空（Redis 没有
对等地址）、`Since` 为该化身注册的时刻。Redis 无法读取时它报告没有
任何对等节点，而不是给出过时视图。重启的节点会以新化身重新出现；
停止节点会删除其键，因此对等节点会立即移除它，而不是在 `NodeTTL` 之后。

node id 的所有权与成员关系分开强制。`Start` 以 `SET NX` 和 `NodeTTL`
获取 `<prefix>:owner:<nodeID>`，随成员键一起续期，并在 `Stop` 时释放。
第二个使用同一 id 的存活进程会以 `gmqx.ErrNodeIDInUse` 失败（错误中
给出持有该 id 的化身），而不是订阅同一通道、重复应用每个事件并在
`Members()` 中折叠为一条记录。租约是一个 TTL，因此未释放就死亡的
进程最多阻塞重启直到 `NodeTTL` 过期。TCP 后端没有这样的键：
它的握手已经保证每个 node id 恰好一个成员，也没有可用于仲裁租约的
共享存储。

### 定向路由

每个节点将其订阅摘要写入 `<prefix>:subs:<nodeID>`，对等节点在构建
路由视图时读取它，该视图最多每个 `SummaryInterval` 刷新一次（同样正是这一刷新在
Redis 不可达时使 `Spread` 返回错误）。规则即 TCP 后端的规则：

- 一次 publish 只发往通告摘要与其主题匹配的节点；
- 摘要未知的节点（没有键，或处理器无法描述其
  会话）仍会收到该 publish——仅该节点，绝不泛洪，因为
  缺少摘要不应导致投递失败；
- 保留消息的 publish 和 kick 会发往每个已知节点的通道：保留状态
  必须存在于每个节点上，以供其未来的订阅者使用；而 kick 指定的是 client id
  而不是主题，因此只有持有该会话的节点才能对其生效；接收节点按与
  本地保留消息相同的规则存储该保留消息（空载荷删除、`MaxRetainMessages`
  限制存储数量、保留 QoS 与属性），保证的是状态最终存在于存储中，
  而不是向已经连接的会话重放；
- 一个节点最多通告 `MaxSummaryFilters` 个过滤器，存在时间最长
  的过滤器会被排除。

定向发布（`Broker.PublishToClient`）改用 presence 路由：broker 报告每次 CONNECT
以及真正结束的会话，后端写入 `client:<id>` 记录标明本节点，在客户端仍在线（或持久会话仍
存储在本节点）期间刷新，并用 compare-and-delete 删除——因此旧连接的断开不会抹掉已经迁移到
其他节点的 client id 记录；对等节点的 CONNECT 时 kick 也会让本节点放弃声明，即使持久会话
仍存储在本节点——队列是本地的，而客户端本身已经连接在对端。`Spread` 读取该记录，只写入那个
节点的通道。没有记录意味着没有
节点声明该 client id：事件不会广播，而是用 `RecordDroppedMessage("target_unknown")` 计数
并丢弃；与 CONNECT 竞争且不足一个刷新周期的定向发布会同样丢失，这是整个集群传输的
at-most-once 取舍。kick 故意保留广播：过期的 presence 记录不能把僵尸连接留在客户端真正
迁移到的节点之外。

### 去重与环路

与 TCP 后端完全相同，且由同一份代码实现：每个本地产生的事件都会打上
`Origin` 和 `Seq`，接收方会在事件到达去重表之前丢弃 `Origin`
为自身节点的事件（因此自身流量不会污染该表），通过共享的有界
TTL 表丢弃重复的 `(Origin, Seq)`，并且从不转发收到的事件。
`RedisClusterConfig.Metrics` 会收到相同的 `cluster_duplicate`、`cluster_loop`
和 `cluster_forward` 丢弃原因。

### 故障行为

- 当 `Addr` 为空、某个 TTL/尺寸为负数、某个 TTL
  无法长于 `SummaryInterval`、node id 被另一个存活进程持有
  （`gmqx.ErrNodeIDInUse`），或 Redis 无法到达时，`Start` 返回错误。
- 运行时，本地产生的 `Spread` 会返回 `gmqx.ErrClusterUnavailable`
  （包装底层原因），而不是就地重试；broker 会记录日志并继续。
  收到的事件永远不会重发，因此没有任何东西会重试远程事件。
- 接收循环以有界退避（50ms 起翻倍至 1s）重新订阅
  直到 `Stop`，始终保持恰好一个 goroutine 和一个订阅，并且只记录
  进入与离开降级状态的转换。
- `Stop` 是幂等的，会注销该节点的键，并等待两个循环，因此
  它在 Redis 宕机时也能返回，且不会遗留 goroutine。

| 字段 | 默认值 | 说明 |
|---|---|---|
| `Addr` / `Password` / `DB` | — / — / 0 | Redis 端点；`Addr` 为必填 |
| `ChannelPrefix` | `gmqx:cluster` | 所有通道与键的命名空间 |
| `SummaryInterval` | 1s | 键刷新与路由视图刷新周期 |
| `SummaryTTL` | 3×`SummaryInterval` | 摘要键的生命周期；过期即表示“摘要未知” |
| `NodeTTL` | 3×`SummaryInterval` | 成员键的生命周期；过期即驱逐该节点 |
| `PresenceTTL` | 3×`SummaryInterval` | client-id presence 记录寿命；过期即“无人声明该 id” |
| `HeartbeatInterval` | 1s | 用于故障检测与日志记录的 Redis `PING` 周期 |
| `DedupSize` / `DedupTTL` | 4096 / 30s | 重复抑制表的边界 |
| `MaxSummaryFilters` | 256 | 每个节点通告并接受的过滤器数量 |
| `Metrics` / `Logger` | 无 / `slog.Default()` | 丢弃计数器 / 成员关系日志 |

Redis 投递是至多一次的，与 TCP 投递完全一样：在对等节点处于两次化身之间时
发布的事件会丢失，而不是排队。node id 在每个集群中必须唯一：
Redis 后端会拒绝使用已被持有 id 启动的第二个存活进程
（`ErrNodeIDInUse`），TCP 网状结构则把重复链路折叠为每个 node id
一个成员。

## 版本管理

见 [VERSIONING.md](VERSIONING.md)。

## 许可证

MIT。见 [LICENSE](LICENSE)。

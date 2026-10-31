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
- QoS 0/1 流程（按订阅 QoS 降级）与抑制入站重复的 QoS 2 握手（broker 侧重传与
  inflight 限制仍在路线图上）
- 保留消息、通配符订阅（`+`/`#`）、带 QoS/retain/属性的遗嘱消息、keepalive
- 线级 MQTT 3.1.1/5 一致性测试套件（`TestConformance`）
- 可插拔钩子：`AuthManager`、`AclManager`、`EventSink`（生命周期/消息）、
  `MetricsManager`、`Interceptor`
- 连接模式：`UNIQUE` / `KICK`（client id 接管）
- 限制：最大连接数、最大订阅数、最大保留消息数、
  连接速率限制
- 优雅关闭与 client-id 会话接管
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
| `MaxConnections` | 0（不限） | 并发连接数限制 |
| `MaxTopicSubscriptions` | 0 | 每会话订阅数限制 |
| `MaxRetainMessages` | 0 | 保留消息数量限制 |
| `ConnectionRateLimit` | 0 | 连接数/秒的令牌桶 |
| `Auth` / `ACL` / `Sink` / `Metrics` | 全部放行 / 空实现 | 可插拔 SPI |

完整的 MQTT 5 属性/语义、完整的 QoS 2 以及客户端库都在路线图中（见 [ROADMAP.md](ROADMAP.md)）。

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

在 `ConnectModeKick` 下，每次成功连接都会广播一次 kick，因此 client-id
接管即使在旧会话位于另一个节点时也会将其断开。

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

### 键与通道

每个键与通道都位于同一个前缀之下（`ChannelPrefix`，默认
`gmqx:cluster`），该前缀在所有节点上必须一致：

| 键 / 通道 | 内容 | TTL |
|---|---|---|
| `<prefix>:node:<nodeID>` | 某个节点的 pub/sub 通道 | — |
| `<prefix>:owner:<nodeID>` | JSON `{node, inc, since}`：node id 所有权租约 | `NodeTTL` |
| `<prefix>:member:<nodeID>` | JSON `{node, inc, since}`：存活性与身份 | `NodeTTL` |
| `<prefix>:subs:<nodeID>` | JSON `{node, filters, updated}`：订阅摘要 | `SummaryTTL` |

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

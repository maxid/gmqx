[English](CHANGELOG.md) | 简体中文

# 变更日志

> 开发快照不打 tag。条目按变更落地日期分组（新到旧），每个日期只记当天**最终状态**：被后续步骤取代的中间步骤并入描述最终形态的那一条，不按步骤各记一条。中文版与英文版逐条对应（同日期、同条目、同顺序），代码标识符与协议名保持原文。

## 2026-11-03

### 新增

- **`Broker.PublishMessage`：宿主自己注入一条发布。** 嵌入方原先没有办法发一条"不是客户端发来的"
  消息——连接问候、告警、来自其它子系统的命令。它走正常的接受路径：retained 发布在路由前先入库
  （并写透持久化），`Config.Dispatcher` 决定活副本去哪，因此集群会像对待任何发布一样扩散它。它不跑
  `Interceptor` 也不跑 `EventSink.OnPublish`——那两条属于入站连接。`broker-demo` 用它演示：
  `EventSink` 对每个被接受的连接回一条 `<topic>_reply` 上的 retained `hi!`；`cluster-node` 用它让
  集群变得可见：问候会被复制到 peer，因此在另一个节点订阅仍能拿到。

- **handler 绑在 filter 上，与 paho 一致。** `client.Subscription` 新增 `Handler`，
  `Client.AddRoute(filter, handler)` 可以只注册路由不订阅，`Config.OnMessage` 降为"没有路由认领时"
  的兜底（即 paho 的 `DefaultPublishHandler`）。一条发布命中的每条路由都会跑，按注册顺序，在投递
  goroutine 上；共享订阅按内层 filter 注册。`docs/CLIENT.md`（中文版同步）给出 paho → gmqx 的
  映射表，以及用 `Token` 的调用方需要改什么。

- **CI 覆盖到本地跑过的那些闸。** 工作流加了 `-race` 作业（broker 是并发程序）、带 Docker 的
  `integration` 作业（跑容器矩阵）、以及一个短时 fuzz 作业，跑两个由 peer 控制输入的解析器：
  `FuzzMatch`（主题过滤）与 `FuzzDecodePropertyBlock`（MQTT 5 属性块）。

- **`gmqx/client` 支持 MQTT 3.1。** 用 `client.Version31` 选择；客户端把 CONNECT 写成
  `MQIsdp`/3，其余一律按 3.1.1 收发——正是 broker `normalizeV31Connect` 的镜像，不需要改编解码器。

- **新增 `docs/DEPLOYMENT.md`**（中文版同步）：三节点 seed 列表、持久化选型与 locker、
  `NotKickSeconds` 抢占窗口、监听器旋钮，以及滚动升级保留什么、不保留什么。

- **取走会话的节点会让 peer 释放它的路由。** 一个节点取走已存储的会话时广播
  `ClusterSessionReleased`（新集群事件 `session-released`），于是写过它的节点会删掉自己已经服务不了
  的离线路由项，而不是继续往一个没人读的 key 里排队。接收方先查自己的 store，会话仍在就保留路由，
  因此节点本地 store 的行为与以前完全一致；不认识该事件的旧版本 peer 直接丢弃，也只回到旧行为。
  三个内置后端都覆盖：TCP 与 gossip 像 kick 一样泛洪，Redis 走发布订阅；
  `cluster.SessionReleaseHandler` 是与 `TargetHandler` 并列的可选处理接口。

- **集群 × 持久化矩阵在真实后端上跑。** `-tags=integration` 下，
  `internal/engine/cluster_persistence_integration_test.go` 用 testcontainers 起
  `redis:7-alpine` 与 `ghcr.io/olric-data/olric:latest`，对"节点本地默认"和两个共享后端跑同一组
  场景：保留消息在任何一种后端下都能到达另一个节点（集群复制它），但只有共享后端能跨重启活下来；
  会话只有共享后端才能在另一个节点续上。`TestSoakClusterSharedStore` 保留浸泡的连接抖动（churn），并加上
  在两个节点之间迁移的会话；`cluster_persistence_bench_integration_test.go` 给出两条路径的代价，
  数字记在 [PERFORMANCE.zh-CN.md](docs/PERFORMANCE.zh-CN.md)。
  [PERSISTENCE.zh-CN.md](docs/PERSISTENCE.zh-CN.md) 现在写明这些用例守的那条规矩：集群不会替你选
  后端——`Config.Store` 不配置时就是节点本地，开不开集群都一样。仅测试用的新增依赖：
  `testcontainers-go`、`gcache/olric`、`olric-data/olric`；`go test ./...` 仍然不需要 Docker。

- **hook 现在能看到连接是由哪个监听器接受的。** `hook.ConnInfo` 增加 `Transport`（`"tcp"/"tls"/"ws"/"wss"`，与 `Addrs()` 同名），审计、指标或 ACL 不必再自己保存一份 broker 配置就能区分 TLS 端口与 WebSocket 端口上的设备。监听器名字从 listener spec 穿过 `conn.Client`（`SetTransport`，握手前调用）；自己把连接交给 broker 的嵌入方报告空值。

- **`client.Config.TLS` 会从 `Addr` 补 `ServerName`。** 只给了根证书池、没给 `ServerName` 的配置原先会报 "either ServerName or InsecureSkipVerify must be specified"，而这个要求字段文档里没写；现在客户端按 `tls.Dial` 的方式从 `Addr` 取值，显式 `ServerName` 照旧优先，且不会改动调用方的 `tls.Config`。

- **互操作矩阵覆盖安全与 WebSocket 监听器。** paho 对本 broker 的 TLS/WS/WSS 与 mTLS 各跑一遍同样的发布订阅；参考 broker 方向加了 TLS 监听器（含反例：不信任 CA 的客户端必须失败）。

### 变更

- **示例改成教客户端的分发模型。** `client-demo`、`broker-demo`、`plugin-codec` 把 handler 挂在
  订阅上（`client.Subscription.Handler`），不再用 `Config.OnMessage`——后者现在是"没有路由认领"
  时的兜底，`Client.AddRoute` 填的是同一张表。`client-demo` 另外支持 `-version 3`，让客户端新加的
  MQTT 3.1 路径有可运行的例子。

- **会话与保留消息共用一个持久化接缝。** `Config.Store` 是一个
  [`gcache.RemoteStore`](https://github.com/maxid/gcache)，`Config.Locker` 让它成为多节点共享
  的 store；broker 用同一套键方案与带版本的 JSON 编码写入持久会话（订阅、离线队列、QoS 2 状态、
  过期截止时间）与保留消息（载荷、QoS、属性、Message Expiry），每条记录以自己的截止时间作 TTL。
  实现了 `gcache.Scanner` 的 store 会在启动时被载入（会话索引进路由表，保留消息填进内存热表）；
  过期清扫走路由索引，因此不依赖枚举。默认是 gcache 的进程内内存实现，即原来的内存行为。
  **破坏性变更**：`SessionStore` SPI、`MemorySessionStore`、`RedisSessionStore`、
  `RedisSessionStoreConfig`、`SessionState`/`SessionInflight`/`SessionSubscription`/
  `QueuedMessage`/`InflightStage`、`retained.Store`/`NewStore` 与 `Config.SessionStore` 均删除；
  `contrib/sessionstore-gcache` 模块删除（逻辑进入 `internal/persist`）。原先用
  `RedisSessionStore` 的部署改用 `gcache/redis` 的 store 作为 `Config.Store`、其 locker 作为
  `Config.Locker`；配置共享后端后保留消息即可跨重启。

- **示例统一成一套程序、一种输出。** `examples/broker-demo` 不再用 `net.Dial` 手写 MQTT 3.1.1
  帧：它嵌入 broker、用 `gmqx/client` 驱动，先发一条保留消息，再让第二个 client 后连上来订阅，
  于是演示变成了"保留消息重放"，而不是把客户端库已经做的事再写一遍。四个示例都按
  `[n/N] 动作 … 结果` 每步一行、结尾 `[done] …` 一行，都能用 Ctrl-C 停止（打印
  `[stop] interrupted` 并以 0 退出），都支持 `-h` 给出可直接复制的命令；新增的
  `examples/README.md` 与中文版逐个说明回答什么问题、怎么跑、应该看到什么。

### 修复

- **多监听器 `Start` 失败后不再报告已停掉监听器的地址。** 第二个监听器绑定失败时，`Addrs()` 仍留着已被停掉的那个地址，监管方可能读到一个没有任何进程监听的端口；回滚现在会清空 servers 与地址，与单监听器路径上"失败的 Start 不留残余"的含义一致。

- **`MaxPacketSize` 现在对 WebSocket 监听器生效。** WS/WSS 监听器从未把消息上限传给 gnety，等于静默沿用其 4 MiB 默认值：配置 `MaxPacketSize: 8 MiB` 后，5 MiB 的 PUBLISH 在 TCP/TLS 上正常，在 WS/WSS 上被以 WebSocket 状态码 1009 断开。现在四条传输统一由 `Config.MaxPacketSize` 决定：4 MiB 是库的默认值而不是协议上限（RFC 6455 没有消息大小上限，MQTT 也允许一帧承载多个或部分控制报文），而其它传输本来就只服从这一个数字。上限按整条 WebSocket 消息计，把多个包黏进一条消息的发送方按总和受限。

## 2026-11-02

### 新增

- **后端的内部状态可以被观测，测试不必伸手进包内。** `StatsView`（`Stats() ClusterStats`）用一个快照取代了八个按后端的计数器访问器：丢弃数、去重表大小、转发数、定向 publish 的路由计数，以及一次目标广播的扇出。`LinkView`（`Links() []Link`）报告每一条物理连接，并给出一个替换后永不复用的 `ID`——这才让"链路集合不再变化"可以从外部观测。`PresenceView`（`Presence(clientID)`）报告某个活跃 presence 声明指向哪个节点；`RoutingView`（`Routed()`）报告基于缓存视图（Redis 扇出）时每个后端此刻会怎么处理一个事件，它最多比 `Members()` 滞后一个 `SummaryInterval`。`Member` 增加 `Filters`、`Shared`、`SummaryKnown`，通告的订阅摘要因此成为成员视图的一部分，而不是单独的访问器。本包自己的测试改用这些视图而不是未导出字段与访问器——正因如此，集群后端可以搬去 `gmqx/cluster` 而留在这里的测试照旧工作。

- **扩展可以在 broker 运行时替换，载荷编码是插件的事。** `Broker.Plugins()` 返回 `gplugin.Registry[hook.Host]`；`hook.Host` 是每个槽位一个 setter（`SetAuthManager`、`SetAclManager`、`SetEventSink`、`SetMetricsManager`），加上 `AddInterceptor`/`AddTranscoder`（各自返回移除函数）与 `Codecs()`。hooks 被收进一份不可变快照（`Broker.ext`）并原子替换，所以 publish 路径读到的始终是一个一致的值，而不是另一个 goroutine 正在改写的字段。`hook.Codec`（`MediaType`/`Encode`/`Decode`）、`hook.CodecSet`、`hook.Transcoder` 提供数据协议缝：应用侧插件注册 JSON、CSV 或二进制格式，在载荷出去的路上做转码，而 broker 对 MQTT 保持不透明——不内置任何 codec，没装 transcoder 的 broker 行为与之前完全一致。`plugin_codec_test.go` 是验收用例（JSON→binary、CSV→JSON，以及卸载插件后逐字节直通）。

- **集群后端放在 `gmqx/cluster`。** gossip、TCP 与 Redis 后端、配置类型、`ErrNodeIDInUse` 与 handler 冲突接口都搬了过去；根包以别名重新导出每个类型与常量，并保留构造函数转发（`NewGossipCluster`、`NewTCPCluster`、`NewRedisCluster`、`NewRedisClusterWithSessionStore`），因此两个 import 路径指向同一批类型、消费方零改动。根包的实现文件从 26 降到 21，后端把自己的测试（含 -race）带在身边。

- **`Config.Namespace` 是 `gns.Namespace`。** broker 的隔离域现在是 gns 的 namespace 值：一个进程跑多个 broker（或多个中间件）时，用 `gns.New` 声明一次就把同一个值到处传。broker 为每个键读 `Name`，只对日志与指标读 `Labels`。零值是默认 namespace（裸键、无隔离，与之前一致）；非零值必须通过 `gns.Namespace.Validate`，`New` 会检查，因此非法名字在启动时失败，而不是产生一个读不回来的键。`Config.Cluster.Namespace` 不再被 broker 继承：它现在只作用于独立构建的后端，因为继承会让共用同一 Redis 的两个 broker 意外落进同一个隔离域。gmqx 现在依赖 gns（叶子库，R15）。

- **在线会话状态进了 `gmqx/session`，不再留在引擎里。** `server/session.go` 原来在 store 的
  `SessionState` 旁边又声明了一个未导出类型 `session`，而它所在的包也叫 `session`——这就是
  `docs/design/source-layout.md` §4.2 记下的同名债务。该类型现在是 `session.State`：订阅
  （`session.Subscription`）、离线队列（`session.Pending`）、入站 QoS 2 的 packet id，以及出站
  exchange 的 `Inflight` 投影。`State.Stored` / `StateFromStored` 是带类型的 MQTT 5 属性与 store
  所存字节块之间唯一的转换处，`session/state_test.go` 钉住往返、让保存形态可复现的 packet-id 排序，
  以及 broker 不会盲信非本进程写入的 store 的三件事（不可能是 QoS 1/2 PUBLISH 的 exchange、
  解不出的属性块、未知 stage）。`server/session.go` 只留 broker 侧（take/save/sweep 与离线队列上限），
  存储格式、线上行为与公开 API 都不变。

- **路由表与共享订阅的节点选举进了 `gmqx/routing`。** `routing.Index` 是 broker 的路由表
  （精确/通配桶、每 filter 的活跃计数、带轮转游标的共享订阅组、离线会话条目），且不再持有引擎的
  连接类型：一个活会话只通过 `routing.Endpoint`（client id、connection id、投递句柄）暴露给表，
  这同时也是“被顶替的连接”规则需要的——两个 endpoint 按身份比较。`routing.Target` 是一次匹配的
  返回值；`LocalTarget` 仍是 `Dispatcher` 使用的名字（`server.LocalTarget` 与
  `gmqx.LocalTarget` 是它的别名），`SendLocal` 仍只接受同一个 broker 产出的 target（改用类型上的
  访问器判定）。`routing.Election` 是“哪个节点投递这条共享订阅发布”的集群选择，带 per-group 轮转，
  让同一组的连续发布走遍所有候选；broker 侧只留本地索引、peer 视图、指标与发送这几步薄调用。
  存储格式、线上行为与公开 API 均不变。

- **连接状态机进了 `gmqx/conn`。** 一条物理连接——CONNECT/AUTH 握手、读循环、写队列与 writer、
  出站 QoS 1/2 窗口与重传定时器、以及它留下的 will——原本是 `gmqx/server` 里的未导出 `type client`，
  现在是 `conn.Client`；broker 通过实现 `conn.Host` 为它服务：命名空间、解析后的每连接 `Limits`、
  可换的 metrics/sink/logger 快照、认证入口、register/unregister、publish/subscribe/retained 路径、
  离线入队与延迟 will 调度。会话交接仍是**一个临界区**（`HandOff`、`TakeOver`）：取走状态与“连接不再
  拥有自己的队列”同时发生，broker 在这个回调里完成 store 写入，因此投递不可能落在“会话已保存”的
  连接上。引擎保留待发 will 表与定时器、认证 exchange 工厂与会话簿记；字节速率整形也归 `conn`
  （`conn.Shape`，`ByteRateMetrics` 是 conn 同名接口的别名）。连接拒绝错误与 `MaxAuthSteps`
  随产生它们的代码走并在 server 重新导出，`errors.Is`、错误值与根包公开 API 均不变。

- **引擎变为 `internal/engine`，根包只剩一个 `facade.go`。** 装配 broker 的东西——`conn.Host` 实现、会话簿记、待发 will 表与定时器、插件宿主、hook 别名、会话 store 门面与 auth exchange 工厂——放在一个外部不可 import 的包里；根包只剩一个 `facade.go`（外加 `version.go`），把每个名字以别名重新导出，因此 `gmqx.Broker` == `engine.Broker`，消费方零改动。`gmqx/server`（一个文件名与根门面重名的中间公开包）已删除。公开 API 不变：与上一提交用 `go doc -all` 对比（标识符集合差异 0），并用一个模块外的消费者验证过（实现 `gmqx.Dispatcher`/`gmqx.EventSink`、嵌入 `gmqx.NoopMetrics`、创建 broker）。

- `hook.SinkFuncs`：只实现关心的事件；nil 字段跳过，没写 publish 钩子的 sink 表示接受。

- `hook.InterceptFunc`：一个闭包即可当 `Interceptor`。

- `hook.NoopMetrics`（原根包的 `NoopMetrics`）：嵌入后只覆写自己在意的计数器。

### 变更

- **扩展契约移到 `gmqx/hook`。** `ConnectInfo`、`AuthManager`、`AuthExchange`
  （`AuthStatus`/`AuthStep`）、`AclManager`（`AclAction`/`SessionInfo`）、`EventSink`
  （`PublishEvent`/`PublishResult`/`Disposition`）、`MetricsManager`、`Interceptor`、
  `ConnInfo` 与 MQTT 5 reason code 都定义在该包，根包以类型别名重新导出，因此已有的
  import、名称与类型断言全部照旧可用。契约从 `hooks.go` 里的一节变成单独可评审的目录，
  且 `gmqx/hook` 不 import broker 的任何东西。

## 2026-11-01

### 新增

- **入站 PUBLISH 不再每包分配报文体、主题字符串与载荷**：读循环用 `codec/mqtt` 的 `ReadPublishInto` 解码进每连接一份、只增长的缓冲区（`client.readBuf`，受 broker `MaxPacketSize` 限制），其余报文（PUBACK、SUBSCRIBE、PINGREQ、读错误）仍走原来的类型化解码器。缓冲区会被下一个包复用，因此发布持有载荷的一份复制：`client.handlePublish` 在调用 `Broker.publish` 之前复制一次，这是热路径上唯一的复制位置（retained 存储、offline 队列与集群广播保持各自的复制）。语义不变：相同的校验与 DISCONNECT reason code（超限报文仍为 0x95，因为大小检查仍在读取任何字节之前）、相同的 topic alias 处理、相同的 inflight 与排队行为、sink 看到的 `PublishEvent` 相同且其载荷活过调用。顺序基准实测：`BenchmarkPublishQoS0` 928 B/op、9 allocs/op → 528 B/op、6 allocs/op，`BenchmarkPublishQoS1` 1,667/13 → 1,264/9，`BenchmarkClientPublishQoS1` 3,121/22 → 2,721/18；`ns/op` 与 200 连接的投递速率都落在运行间噪声带内，因为该负载下瓶颈是系统调用而不是解码（见 `docs/PERFORMANCE.md`）。

- **持久会话的 inflight 队列满不再丢弃已确认的 QoS 1/2 投递**：投递进入连接的 pending 队列，session 转为 offline；连接按丢失路径关闭，`unregister` → `SessionStore` 保存完整状态，下一次 CONNECT 继续消费。clean session 仍只有 `inflight_overflow_clean` 一条显式丢弃路径。持久会话的 offline queue 上限（`MaxOfflineQueueSize` 丢最旧、`MaxTotalOfflineMessages` 拒最新）仍是仅有的丢失点，且已有计数。

- 持久化与可用性蓝图（`docs/PERSISTENCE.md`）：重启后必须保留/可以丢失的内容、`SessionStore` 契约（原子 take、按 session expiry 的 per-key TTL、namespace-scoped key、精确往返、损坏值丢弃并计数）、可编译的自定义 store 示例、Redis 蓝图与失败模式、SQL 事务草图、为什么缓存库不能当 session store、共享 store + gossip cluster 的可用性模型，以及 `MaxOfflineQueueSize`/`MaxTotalOfflineMessages` 容量计算。

- `interop/` 互操作矩阵：独立 module，保持库零第三方依赖；第三方 client 对本 broker、本 client 对 docker 参考 broker、参考命令行 client 对本 broker；docker 不可用时干净跳过。

- 基准与 soak 测试（`docs/PERFORMANCE.md`）：QoS 0/1 publish 吞吐、10/100 subscriber fanout、10k 订阅、retained replay、client round trip，以及断言空表、goroutine 有界、delivery ledger 的 churn soak。

- **MQTT client library `github.com/maxid/gmqx/client`**：基于标准库与 `gnety/codec/mqtt` 的完整 MQTT 3.1.1/5 client；QoS 1/2 状态机、DUP/PUBREL 重传、inflight window、重连退避、订阅恢复、keepalive、MQTT 5 属性、有界有序 writer/delivery 队列。

- **MQTT 5 双向流控、跨重连的 QoS 2 状态、字节速率整形与监听器端口回退。**

  *Receive Maximum（双向）。* 客户端在 CONNECT 里带的 Receive Maximum 现在限制 broker 的出站窗口：各 QoS 的有效窗口是 `min(MaxInflightQoS1/2, ReceiveMaximum)`，两个 QoS 共用客户端这条窗口（已用 PUBREC 确认的 PUBLISH 不再占位），进不了窗口的投递在既有的 inflight 队列（`MaxInflightQueueSize`）里等，而不是被丢弃。broker 自己的 receive maximum 是 `Config.ReceiveMaximum`（默认 64，被更小的 `MaxInflightQoS2` 压低），在 CONNACK 里通告并对入站 publish 生效：超过它的 QoS 1 publish 既不投递也不确认（客户端稍后重传），QoS 2 的则回 PUBREC 0x97、根本不开始交换。丢弃以 `receive_maximum_full` / `RecordOverflow("receive_maximum")` 计数；原来的 `inflight_qos2_full` / `inflight_qos2` 名字不再存在。

  *客户端 Maximum Packet Size。* CONNECT 里的 Maximum Packet Size 按连接保存，每个包在写出前都要过一遍。放不下的包被丢弃并计数（`max_packet_size_too_large`）——PUBLISH、retained 重放、以及 SUBACK 这类控制包走同一条路——连接保持可用。broker 自己的入站上限（`MaxPacketSize`）不变，照旧通告。

  *跨重连的 QoS 2。* 客户端以 QoS 2 发布、尚未用 PUBREL 释放的入站 packet id，以及一个会话尚未确认的出站交换，现在属于被存储的会话（`SessionState.InboundQoS2`、`SessionState.Inflight`，配 `SessionInflight`/`InflightStage`）。恢复的会话会抑制重传的入站 PUBLISH，并按原始 packet id 重发仍欠的包——PUBLISH 带 DUP、已确认的 QoS 2 发布发 PUBREL——且排在离线队列之前，符合 §4.4 的要求。会话结束（Clean Start 或过期）时两部分一起丢弃。Redis store 往返这些新字段；旧版本写出的记录解码为“无 QoS 2 状态”。

  *字节速率整形。* `MaxReadByteRate`、`MaxWriteByteRate`、`MaxTotalReadByteRate`、`MaxTotalWriteByteRate`（默认每连接 8 MiB/s 与 16 MiB/s，全局各 256 MiB/s；负值关闭）是一秒突发的令牌桶（至少 16 KiB）。超出部分被延后而不是丢弃或截断：读在字节到达后等、写在字节发出前等，所以突发被 TCP 背压放慢，会话不会因为一次断连被扔掉。被限速的字节通过新的可选 `ByteRateMetrics` 接口上报，并以 `RecordOverflow("byte_rate_read"/"byte_rate_write")` 计数。

  *端口回退。* `Config.PortFallback`（默认 16，负值关闭）让端口已被占用的监听器去试后面的端口，而不是让 `Start` 失败，TCP/TLS/WS/WSS 一视同仁。每次尝试都真正绑定下一个端口（没有 check-then-bind 竞态），绑定结果写日志并由 `Addrs()` 报告，临时 `:0` 绑定永不回退。

## 2026-10-31

### 新增

- **gossip 集群后端成为默认。** `NewGossipCluster` 用 SWIM 成员服务与流行病式传播实现 `Cluster`/`MemberView`，跑在 gnety TCP 上：每 `PingInterval` 一次随机直探、一个怀疑窗口、有界成员增量，每个事件写 `Fanout` 个对端（默认 `min(len(members), 3)`），因此 N 个节点是 O(N·Fanout) 帧而不是 N²。订阅摘要与 client-id presence 作为成员元数据捎带传播，这正是 `PublishToClient` 能只写给持有该 client id 的那个节点的原因。`Config.Cluster.Enabled` 不指定后端时用 `ClusterConfig.Addr`/`Seeds`/`PingInterval`/`PingTimeout`/`SuspicionMultiplier`/`Fanout` 构建它；只设了 `Enabled` 的部署现在必须设 `Addr`，`Cluster.Node` 与 `Cluster.Namespace` 从被忽略变为生效，从未被读取的 `Cluster.URL`/`Cluster.Port`/`Cluster.ClusterMessageBufferSize` 已删除。

- **MQTT 5 增强认证可用。** `Config.Authenticator` 按连接构建 `AuthExchange`（`Method`/`Begin`/`Continue`），`Config.AuthMethods` 是通告集合，`AuthStep` 报告继续（AUTH 0x18）、成功（CONNACK 或 AUTH 0x00，带方法与其数据）或失败。成功步骤的 `ConnectInfo` 对 `Username`/`ClientID` 是权威的，并跳过 `Auth` hook（ACL 仍生效）；AUTH 0x19 在活动连接上重认证且不动会话。指定不支持方法的 CONNECT 现在以 CONNACK 0x8C 拒绝（此前是接受），没有方法的 Authentication Data 是 0x82，交换受 `MaxAuthSteps`（8）、`ConnectTimeout` 与 `AuthTimeout` 约束。

- **共享订阅与 subscription identifier 可用，CONNACK 把两者都通告为可用。** 畸形的 `$share/{name}/{filter}` 按 filter 以 SUBACK 0x8F 拒绝；命中某个组的发布只交给一个存活成员（轮转），不重放 retained，离线的持久成员也不排队。跨集群时产生发布的节点为每组选出一个节点（在排序后的持有者上做 `fnv64(groupKey)` 加每组一个座位），并在新的 `ClusterMessage.Shared` 里指名它，因此一个组全集群只投递一次；两个后端都在摘要里通告组键。带属性 0x0B 的 SUBSCRIBE 被接受，标识符存进 `SessionSubscription.SubscriptionIdentifiers` 并附加到该订阅导致的每条 PUBLISH 上（合并、去重、每次投递最多 8 个），值为 0 是协议错误（DISCONNECT 0x82）。两个 not supported 的 reason code 已删除。

- **kick 带 epoch，持久会话可以放在共享 store 里。** `ClusterKick` 现在携带 `ConnectTime`，`OnRemoteKick` 只对严格更老的连接执行 kick（相同 epoch 由 node id 裁决），延迟的 kick 因此不能关掉已经顶替它的连接；`NotKickSeconds` 在接收侧也生效，旧对端不带 `ConnectTime` 的 kick 无条件应用并以 `cluster_kick_legacy` 计数，被拒的是 `cluster_kick_stale`/`cluster_kick_rate_limit`；跨节点排序假定时钟经 NTP 同步。本条引入的 Redis session store 后来并入 2026-11-03 的统一持久化接缝。

- **订阅索引取代逐发布扫描，发布可以定向到某个 client id。** 字面 filter、通配 filter 与每个 (filter, session) 一个条目组成路由表，发布不再遍历每条连接与每个会话，热路径不取全局锁、也不碰 session store；语义（QoS 上限、no-local、retain-as-published、`$` 排除、离线目标）不变，`BenchmarkLocalTargets` 从 10 到 10 000 个订阅是平的。`Broker.PublishToClient(ctx, clientID, Publication)` 投给本地存活连接、本地离线会话（排队）或持有该 id 的节点，走新的 `ClusterTarget` 事件；Redis 用 `PresenceTTL` 保存 presence 记录，TCP mesh 广播该事件并只在实际持有该 id 的节点应用，定向发布是 at-most-once。

- **CONNECT 与鉴权截止时间、能力拒绝、延迟 will、retained 过期与跨节点接管。** `Config.ConnectTimeout`（10 s）约束 CONNECT 阶段，`Config.AuthTimeout`（5 s）约束 `Authenticate`/`ACL.Check`，超时按拒绝处理（`auth_timeout`/`acl_timeout`）而不是成功。CONNACK 通告为不可用的能力改为拒绝而不是存储：`$share/` 订阅回 SUBACK 0x9E，subscription identifier 回 DISCONNECT 0x9A。will 延迟是 `min(delay, MaxWillDelayInterval, 剩余 Session Expiry)`，恢复会话会取消它；retained 消息保存发布时间与 Message Expiry Interval，并按剩余时间重放。每次 CONNECT 现在在两种 connect mode 下都传播 kick，被顶替的连接收到 DISCONNECT 0x8E，而 `NotKickSeconds`（30 s）窗口内的接管以 CONNACK 0x9F 拒绝。

- **inflight 窗口满不再丢 QoS 1/2 投递，卡住的 socket 也不再阻塞 broker。** 发现窗口满的投递为该连接排队（`Config.MaxInflightQueueSize`，256），在某个确认腾出位置时发出；只有发现队列也满的投递才被丢弃（`inflight_overflow` 加 `inflight` 溢出计数），取代之前的静默 `inflight_full` 跳过。每个包现在由一个 writer goroutine 从受 `MaxOutboundQueueSize`（256）约束的队列里写出，每包受 `WriteTimeout`（5 s）约束，因此停止读取的对端不能卡住发布者的读循环或节点唯一的集群接收 goroutine：队列满时丢弃 QoS 0 PUBLISH（`outbound_queue_full`），对 QoS 1/2 则断开慢消费者（`slow_consumer`）。

- **持久会话存储、有界离线队列与 broker 侧 QoS 1/2 重传。** `SessionStore`（`Load`/`Save`/`Delete`/`Keys`）持有没有存活连接的会话，有界清扫器（`SessionSweepInterval`，30 s）执行 Session Expiry，过期会话不再被恢复。持久会话按 FIFO 顺序保存其订阅命中的消息（`MaxOfflineQueueSize` 1000 丢最旧，`MaxTotalOfflineMessages` 100000 拒最新），并在下一次 CONNACK 后排空；未确认的 QoS 1/2 投递存在每连接的 inflight 表里，由 `QoSRetryInterval`（5 s，翻倍）重试到 `QoSMaxRetries`（5）并带 DUP，受 `MaxInflightQoS1`/`MaxInflightQoS2`（各 64）约束。本条与 kick 条目引入的存储后来都并入 2026-11-03 的统一持久化接缝。

- **Dispatcher SPI、连接上下文与 namespace 隔离。** `Config.Dispatcher` 是已接受发布的唯一决策点（nil 选 `DefaultDispatcher`），自定义 dispatcher 通过只读的 `LocalTargets`/`SendLocal`/`SpreadPublication` 缝路由，而不必重新实现 topic 匹配。每个 hook 都跑在带 `ConnInfo` 的连接作用域 context 上，它在 CONNECT 时由 `Config.ConnContext` 富化一次，并随连接取消。`Config.Namespace` 划分会话表、订阅匹配、retained store 与通告的摘要，两个后端都会丢弃其它 namespace 的事件。

- **协议一致性修复（MQTT 3.1.1 / 5）。** 一套对着真实 broker 的线上格式测试暴露并修好了这些：持久 CONNECT 恢复订阅并回答 Session Present；will 保留其 QoS、retain 标志与属性，非法 will topic 拒绝 CONNECT；投递使用 `min(publication QoS, subscription QoS)`；重传的 QoS 2 PUBLISH 只投递一次；PUBLISH topic 必须非空且不含通配符，开头的 `#`/`+` 不再匹配 `$` 主题；协议错误以匹配的 DISCONNECT reason code 结束 MQTT 5 连接（0x81、0x82、0x90、0x94、0x95）。

- **集群：retained 复制与 node id 归属。** 从对端收到的 retained 发布现在按本地规则应用到接收节点的 retained store（空载荷删除，`MaxRetainMessages` 约束），而不是只当作实时消息投递，因此之后连到别的节点的订阅者能拿到 retained 值。Redis 后端强制每个 node id 只有一个存活持有者：`Start` 用 `SET NX` 与 `NodeTTL` 取得 `<ChannelPrefix>:owner:<nodeID>`、续租并在 `Stop` 释放，同 id 的第二个进程以新的 `ErrNodeIDInUse` 失败，而不是共用频道。

- **Redis 集群后端。** `NewRedisCluster` 用 Redis 取代 TCP mesh：每个节点一个私有事件频道（`<ChannelPrefix>:node:<nodeID>`）、以成员键 TTL（`NodeTTL`）表成员、每个节点一份订阅摘要（`SummaryTTL`）、每条发布对每个摘要匹配的节点一次 `PUBLISH`，因此 N 个节点是 O(N) 次操作而不是 O(N²) 条链路。它复用 TCP 后端的 Origin/Seq 去重、环路保护与丢弃原因，`Members()` 按 node id 返回对端。Redis 不可达时本地产生的 spread 返回新的 `ErrClusterUnavailable`，接收循环以有界退避重新订阅；`Stop` 幂等。依赖新增 `github.com/redis/go-redis/v9`（测试用 `github.com/alicebob/miniredis/v2`）。

## 2026-10-30

### 新增

- **内置 TCP 集群：成员、去重与定向路由。** 每条链路以带版本的握手开始（node id、通告地址、incarnation、成员视图），因此一个 node id 恰好一个逻辑成员，版本不匹配在握手处被拒绝；心跳在 `HeartbeatMissBudget` 个间隔后驱逐沉默成员，`Members()`（新的可选 `MemberView`）暴露该视图。`ClusterMessage` 增加 `Origin`/`Seq`、有界 TTL 去重表、收到的事件不转发与源节点环路保护。对端通告其会话订阅的 filter（每 `SummaryInterval` 一次、订阅变化时推送、受 `MaxSummaryFilters` 约束），发布只发给摘要匹配的对端，而 retained 发布与 kick 仍广播。线格式带版本，与之前的裸 JSON 帧不兼容。

- **报文改走类型化 codec。** `mqtt.Decoder` 与 `mqtt.Encode` 读写每个包，连接循环按消息类型分派，因此 `packet.go` 与 `properties.go` 已删除（新的 `props.go` 只负责把 MQTT 5 属性转给 sink 与集群帧）。线上可见差异都是规范正确的：CONNACK 不再通告非法的 `Maximum QoS 2`、v5 UNSUBACK 每个 filter 一个 reason code、缺席的 v5 默认值被省略。MQTT 3.1 通过 CONNECT 改写继续工作。

- **H1：带结果的 publish。** `EventSink.OnPublish` 收到 `PublishEvent` 并返回 `PublishAccepted`/`PublishRetryable`/`PublishRejected`；PUBACK/PUBREC 只在结果为接受之后发送，因此被拒绝的发布绝不会被乐观确认（MQTT 3.1.1 没有否定确认），`Config.PublishCallbackTimeout` 约束 sink 等待，retained 状态只为被接受的发布写入。

- Publisher identity：`ConnectionID` 是进程内唯一的物理连接 id，与跨重连存活的 client id 区分开。

- MQTT 3.1（`MQIsdp`，协议级别 3）CONNECT 兼容；v3.1 的 client-id 规则。

## 2026-10-29

### 新增

- **单节点 MQTT 3.1.1 broker。** TCP listener、CONNECT/SUBSCRIBE/PUBLISH/PING/DISCONNECT、
  QoS 0/1/2、retained、wildcard 订阅、will 与 keepalive。

- **扩展 SPI 与连接治理。** `AuthManager`、`AclManager`、`EventSink`、`MetricsManager`、
  `Interceptor` 是扩展缝；`ConnectMode`（`UNIQUE`/`KICK`，两者都有测试）、连接/订阅/retained
  限额与连接速率限制是边界；metrics counter 与生命周期回调（connect/disconnect/lost/ping）负责上报。

- **四个监听器：TCP、TLS（服务端证书，可选客户端证书）、WebSocket、WSS。** WebSocket 监听器校验
  `mqtt` 子协议、path 取自 `Config.WebsocketPath`，`Addrs()` 报告每个监听器真正绑定的地址，
  TLS/WebSocket 集成测试覆盖这些传输。

- **MQTT 5。** 带 properties 的 CONNECT、v5 CONNACK/SUBACK/UNSUBACK 格式、PUBLISH property block
  （入站与 per-session 的 topic alias，外加转发 User Property、content type、response topic、
  correlation data）、CONNACK capability 属性、订阅选项（no-local、retain-as-published、
  retain-handling）与负向 ACK reason code（Not authorized、Quota exceeded、Client Identifier not
  valid），并有 v5 connect/pub-sub/auth 测试。

- **集群缝。** `Cluster`/`ClusterMessage` 与跨节点 publish/kick 的 broker hook，配一个内存双节点测试。

### 变更

- README/docs 产品中性、自包含。
- **传输来自 gnety。** TCP/TLS/WS/WSS 监听器是 gnety 的 `RawHandler`；MQTT codec 与 broker 状态机
  仍留在 gmqx。

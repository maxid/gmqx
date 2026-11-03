[English](CHANGELOG.md) | 简体中文

# 变更日志

> 开发快照不打 tag。条目按变更落地日期分组（新到旧），每个日期只记当天**最终状态**：被后续步骤取代的中间步骤并入描述最终形态的那一条，不按步骤各记一条。中文版与英文版逐条对应（同日期、同条目、同顺序），代码标识符与协议名保持原文。

## 2026-11-03

### 新增

- **hook 现在能看到连接是由哪个监听器接受的。** `hook.ConnInfo` 增加 `Transport`（`"tcp"/"tls"/"ws"/"wss"`，与 `Addrs()` 同名），审计、指标或 ACL 不必再自己保存一份 broker 配置就能区分 TLS 端口与 WebSocket 端口上的设备。监听器名字从 listener spec 穿过 `conn.Client`（`SetTransport`，握手前调用）；自己把连接交给 broker 的嵌入方报告空值。

- **`client.Config.TLS` 会从 `Addr` 补 `ServerName`。** 只给了根证书池、没给 `ServerName` 的配置原先会报 "either ServerName or InsecureSkipVerify must be specified"，而这个要求字段文档里没写；现在客户端按 `tls.Dial` 的方式从 `Addr` 取值，显式 `ServerName` 照旧优先，且不会改动调用方的 `tls.Config`。

- **互操作矩阵覆盖安全与 WebSocket 监听器。** paho 对本 broker 的 TLS/WS/WSS 与 mTLS 各跑一遍同样的发布订阅；参考 broker 方向加了 TLS 监听器（含反例：不信任 CA 的客户端必须失败）。

### 变更

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

- **gossip 集群后端成为默认；`Config.Cluster.Enabled` 不指定后端时也会做事。**

  *后端。* `NewGossipCluster`（与 `GossipClusterConfig`）在 gnety 的长度前缀 TCP 传输上实现 SWIM 风格的成员服务与流行病式事件传播，满足既有的 `Cluster`/`ClusterHandler`/`MemberView` 契约。成员：每个进程一个随机 incarnation，每 `PingInterval` 一次随机直探，`PingTimeout` 后最多三次间接探测，怀疑窗口为 `SuspicionMultiplier × PingInterval`，用 incarnation 号反驳误判，每次 ping/ack 捎带有界成员增量，死成员在 `DeadTimeout` 后移除。播种只需要一个可达地址：其余视图经增量到达。

  *传播。* publish、kick、retained 更新与定向 publish 最多写 `Fanout` 个对端（默认 `min(len(members), 3)`），每个收到者再转发给不是从它那里收到的对端；既有的 `(Origin, Seq)` 去重表终结传播，因此 N 个节点的集群每个事件是 O(N·Fanout) 帧而不是 N²。N ≤ Fanout 时源节点写给每个对端、没有需要转发的，这正是小集群想要的全广播。订阅摘要（filter 与共享组键，同一套 `summarySet`）作为成员元数据传播，并仍然指导普通 publish；未知或过期的摘要被并入传播而不是跳过。client-id presence 同样以 gossip 传播（Lamport 版本、进程 incarnation、TTL、按 namespace 划分），因此 `PublishToClient` 只写给持有该 client id 的那一个节点，只有在该声明缺失、过期或死亡时才回落到传播。

  *边界。* 一帧最多带 `MaxGossipDelta` 条成员记录与 `MaxPresenceDelta` 条 presence 声明；presence 表、去重表、探测表、拨号队列与每链路帧队列都有界；扇出有界；每条链路一个写 goroutine，绝不按消息开。`Members()` 报告新的 `Member.State`（`alive`/`suspect`/`dead`），按 node id 排序。

  *默认选择与配置。* `Config.Cluster.Enabled` 且没有 `Config.ClusterBackend` 时，用 `ClusterConfig.Addr`、`Seeds`、`PingInterval`、`PingTimeout`、`SuspicionMultiplier`、`Fanout` 创建 gossip 后端；显式后端仍然优先，`NewTCPCluster` 与 `NewRedisCluster` 照旧可用。`Cluster.Node`/`Cluster.Namespace` 现在在为空时给 `Config.NodeID`/`Config.Namespace` 播种。没有 seed 的节点独自启动并记录这一点。

  *删除的配置*（它们从未被读取）：`Config.Cluster.URL`、`Config.Cluster.Port`、`Config.Cluster.ClusterMessageBufferSize`。`Config.Cluster.PingTimeout` 与 `Config.Cluster.SuspicionMultiplier` 同样是死字段，现在接到 gossip 后端上，并给它补了 `Cluster.Addr`、`Seeds`、`PingInterval`、`Fanout`。

  *对既有用户的行为变化。* 设了 `Cluster.Enabled` 但没有后端的部署现在真的跑一个集群后端（因此必须设 `Cluster.Addr`）；设了 `Cluster.Node` 或 `Cluster.Namespace` 的部署现在会生效而不是被忽略。显式安装 `NewTCPCluster`/`NewRedisCluster` 的部署无变化。设过 `Cluster.URL`、`Cluster.Port`、`Cluster.ClusterMessageBufferSize` 的代码不再编译，因为这些字段被删除了。

- **MQTT 5 增强认证（AUTH）可以用了；broker 不再对每个 AUTH 都回 DISCONNECT 0x82。**

  *SPI。* 新 hook `AuthExchange`（`Method`、`Begin`、`Continue`）由 `Config.Authenticator func(method string) AuthExchange` 按连接构建，因此一个 broker 可以支持多种方法；`Config.AuthMethods` 是通告集合，CONNECT 会据此校验，工厂为 `nil` 时保持旧行为（无方法、AUTH 即协议错误）。`AuthStep` 报告 `AuthContinue`（broker 发 AUTH 0x18 带该步数据，并用客户端的回答调 `Continue`）、`AuthSucceeded`（CONNACK 0x00——活动连接上是 AUTH 0x00——带上方法与最终数据，外加可选的 `ConnectInfo` 携带已认证身份）或 `AuthFailed`（用该步的 reason code 发 CONNACK/DISCONNECT，默认 0x87）。`ConnectInfo` 增加 `AuthMethod` 与 `AuthData`，CONNECT 的认证属性由此到达 `Begin`。

  *握手。* CONNECT 指定了 broker 没有对应交换的方法时，以 CONNACK 0x8C（Bad authentication method）拒绝并关闭；交换的第一步可以立即成功（CONNACK 0x00 带方法与其数据）或继续（AUTH 0x18，每个挑战一次，客户端以 AUTH 0x18 回答并喂给 `Continue`）。CONNACK 的 Authentication Method 就是客户端实际用的方法（§3.2.2.3.8 的要求）；不带方法却带 Authentication Data 的 CONNECT 是协议错误（CONNACK 0x82）。

  *身份。* 成功步骤的 `ConnectInfo` 是权威的：其中非空的 `Username` 与 `ClientID` 取代 CONNECT 里的值，会话按它们注册，hooks 看到的就是它们。由于交换本身就是认证，这类连接跳过 `Auth`（`AuthManager`）hook——`DenyAll` 不会挡住一个已增强认证的客户端——而 `AclManager` 仍对它每次 publish/subscribe 授权。

  *重认证。* 活动连接上的 AUTH 0x19 在连接上开一次新交换（从工厂取新交换、再走 `Begin`），成功回 AUTH 0x00，失败按该步 reason code 发 DISCONNECT；会话（订阅、离线队列、inflight 交换）不受影响，`Username` 可以更新。重认证指定别的方法，或不带重认证标志的 AUTH，都是协议错误（DISCONNECT 0x82）。

  *边界。* 一次交换最多 `MaxAuthSteps`（8）个挑战，超了以 0x87 拒绝（`RecordDroppedMessage("auth_steps")`）。整个 CONNECT 交换处在 `Config.ConnectTimeout` 之内（从不回应挑战的客户端到点断开），每次 `Begin`/`Continue` 受 `Config.AuthTimeout` 约束，活动连接上同一超时是整个重认证的读截止时间。交换跑在连接自己的 goroutine 上，因此 AUTH 绝不会与同一客户端的其它包并发处理。

  对既有部署的行为变化：

  - broker 没有对应交换却指定了 Authentication Method 的 CONNECT 现在以 CONNACK 0x8C 拒绝；此前是接受（CONNACK 不带方法），随后的 AUTH 以 0x82 回答。
  - 没有 Authentication Method 的 Authentication Data 现在是 CONNACK 0x82，而不是被忽略的属性。
  - 未协商方法的连接行为与之前完全一致：CONNACK 不带 Authentication Method，AUTH 回 DISCONNECT 0x82。
  - 新增 `RecordDroppedMessage` 原因：`auth_method`（CONNECT 指名了不支持的方法）与 `auth_steps`（交换超过 `MaxAuthSteps`）；新增导出的哨兵错误 `ErrBadAuthMethod` 与常量 `ReasonBadAuthMethod`（0x8C）命名这次拒绝。

- **共享订阅与 subscription identifier 可用；CONNACK 把两者都通告为可用。**

  *共享订阅。* `$share/{ShareName}/{filter}` 按 MQTT 5 §4.8.2 校验（ShareName 非空且不含 `/`、`+`、`#`，其后是合法 topic filter），畸形时**按 filter** 以 SUBACK 0x8F 拒绝；仅仅以 `$share` 开头的 filter（`$share` 本身）照旧是普通字面 filter。命中某个组的发布只交给该组的一个存活成员，按轮转选择；同一 filter 上的普通订阅者仍各得一份；带 No Local 的共享订阅以 0x8F 拒绝（MQTT-3.8.3-4）；共享订阅不重放 retained（§4.8.2）；离线的持久成员收不到——共享状态从不排队（订阅本身留在存储的会话里，恢复时重新入组）；组的轮转状态随最后一个成员一起丢弃。

  跨集群时，产生发布的节点为每个组选出恰好一个节点——在通告该组存活成员的排序节点上做 `fnv64(groupKey) + 每组一个座位`——并在新的 `ClusterMessage.Shared`（`SharedDelivery`）里指名它。被选的节点在本地挑成员。两个内置后端都在订阅摘要里通告组键（TCP 摘要帧里 `Filters` 旁的新 `Shared` 字段，以及 `redisSummaryRecord`/`redisPeer`），并把发布路由到 `Shared` 里指名的对端，不看它的 topic 摘要。一条发布全集群每个组最多投递一次；被选节点若已失去最后一个成员，就丢弃并计数 `RecordDroppedMessage("shared_no_member")`，而不是交给别的节点的成员。本包之外的 `Cluster` 后端不通告组，因此那里的共享订阅只留在发布节点本地；旧版本的对端忽略 `Shared`、跨节点不投共享副本——绝不会重复。

  *Subscription Identifier。* 带属性 0x0B 的 SUBSCRIBE 被接受（DISCONNECT 0x9A 不再出现）。标识符随订阅存储，经 `SessionStore` 写进新的 `SessionSubscription.SubscriptionIdentifiers`（内存与 Redis 记录都是增量字段），恢复时还原，并附加到该订阅导致的每个 PUBLISH 上：在线、retained 重放、离线队列排空与共享组投递。命中同一会话的多个订阅的标识符合并进一条 PUBLISH、去重，并受 `maxSubscriptionIdentifiersPerDelivery` 限制（8 个，另外每个订阅存 8 个）；不得不省略的以 `RecordDroppedMessage("subscription_identifier_limit")` 计数。值为 0 是协议错误：连接以 DISCONNECT 0x82 关闭（gnety codec 在解析时拒绝该值，broker 只映射这一种拒绝；其它不可解析的包仍是 0x81）。该属性保持逐跳：客户端 PUBLISH 携带它仍会被剥掉，投递标识符由写出 PUBLISH 的节点附加。

  对既有部署的行为变化：

  - CONNACK 现在通告 `SubscriptionIdentifierAvailable = true` 与 `SharedSubscriptionAvailable = true`，`$share/...` 的 SUBSCRIBE 或 Subscription Identifier 不再得到 SUBACK 0x9E / DISCONNECT 0x9A。命名这些拒绝的两个导出 reason code 常量（`ReasonSharedSubscriptionsNotSupported`、`ReasonSubscriptionIdentifiersNotSupported`）已删除。
  - `SessionSubscription` 增加了一个字段。旧版本写出的 store 解码为“无标识符”，正是那些订阅的情形，因此混合版本集群双向都还能工作。
  - `Broker.SpreadPublication` 现在也执行该发布的共享订阅路由（本节点被选中的组的本地投递），因此先做本地扇出再 spread 的自定义 `Dispatcher` 无需新调用就保持正确的共享行为。
  - `ClusterMessage` 增加了增量字段 `Shared`；TCP 摘要帧与 Redis 摘要记录增加了 `shared` 列表。旧对端忽略两者。

- **kick 带 epoch，Redis store 让集群节点共享持久会话。**

  *kick epoch。* `ClusterMessage` 在 `ClusterKick` 事件上携带 `ConnectTime`——产生这次 kick 的连接建立时间。每次成功 CONNECT 都会设置它，`Broker.OnRemoteKick` 只对严格更老的连接执行 kick，因此延迟或乱序的 kick 不能再关掉一个比它本要替换的更年轻的连接。相同 epoch 由 node id 确定性地裁决（node id 更大的那侧连接胜出），所以一个 client id 的两端会得出同一个结论，而不是都关掉。被拒的 kick 用新的 `RecordDroppedMessage` 原因计数：`cluster_kick_stale`（输掉 epoch 比较）与 `cluster_kick_rate_limit`（在位者仍在 `Config.NotKickSeconds` 防抖窗口内），并以 Debug 级别记录；真正顶替的 kick 仍发 DISCONNECT 0x8E 并取消待发的延迟 will。

  对既有部署的行为说明：

  - `Config.NotKickSeconds` 现在在接收侧也生效：对端 kick 到达时，若它指名的连接比窗口更年轻，就拒绝而不是关闭它。客户端在节点间迁移比窗口更快的集群（这正是窗口的用途）因此会保留旧连接直到窗口过去；把 `NotKickSeconds` 设为负值可保留之前无窗口的跨节点行为。
  - 没有 `ConnectTime` 的 kick（来自此改动之前的对端）无条件执行，即那个对端自身的行为，并以 `cluster_kick_legacy` 计数。混合集群因此降级为旧语义，而不是拒绝所有 kick，线上格式双向兼容（字段是增量；旧对端忽略它）。
  - 跨节点排序假定在防抖窗口内时钟经 NTP 同步；epoch 是一次墙钟读数，因为单调时钟在另一个进程里没有意义。单节点内不跨时钟（`time.Since`）。

  *共享会话。* `NewRedisSessionStore` 是本包提供的第二个 `SessionStore`：它把每个存储的会话放在集群 Redis 的 `<prefix>:session:<namespaceKey(ns, clientID)>` 下，以会话自己的截止时间作为键的 TTL（无截止时间的会话无 TTL，截止时间已过的状态干脆不写）。`NewRedisClusterWithSessionStore(RedisClusterConfig)` 用一份配置同时构建 Redis 集群后端与该 store。解不出的值被移除并计数（`session_store_corrupt`）；失败的 Redis 命令以 `session_store_error` 计数并降级为“无会话”。

  该 store 满足新的可选 `SharedSessionStore` 接口，以及 broker 用来原子地交出存储会话的可选 `Take` 方法，因此两个节点重连同一 client id 不会都恢复它。被对端 kick 顶替的连接现在不动共享 store（既不写回自己那份，也不删键），于是接管会话的节点拥有它；用默认内存 store 时无变化。`MemorySessionStore` 仍是默认，store 缝其余不变（`Keys`、`Load`、`Save`、`Delete`），既有部署行为完全一致。与节点本地 store 一样，store 搬状态不搬路由：节点路由索引里没有该存储会话时，发布不会为它排队。

- **订阅索引取代逐发布扫描，发布可以定向到某个 client id（本地或跨集群）。**

  *路由索引。* `LocalTargets` 不再为每条发布遍历所有存活连接与存储会话。broker 维护一张路由表（`routing_index.go`）：字面 filter 放在以 filter 为键的 map 里（每条发布一次查找），通配 filter 放在以 filter 为键的 map 里（每个不同的通配 filter 一次 `matchTopic`，而不是每个订阅一次），每个 (filter, session) 一个条目，因此一个会话的重叠 filter 合并成一个目标，取最高的匹配 QoS 与 retain-as-published 的或。这条路径完全不取 broker 全局锁，也不调用 `SessionStore`：会话在保存时进索引，在被接管、被 clean CONNECT 丢弃或过期时移除。`New` 会给 store 里已有的会话建索引，因此在 broker 启动前恢复会话状态的部署仍能路由到它们。`matchTopic` 不再把 filter 与 topic 切成分片，路径上最后一个逐通配分配也随之消失。语义不变：`min(publication, subscription)` 的 QoS 上限、no-local、retain-as-published、通配 filter 对 `$` 主题的排除、同 client id 接管，以及离线目标的存在（`LocalTarget.Offline()`）。保证：订阅在 SUBACK 写出前即可路由，退订在 UNSUBACK 前即不可路由，恢复会话的 filter 在其 CONNACK 前即可路由，一个会话绝不会同时是存活目标与存储目标，因此发布只到达一个会话一次。`BenchmarkLocalTargets` 显示 10 到 10 000 个订阅下成本是平的。

  对既有部署的行为说明：路由不再调用 `SessionStore.Keys`（它仍是启动索引来源与清扫器的枚举来源），外部进程在 broker 运行时写 store 不会被重读——这样的写入方必须像以前一样在 `New` 之前预热 store。

  *定向发布。* 新的 `Broker.PublishToClient(ctx, clientID, Publication)` 把一条发布投给一个 client id：本地存活连接（经 `SendLocal`，按发布的 QoS 并遵守连接的 inflight 窗口）、本地持久会话但无连接（排队，在下一次 CONNACK 后按既有离线队列上限排空），或集群。新的 `ClusterTarget` 事件携带 client id 与发布，按 namespace 划分、像其它事件一样以 `(Origin, Seq)` 去重，并由实现了可选 `targetHandler` 接口的 `ClusterHandler` 应用——broker 就是。Redis 后端在 `<prefix>:client:<key>` 下保存 `clientId -> node` 的 presence 记录，配新的 `RedisClusterConfig.PresenceTTL`（默认 3×`SummaryInterval`），在 broker 报告 CONNECT 时写入，会话仍存储在该节点期间保留，会话真正结束或迁移到别的节点时以 compare-and-delete 删除（对端的按连接时间 kick 即使持久会话仍被存储也会释放声明，因为队列是节点本地的），因此旧连接的断开不会抹掉已经迁移的客户端的记录。目标事件只写到那个节点的频道；记录缺失意味着没有节点声明该 client id，事件被计数（`RecordDroppedMessage("target_unknown")`）并丢弃，绝不广播——定向发布是 at-most-once，一次比 presence 刷新更早的发布竞争会导致丢失。TCP mesh 没有 presence：它像 kick 一样把目标事件广播给每个对端，每个对端只在自己持有该 client id 时（或为其离线持久会话排队时）应用它，否则计数。kick 刻意保持广播：过期的 presence 记录不能留下一个僵尸连接。`PublishToClient` 只在调用方出错（`ErrClientIDRequired`、`ErrNamespaceMismatch`、非法 topic 或 QoS）或集群后端拒绝事件时返回错误；定向发布不经过 ACL、拦截器、`EventSink` 或 retained store。

- **CONNECT 阶段与鉴权截止时间、能力拒绝、延迟 will、retained 过期与跨节点接管。** 一批对齐与安全修复。

  *CONNECT 与鉴权截止时间（安全）。* 连接现在在读截止时间下跑 CONNECT 阶段：`Config.ConnectTimeout`（默认 10 s）在读取第一个包之前设置、CONNACK 之后清除，随后读循环设置自己的 keep-alive 截止时间。打开 socket 却什么都不发——或 CONNECT 发得太慢——的客户端会被断开，因此它不能再永远占住一个连接槽、一个 goroutine 和一个会话。`Config.AuthTimeout`（默认 5 s）约束 `AuthManager.Authenticate` 与 `AclManager.Check`：hook 在自己的 goroutine 里跑，context 带截止时间，超时的调用按拒绝处理——认证回 CONNACK 0x87，鉴权回 SUBACK 0x87 或不给成功 PUBACK——绝不判成功（`RecordDroppedMessage("auth_timeout")`、`"acl_timeout"`）。内置 `AllowAll` hooks 走同步路径，因此默认发布路径不额外起 goroutine；负的 `AuthTimeout` 关闭这个界限。

  *CONNACK 通告为不可用的能力会被拒绝。* 以 `$share/` 开头的 SUBSCRIBE filter 不再被存成永远匹配不到的字面 filter：在 MQTT 5 上按 filter 回 SUBACK 0x9E（Shared Subscriptions not supported，§3.9 要求），在 MQTT 3.1.1 上回 0x80（`RecordDroppedMessage("shared_subscription")`）。带 Subscription Identifier 的 SUBSCRIBE 在 MQTT 5 上回 DISCONNECT 0x9A（Subscription Identifiers not supported），在 3.1.1 上关闭连接（`RecordDroppedMessage("subscription_identifier")`）。对 `props.go` 的审计确认这是它通告为不可用的仅有两项；Retain Available、Wildcard Subscription Available 与 Topic Alias Maximum（16）都通告为可用且已实现。reason code 现在导出为 `ReasonSessionTakenOver`、`ReasonSubscriptionIdentifiersNotSupported`、`ReasonSharedSubscriptionsNotSupported` 与 `ReasonConnectionRateExceeded`。

  *Will Delay Interval（MQTT 5）。* 带延迟的 will 在延迟到期或会话结束时发布，以先到者为准：随连接结束的会话（Clean Start、Session Expiry 为零）立即发布它的 will；否则等待 `min(delay, Config.MaxWillDelayInterval, 剩余 Session Expiry)`；恢复会话的 CONNECT 取消待发 will（本地或通过连接时的集群 kick），而带 Clean Start 的 CONNECT 丢弃会话并立刻发布该 will；`Stop` 丢弃仍在待发的。新的 `Config.MaxWillDelayInterval`（默认 5 分钟，负值表示无界）截断更长的请求并以 `RecordOverflow("will_delay")` 计数。调度是每个待发 will 一个定时器。

  *retained 消息过期（MQTT 5）。* retained 发布现在保存发布时间与 Message Expiry Interval。重放时通告剩余区间（向上取整，因为 0 在线上表示“永不过期”）；已过期的消息不重放并从 store 删除（`RecordDroppedMessage("retained_expired")`）。没有区间的 retained 消息仍然永不过期。

  *跨节点接管。* 每次成功 CONNECT 现在都在两种 connect mode 下传播 kick，而不再只在 `KICK` 下：否则重连到别的节点的客户端会留下一个僵尸连接，在 keep-alive 到期前把每条投递都复制一遍。被顶替的连接——本地或由对端 kick——在关闭前于 MQTT 5 上收到 reason code 0x8E（Session taken over）的服务端 DISCONNECT，因此客户端能区分接管与网络掉线，且该 client id 待发的延迟 will 被取消。

  *`NotKickSeconds` 现在生效。* 该字段（默认 30 s）是接管的防抖窗口：`KICK` 模式下，会顶替一个比窗口更年轻的连接的 CONNECT 在 MQTT 5 上以 CONNACK 0x9F（Connection rate exceeded，`RecordDroppedMessage("takeover_rate_limit")`）拒绝、在 3.1.1 上以返回码 3（Server unavailable）拒绝，在位会话原样保留。0x9F 是 MQTT 5 为限速连接保留的 reason code；0x8A 会声称管理封禁、0x89 会声称整机过载。

  对既有部署的行为说明：`KICK` 模式不再顶替一个开始时间不足 `NotKickSeconds` 的连接——设负值可恢复之前的立即接管；`UNIQUE` 模式仍只在本地决策（节点看不到对端的会话），但每次 CONNECT 现在也会关掉其它节点上同一 client id 的重复连接，因此在节点间漫游的客户端不再收到重复投递；`AuthTimeout` 对自定义 `AclManager` 的每次 subscribe 与 publish 生效；`Authenticator` 在 `AuthTimeout` 内不回答的客户端会被拒绝。

- **inflight 窗口满不再丢 QoS 1/2 投递，卡住的 socket 也不再阻塞 broker。** 两个相关的投递缺陷被修好。

  *inflight 队列。* 发现连接 inflight 窗口满的 QoS 1/2 投递现在为该连接排队，在某个确认腾出位置时按到达顺序发出。队列受新的 `Config.MaxInflightQueueSize`（默认 256）约束；只有发现它也满的投递才被丢弃，且该丢弃计两次（`RecordDroppedMessage("inflight_overflow")`、`RecordOverflow("inflight")`）并以 Warn 级别带 client id 记录。行为变化：之前 `RecordDroppedMessage("inflight_full")` 的跳过没有了——已确认的发布不再因为订阅者慢而被静默丢弃。离线队列排空在窗口满时保留其有文档的 QoS 0 降级。

  *writer goroutine、有界出站队列与写截止时间。* 一个连接的每个包现在都由一个 writer goroutine 按入队顺序写出，队列受新的 `Config.MaxOutboundQueueSize`（默认 256 个包）约束，每个包的写截止时间来自新的 `Config.WriteTimeout`（默认 5 s）。`SendLocal`、`close` 里的 will 发布、集群接收路径与每个确认都不再在调用方 goroutine 上写 socket，因此停止读取的对端不能卡住发布者的读循环、一个正在关闭的连接或节点唯一的集群接收 goroutine。队列满时丢弃 QoS 0 PUBLISH（`RecordDroppedMessage("outbound_queue_full")`、`RecordOverflow("outbound_queue")`）；对 QoS 1/2 PUBLISH 或确认则改为断开慢消费者（`RecordDroppedMessage("slow_consumer")` 加一条 Warn 日志）。队列按包数计，不按字节。握手的 CONNACK 与服务端 DISCONNECT 仍同步写出，因此两者都在连接关闭前到达线上。writer goroutine 随连接结束，由 `close` 与 `serve` 汇合，`Stop` 仍等待 broker 的每个 goroutine。

  对既有部署的行为说明：`SendLocal` 现在在包到达线上之前就返回（报错表示包未被接受——连接被关闭，或消息在队列满时按 QoS 0 被丢弃）；不读自己 socket 的客户端最终会被断开而不是让投递停摆；超过 `WriteTimeout` 的一次写会关闭连接。`MaxInflightQueueSize`、`MaxOutboundQueueSize` 与 `WriteTimeout` 没有“无上限”设置：非正值取默认。

- **持久会话存储、有界离线队列与 broker 侧 QoS 1/2 重传。** 本族参考 broker 的两个对齐缺口一起补上，因为重连后的投递若从不确认也不重试，排队离线消息就没有意义。默认值都有界且是选定的；部署依赖过的每个早先行为都仍然可达。

  *会话 store 缝。* `SessionStore`（`Load`、`Save`、`Delete`、`Keys`）现在持有没有存活连接的会话状态：订阅、排队消息与过期截止时间。`Config.SessionStore` 默认 `MemorySessionStore`，即早先版本的内存行为，这里不内置网络或磁盘后端。`Broker.sessions` 已删除；连上的会话自己拥有状态，因此一条发布只到达一个会话一次。

  *会话过期。* MQTT 5 的 Session Expiry Interval 现在生效：`ConnectInfo.SessionExpiry` 在断开时记为截止时间，一个有界清扫器（`SessionSweepInterval`，默认 30 s，每轮最多 512 个会话）删除过期会话及其订阅与排队消息，CONNECT 路径拒绝恢复截止时间已过的会话。非 clean 会话上的零区间保持之前行为、永不过期（MQTT 3.1.1 没有区间）；MQTT 5 上零区间仍随连接结束会话。

  *离线队列。* 没有存活连接的持久会话按 FIFO 顺序保存其订阅命中的消息，并在下一次 CONNACK 之后立即收到，packet id 在排空时分配。拿不到空闲 packet id 或 inflight 位置的消息以 QoS 0 投递，而不是被丢弃或乱序。`MaxOfflineQueueSize`（默认 1000）在会话满时丢最旧；`MaxTotalOfflineMessages`（默认 100000）在全局上限处拒绝最新。clean 会话从不排队（参考 broker 连它们也排队——未照搬）。两种丢弃都可通过 `MetricsManager` 看到（`offline_queue_full`、`offline_queue_total`），`RecordQueueDepth("offline", n)` 报告当前深度。

  *出站 QoS 1/2。* 每个未确认的 QoS 1/2 投递按 packet id 保存在每连接的 inflight 表里，每连接一个定时器驱动，而不是每条消息一个 goroutine。`QoSRetryInterval`（默认 5 s）是基础间隔，每次尝试翻倍，`QoSMaxRetries`（默认 5）封顶。重传的 PUBLISH 带 DUP（参考 broker 从不设置——未照搬）；QoS 2 走 PUBLISH → PUBREC → PUBREL → PUBCOMP，PUBREL 重试到 PUBCOMP。`MaxInflightQoS1`（默认 64）约束一个连接未确认的 QoS 1 投递，`MaxInflightQoS2`（默认 64）约束其两个方向的 QoS 2 交换；超过窗口的投递为该连接排队而不是阻塞发布者（见上面那条，它取代了本条引入的 `inflight_full` 跳过），入站 QoS 2 PUBLISH 超过窗口在 MQTT 5 上以 PUBREC 0x97 Quota exceeded 回答。`MaxInflightQoS2` 在本次改动前被声明但无人读取。

  行为说明：clean 会话不再累积任何东西；会话清扫器每个 broker 一个 goroutine，`Stop` 会等它；inflight 交换与入站 QoS 2 状态是每连接的，因此重连恢复离线队列但不恢复上一连接未确认的投递；`LocalTargets` 可能返回离线目标（`LocalTarget.Offline`），遍历所有目标的自定义 `Dispatcher` 现在也能到达离线会话。想要之前“不重试、不丢弃”流程的部署把 `MaxInflightQoS1` 与 `QoSMaxRetries` 设为负值。

- **Dispatcher SPI、连接上下文与 namespace 隔离。** 本族参考 broker 的三个可移植性缺口用 Go 的等价物补上；既有部署全部保持行为，因为零值选择之前的路径。

  *Dispatcher。* `Config.Dispatcher`（`Dispatcher`、`Publication`）现在是已接受发布的唯一决策点。之前的硬编码路径是 `DefaultDispatcher`（本地匹配会话加集群 spread），因此 nil dispatcher 什么都不变；自定义 dispatcher 可以抑制投递，或通过新的只读 broker 缝（`LocalTargets`、`SendLocal`、`SpreadPublication`）走路由，而不必重新实现 topic 匹配。retained 状态仍由 broker 在路由前写入。

  *连接上下文。* 每个 hook 现在收到连接作用域的 context，而不是 `context.Background()`：它携带 `ConnInfo`（`ContextWithConn`/`ConnFromContext`），在 CONNECT 时由新的 `Config.ConnContext` 富化一次，整个连接复用，连接关闭时取消。断开 hook 观察到的是已取消的 context，它仍携带结束的那条连接的 ConnInfo。

  *Namespace。* `Config.Namespace`（早已存在）现在划分会话表、订阅匹配、retained store 与通告的订阅摘要。`ClusterMessage` 携带 namespace，两个内置后端都会丢弃其它 namespace 的远端事件或摘要。空 namespace 是默认值并保持之前行为，包括 Redis 键布局。

  行为说明：集群 spread 失败现在记为 `gmqx dispatch failed`（错误由 dispatcher 返回），远端发布刻意不再交给 dispatcher——它的源节点已经路由过，再分发会把它送回集群。

- **协议一致性修复（MQTT 3.1.1 / 5）。** 一套线上格式一致性测试（`TestConformance`、`mqtt_conformance_test.go` 与 `mqtt_conformance_v5_test.go`）对着真实进程内 broker 走完 MQTT 3.1.1 与 MQTT 5 特性矩阵。它暴露出以下缺陷，本次修好；命中已记录限制的用例带原因跳过。

  *持久会话。* 清空 Clean Session（MQTT 5：Clean Start）的 CONNECT 现在恢复存储的订阅并回答 Session Present；clean CONNECT 丢弃存储的会话。CONNECT 携带非零 Session Expiry Interval 时 MQTT 5 会话被存储。定时过期仍未做（见 ROADMAP.md）。

  *will 保真。* will 以其配置的 QoS、RETAIN 标志与 PUBLISH 合法属性发布，而不是 QoS 0 且两个标志都不带。非法的 will topic 拒绝 CONNECT（MQTT 5 上 CONNACK 0x90，3.1.1 上关闭），MQTT 5 上 reason 0x04 的 DISCONNECT 会发布 will。

  *订阅 QoS 是上限。* 投递使用 `min(publication QoS, 命中的最高订阅 QoS)`，因此 QoS 1 发布不再以 QoS 1 到达 QoS 0 订阅。

  *QoS 2 重复抑制。* 重传的 QoS 2 PUBLISH（同 packet id、带 DUP）再次以 PUBREC 回答但只投递一次；PUBREL 释放 packet id 供复用。

  *主题规则。* PUBLISH topic name 必须非空且不含通配符；非法 topic filter 被拒绝（MQTT 5 上 SUBACK 0x8F，3.1.1 上关闭连接）；开头的 `#`/`+` 不再匹配保留的 `$` 主题。

  *MQTT 5 错误上报。* 协议错误现在以携带对应 reason code 的 DISCONNECT 结束连接：0x81 畸形包、0x82 协议错误（无 alias 的空主题）、0x90 topic name 非法、0x94 topic alias 非法、0x95 包过大。MQTT 3.1.1 直接关闭，因为它没有服务端 DISCONNECT。

- **集群：retained 复制与 node id 归属。** 共享集群路径上的两个缺陷修好。

  *retained 复制。* 从对端收到的 retained 发布现在按本地 retained 发布的规则应用到接收节点的 retained store（空载荷删除、`MaxRetainMessages` 约束 store、保留 QoS 与属性），而不是只当作一条实时消息投递。两个后端本来就在做的广播因此变得有用：之后连到别的节点的订阅者能从那个节点的 store 拿到 retained 值。保证是最终存在于 store；远端 retained 发布到达时已经接入的订阅者不会被重放该值。两个后端都受影响；store 更新由 broker 共享，重复的 `(Origin, Seq)` 仍在到达 store 之前被后端丢弃。

  *node id 归属。* Redis 后端现在强制每个 node id 只有一个存活持有者。`Start` 在订阅节点频道之前用 `SET NX` 与 `NodeTTL` 取得 `<ChannelPrefix>:owner:<nodeID>`，随成员键续租，`Stop` 时释放（compare-and-delete，因此丢掉租约的进程无法释放新持有者的 id）。同一 id 的第二个存活进程以新的 `ErrNodeIDInUse` 失败 `Start`（点名持有者的 incarnation），而不是共用频道、把每个事件应用两遍、并收敛成一个 `Members()` 条目。由于租约是 TTL，未释放就死掉的进程只会阻塞重启直到 `NodeTTL` 过期。TCP 后端不变：它的握手已经保证每个 node id 恰好一个成员，且它没有可用于仲裁租约的共享 store。

  *行为变化。* `RedisClusterConfig.NodeTTL` 现在也管归属键。node id 已被存活进程持有的 Redis 集群不再启动；调用方可以用 `errors.Is(err, ErrNodeIDInUse)` 匹配该拒绝。丢掉租约的进程（例如暂停超过 `NodeTTL`）停止刷新自己的成员键并记录这次丢失，而不是覆盖新持有者的成员关系。

- **Redis 集群后端。** `NewRedisCluster` 给 O(N²) 的 TCP mesh 加了一个集中式替代：节点共用 Redis 而不互相拨号，成员、路由与定向投递都在 Redis 里。

  *布局。* `<ChannelPrefix>:node:<nodeID>` 是节点的私有事件频道——它唯一订阅的频道；`<ChannelPrefix>:member:<nodeID>` 以 `NodeTTL` 保存存活与身份；`<ChannelPrefix>:subs:<nodeID>` 以 `SummaryTTL` 保存订阅摘要。一条发布对每个摘要匹配其 topic 的节点是一次 `PUBLISH`，因此 N 个节点是 O(N) 次操作而不是 O(N²) 条链路，且发布没有共享广播频道。

  *与 TCP 后端相同的语义。* Origin/Seq 打戳、共享的有界 TTL 去重表、先源后去重的环路保护、“收到的事件绝不转发”、丢弃原因（`cluster_duplicate`、`cluster_loop`、`cluster_forward`）、带最旧 filter 上限的摘要匹配，以及 retained/kick 广播规则都是复用而不是重写；摘要计算搬到了 `cluster_summary.go`，现在由两个后端共享。

  *成员。* 成员键的 TTL 就是整个成员协议：停止刷新的节点在键过期时从所有视图消失，没有驱逐流程，也不需要干净关闭。`Members()`（可选 `MemberView`）读取成员键，返回按 node id 排序的 `[]Member`（只含对端，`Addr` 为空因为 Redis 没有对端地址），Redis 读不到时不报告对端。重启就是新 incarnation；`Stop` 删除节点的键，对端立即丢弃它。

  *故障行为。* Redis 不可达或配置无法满足（空 `Addr`、负 TTL/尺寸、TTL 无法长过 `SummaryInterval`）时 `Start` 失败。运行时本地产生的 `Spread` 返回新的 `ErrClusterUnavailable`（包装原因）而不是原地重试，接收循环以有界退避（50ms→1s）持续重新订阅直到 `Stop`；只记录进入与离开降级状态的转变。`Stop` 幂等且不留下 goroutine。

  *依赖。* `github.com/redis/go-redis/v9`（客户端），以及仅测试用的 `github.com/alicebob/miniredis/v2`。

## 2026-10-30

### 新增

- **内置 TCP 集群：成员、去重、定向路由。** 集群不再只是裸 flood 的 `ClusterMessage` 载荷。

  *成员。* 每条链路现在以一个带版本的握手帧开始（node id、通告的监听地址、每次 `Start` 随机的进程 incarnation、发送方的成员视图）；说另一种线版本的 peer 被拒绝，因此版本不匹配不会造出第二个逻辑成员。每个 node id 恰好一个逻辑成员：指向同一 incarnation 的重复链路收敛到由较小通告地址拨出的那条并记录，新 incarnation（peer 重启）替换旧链路并重置 `Since`。心跳每 `HeartbeatInterval` 一次，错过 `HeartbeatMissBudget` 个间隔的成员被驱逐并关闭其链路；在同一窗口内从未完成握手的链路被关闭。`Members()`（新的可选 `MemberView` 接口、`Member{NodeID, Addr, Incarnation, Since}`）在不改 `Cluster` 接口的前提下暴露该视图。来自未知或已被驱逐节点的事件仍被处理，但绝不会让它复活。

  *去重。* `ClusterMessage` 增加增量字段 `Origin` 与 `Seq`：集群为每个本地产生的事件打上自己的 node id 与一个单调递增的节点内序号。接收方保留一张有界、带 TTL 的已应用 `(Origin, Seq)` 标识表（`DedupSize`、`DedupTTL`，惰性清扫，最旧的先被驱逐）并把重复只应用一次。收到的事件绝不转发，源是接收节点自身的事件被丢弃，因此重广播拓扑不会成环。丢弃被计数并通过可选 `TCPClusterConfig.Metrics` 上报。

  *定向路由。* 对端通告其本地会话订阅的 topic filter，每 `SummaryInterval` 刷新，订阅变化时立即刷新（broker 增加了推送路径）。发布只发给摘要匹配该 topic 的对端；摘要未知或过期的对端仍会收到这次发布——只发给它自己，绝不 flood。retained 发布与 kick 照旧发给每个对端（retained 状态必须处处存在；kick 指名 client id 而不是 topic）。摘要每个节点受 `MaxSummaryFilters` 限制，最旧的 filter 先被驱逐。

  *行为变化。* 线格式带版本，与之前的裸 JSON 帧不兼容：混合版本节点在握手处互相拒绝，而不是半工作。集群未运行时 `Spread` 返回错误，拒绝携带别的节点 `Origin` 的事件，并在本地产生的事件上用自己的 node id 重打 `NodeID`/`Origin`。路由基于摘要，因此在发布者最后一次摘要更新之后出现的订阅者最多可能漏一条消息一个 `SummaryInterval`；投递仍是 best-effort。

- **报文层改走类型化 codec。** gmqx 不再逐字节解析或构造 MQTT 包：`mqtt.Decoder` 读每个包，`mqtt.Encode` 写每个包，连接循环按具体消息类型（`*mqtt.Publish`、`*mqtt.Subscribe`……）分派，而不是按固定头部字节。`packet.go` 与 `properties.go` 已删除；新的 `props.go` 只保留把类型化 MQTT 5 属性集与 `PublishEvent.Properties`、`ClusterMessage.Props` 携带的属性字节互相转换的缝。公开 API 不变。线上可见差异都是规范正确的：CONNACK 不再通告非法的 `Maximum QoS 2`（缺席本就意味着 QoS 2）、MQTT 5 UNSUBACK 每个 filter 一个 reason code、MQTT 5 未确认时的默认值（reason code 0、空属性块）按规范允许省略。MQTT 3.1 仍工作：它的 CONNECT 在 codec 看到之前被改写成 3.1.1 的拼法。

- **H1：带结果的 publish。** `EventSink.OnPublish` 现在收到 `PublishEvent`（connection id、client id、topic、payload、QoS、packet id、retain、属性）并返回 `PublishResult`（`PublishAccepted`/`PublishRetryable`/`PublishRejected` + MQTT 5 reason code）。PUBACK/PUBREC **只在**结果为接受之后发送：MQTT 3.1.1 没有否定确认，因此可重试或拒绝的发布保持未确认，而不是乐观确认。`Config.PublishCallbackTimeout` 约束 sink 等待，并把超时的等待视为可重试。retained 状态现在只为被接受的发布写入，因此被拒绝的消息不会变成 retained 值。

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

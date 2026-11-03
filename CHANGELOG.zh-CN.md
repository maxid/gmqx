[English](CHANGELOG.md) | 简体中文

# 变更日志

> 开发快照不打 tag。条目按变更落地日期分组（新到旧），当日发布的版本作为该日期节内的版本号子标题；没有挂在版本号下的条目属于未发布快照。本文为中文摘要版，逐条技术细节以英文版为准。

## 2026-11-03

### 新增

- **hook 现在能看到连接是由哪个监听器接受的。** `hook.ConnInfo` 增加 `Transport`（`"tcp"/"tls"/"ws"/"wss"`，与 `Addrs()` 同名），审计、指标或 ACL 不必再自己保存一份 broker 配置就能区分 TLS 端口与 WebSocket 端口上的设备。监听器名字从 listener spec 穿过 `conn.Client`（`SetTransport`，握手前调用）；自己把连接交给 broker 的嵌入方报告空值。

- **`client.Config.TLS` 会从 `Addr` 补 `ServerName`。** 只给了根证书池、没给 `ServerName` 的配置原先会报 "either ServerName or InsecureSkipVerify must be specified"，而这个要求字段文档里没写；现在客户端按 `tls.Dial` 的方式从 `Addr` 取值，显式 `ServerName` 照旧优先，且不会改动调用方的 `tls.Config`。

- **互操作矩阵覆盖安全与 WebSocket 监听器。** paho 对本 broker 的 TLS/WS/WSS 与 mTLS 各跑一遍同样的发布订阅；参考 broker 方向加了 TLS 监听器（含反例：不信任 CA 的客户端必须失败）。

### 修复

- **多监听器 `Start` 失败后不再报告已停掉监听器的地址。** 第二个监听器绑定失败时，`Addrs()` 仍留着已被停掉的那个地址，监管方可能读到一个没有任何进程监听的端口；回滚现在会清空 servers 与地址，与单监听器路径上"失败的 Start 不留残余"的含义一致。

- **`MaxPacketSize` 现在对 WebSocket 监听器生效。** WS/WSS 监听器从未把消息上限传给 gnety，等于静默沿用其 4 MiB 默认值：配置 `MaxPacketSize: 8 MiB` 后，5 MiB 的 PUBLISH 在 TCP/TLS 上正常，在 WS/WSS 上被以 WebSocket 状态码 1009 断开。现在四条传输统一由 `Config.MaxPacketSize` 决定：4 MiB 是库的默认值而不是协议上限（RFC 6455 没有消息大小上限，MQTT 也允许一帧承载多个或部分控制报文），而其它传输本来就只服从这一个数字。上限按整条 WebSocket 消息计，把多个包黏进一条消息的发送方按总和受限（详见英文版）。

## 2026-11-02

### 新增

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

- **broker 引擎移到 `gmqx/server`，根包只剩门面。** 连接/会话状态机、路由索引、字节速率整形、配置、
  错误与 broker 侧的 cluster 适配器一起进一个包——它们是同一个整体（inflight/will/auth 是未导出
  `client` 的方法，路由索引持有 `*client`）。根包把公开类型全部别名导出并转发构造函数，消费方零改动；
  根实现文件降到 8 个（全是门面），`libcheck` 的 gmqx 欠账条目随之删除。`server/` 是落脚点不是终点：
  后续按角色（会话状态、routing、conn）的拆分计划记在 `docs/design/source-layout.md` §4.3。

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
  连接上。引擎保留待发 will 表与定时器、认证 exchange 工厂与会话簿记；`gmqx/server` 从 ~7.5k 行降到
  ~4k 行。连接拒绝错误与 `MaxAuthSteps` 随产生它们的代码走并在 server 重新导出，`errors.Is`、错误值
  与根包公开 API 均不变。

- **`gmqx/server` 成为装配层，只剩五个文件。** 角色拆分的最后一刀把两件本就是独立角色的事搬出引擎：
  字节速率整形归 `gmqx/conn`（它包住的是一条连接的 `net.Conn`；broker 只保留每监听器设置与全局桶，
  调用 `conn.Shape`）；其余属于 Broker 的各个侧面——`conn.Host` 实现、会话簿记、待发 will 表与定时器、
  插件宿主、公开编程面与 hook 别名、会话 store 门面、auth exchange 工厂——都成为 `broker.go` 的分节；
  共享订阅分发并入 `cluster.go`。目录在打开任何文件前就回答“这里是装配层”：`broker.go`、
  `cluster.go`、`config.go`、`errors.go`、`doc.go`。行为零变化；根包公开 API 逐字节相同
  （`go doc -all`），`ByteRateMetrics` 是 conn 同名接口的别名。

- **引擎变为 `internal/engine`，根包只剩一个 `facade.go`。** 装配包原本是 `gmqx/server`——一个公开
  import 路径，文件名与根门面一一对应：同一个 broker 有两个公开包，其中一个还是逐文件复制的名字。
  现在它是 `internal/engine`（包名 `engine`），外部不可 import；根目录的八个别名文件合成一个
  `facade.go`（外加 `version.go`）。公开 API 不变：每个名字仍是别名（`gmqx.Broker` ==
  `engine.Broker`），与上一提交用 `go doc -all` 对比（标识符集合差异 0），并用一个模块外的消费者验证过
  （实现 `gmqx.Dispatcher`/`gmqx.EventSink`、嵌入 `gmqx.NoopMetrics`、创建 broker）。
  `gmqx/server` 从未被文档列为公开路径、仓内除门面外无人 import，现已删除。

## 2026-11-01

### 新增

- **入站 PUBLISH 不再每包分配报文体、主题字符串与载荷**：读循环用 `codec/mqtt` 的 `ReadPublishInto` 解码进每连接一份、只增长的缓冲区（`client.readBuf`，受 broker `MaxPacketSize` 限制），其余报文（PUBACK、SUBSCRIBE、PINGREQ、读错误）仍走原来的类型化解码器。缓冲区会被下一个包复用，因此发布持有载荷的一份复制：`client.handlePublish` 在调用 `Broker.publish` 之前复制一次，这是热路径上唯一的复制位置（retained 存储、offline 队列与集群广播保持各自的复制）。语义不变：相同的校验与 DISCONNECT reason code（超限报文仍为 0x95，因为大小检查仍在读取任何字节之前）、相同的 topic alias 处理、相同的 inflight 与排队行为、sink 看到的 `PublishEvent` 相同且其载荷活过调用。顺序基准实测：`BenchmarkPublishQoS0` 928 B/op、9 allocs/op → 528 B/op、6 allocs/op，`BenchmarkPublishQoS1` 1,667/13 → 1,264/9，`BenchmarkClientPublishQoS1` 3,121/22 → 2,721/18；`ns/op` 与 200 连接的投递速率都落在运行间噪声带内，因为该负载下瓶颈是系统调用而不是解码（见 `docs/PERFORMANCE.md`）。

- **持久会话的 inflight 队列满不再丢弃已确认的 QoS 1/2 投递**：投递进入连接的 pending 队列，session 转为 offline；连接按丢失路径关闭，`unregister` → `SessionStore` 保存完整状态，下一次 CONNECT 继续消费。clean session 仍只有 `inflight_overflow_clean` 一条显式丢弃路径。持久会话的 offline queue 上限（`MaxOfflineQueueSize` 丢最旧、`MaxTotalOfflineMessages` 拒最新）仍是仅有的丢失点，且已有计数。

- 持久化与可用性蓝图（`docs/PERSISTENCE.md`）：重启后必须保留/可以丢失的内容、`SessionStore` 契约（原子 take、按 session expiry 的 per-key TTL、namespace-scoped key、精确往返、损坏值丢弃并计数）、可编译的自定义 store 示例、Redis 蓝图与失败模式、SQL 事务草图、为什么缓存库不能当 session store、共享 store + gossip cluster 的可用性模型，以及 `MaxOfflineQueueSize`/`MaxTotalOfflineMessages` 容量计算。

- `interop/` 互操作矩阵：独立 module，保持库零第三方依赖；第三方 client 对本 broker、本 client 对 docker 参考 broker、参考命令行 client 对本 broker；docker 不可用时干净跳过。

- 基准与 soak 测试（`docs/PERFORMANCE.md`）：QoS 0/1 publish 吞吐、10/100 subscriber fanout、10k 订阅、retained replay、client round trip，以及断言空表、goroutine 有界、delivery ledger 的 churn soak。

- **MQTT client library `github.com/maxid/gmqx/client`**：基于标准库与 `gnety/codec/mqtt` 的完整 MQTT 3.1.1/5 client；QoS 1/2 状态机、DUP/PUBREL 重传、inflight window、重连退避、订阅恢复、keepalive、MQTT 5 属性、有界有序 writer/delivery 队列。

- 连接/session 治理、共享订阅、subscription identifier、Will Delay、AUTH 增强认证、retained expiry、跨节点 takeover、kick epochs、Redis shared session store、Receive Maximum、Maximum Packet Size、QoS 2 重连状态、字节速率整形、listener 端口回退、gossip/TCP/Redis cluster、client-id presence、directed routing、MQTT 3.1 兼容等（详见英文版逐条说明）。

### 0.6.0-dev.1

- Transport 迁移到 gnety（`RawHandler`）：TCP/TLS/WS/WSS listener 由 gnety 提供，MQTT codec 与 broker 状态机仍在 gmqx。

### 0.5.0

- MQTT 5 CONNACK capability 属性（retain/wildcard/subscription-id/shared/max-qos/max-packet/topic-alias）。
- Topic Alias（入站与 per-session 解析）。
- Publish 属性转发（User Property、content type、response topic、correlation data）。
- Subscription options：no-local、retain-as-published、retain-handling。
- v5 CONNACK 属性、topic alias、user property 转发测试。

### 0.4.0

- 基础 MQTT 5：带 properties 的 CONNECT（跳过）、v5 CONNACK/SUBACK/UNSUBACK 格式、v5 PUBLISH property block、v5 负向 ACK reason code（Not authorized、Quota exceeded、Client Identifier not valid），以及 v5 connect/pub-sub/auth 测试。

### 0.3.0

- 多 listener：TCP、TLS（server cert/可选 client cert）、WebSocket、WSS。
- WebSocket subprotocol 校验与 path 配置。
- TLS/WS 集成测试与 listener address 访问器。

### 0.2.3

- 文档清理：README/docs 产品中性、自包含。

### 0.2.2

- 升版本常量；补 connect mode 测试。

### 0.2.1

- 补 UNIQUE/KICK connect mode 测试。

### 0.2.0

- Broker SPI：AuthManager、AclManager、EventSink、MetricsManager、Interceptor。
- ConnectMode UNIQUE/KICK、连接/订阅/retained 限额、连接速率限制。
- Metrics counter 与生命周期回调（connect/disconnect/lost/ping）。

### 0.1.0

- 初始单节点 MQTT 3.1.1 broker：TCP listener、CONNECT/SUBSCRIBE/PUBLISH/PING/DISCONNECT、QoS 0/1/2 基础流程、retained、wildcard 订阅、will 与 keepalive。

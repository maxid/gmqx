[English](CHANGELOG.md) | 简体中文

# 变更日志

> 开发快照不打 tag；以下条目按时间顺序排列。本文为中文摘要版，逐条技术细节以英文版为准。

## [Unreleased]

- **持久会话的 inflight 队列满不再丢弃已确认的 QoS 1/2 投递**：投递进入连接的 pending 队列，session 转为 offline；连接按丢失路径关闭，`unregister` → `SessionStore` 保存完整状态，下一次 CONNECT 继续消费。clean session 仍只有 `inflight_overflow_clean` 一条显式丢弃路径。持久会话的 offline queue 上限（`MaxOfflineQueueSize` 丢最旧、`MaxTotalOfflineMessages` 拒最新）仍是仅有的丢失点，且已有计数。
- 持久化与可用性蓝图（`docs/PERSISTENCE.md`）：重启后必须保留/可以丢失的内容、`SessionStore` 契约（原子 take、按 session expiry 的 per-key TTL、namespace-scoped key、精确往返、损坏值丢弃并计数）、可编译的自定义 store 示例、Redis 蓝图与失败模式、SQL 事务草图、为什么缓存库不能当 session store、共享 store + gossip cluster 的可用性模型，以及 `MaxOfflineQueueSize`/`MaxTotalOfflineMessages` 容量计算。
- `interop/` 互操作矩阵：独立 module，保持库零第三方依赖；第三方 client 对本 broker、本 client 对 docker 参考 broker、参考命令行 client 对本 broker；docker 不可用时干净跳过。
- 基准与 soak 测试（`docs/PERFORMANCE.md`）：QoS 0/1 publish 吞吐、10/100 subscriber fanout、10k 订阅、retained replay、client round trip，以及断言空表、goroutine 有界、delivery ledger 的 churn soak。
- **MQTT client library `github.com/maxid/gmqx/client`**：基于标准库与 `gnety/codec/mqtt` 的完整 MQTT 3.1.1/5 client；QoS 1/2 状态机、DUP/PUBREL 重传、inflight window、重连退避、订阅恢复、keepalive、MQTT 5 属性、有界有序 writer/delivery 队列。
- 连接/session 治理、共享订阅、subscription identifier、Will Delay、AUTH 增强认证、retained expiry、跨节点 takeover、kick epochs、Redis shared session store、Receive Maximum、Maximum Packet Size、QoS 2 重连状态、字节速率整形、listener 端口回退、gossip/TCP/Redis cluster、client-id presence、directed routing、MQTT 3.1 兼容等（详见英文版逐条说明）。

## [0.6.0-dev.1] - 2026-10-29

- Transport 迁移到 gnety（`RawHandler`）：TCP/TLS/WS/WSS listener 由 gnety 提供，MQTT codec 与 broker 状态机仍在 gmqx。

## [0.5.0] - 2026-10-29

- MQTT 5 CONNACK capability 属性（retain/wildcard/subscription-id/shared/max-qos/max-packet/topic-alias）。
- Topic Alias（入站与 per-session 解析）。
- Publish 属性转发（User Property、content type、response topic、correlation data）。
- Subscription options：no-local、retain-as-published、retain-handling。
- v5 CONNACK 属性、topic alias、user property 转发测试。

## [0.4.0] - 2026-10-29

- 基础 MQTT 5：带 properties 的 CONNECT（跳过）、v5 CONNACK/SUBACK/UNSUBACK 格式、v5 PUBLISH property block、v5 负向 ACK reason code（Not authorized、Quota exceeded、Client Identifier not valid），以及 v5 connect/pub-sub/auth 测试。

## [0.3.0] - 2026-10-29

- 多 listener：TCP、TLS（server cert/可选 client cert）、WebSocket、WSS。
- WebSocket subprotocol 校验与 path 配置。
- TLS/WS 集成测试与 listener address 访问器。

## [0.2.3] - 2026-10-29

- 文档清理：README/docs 产品中性、自包含。

## [0.2.2] - 2026-10-29

- 升版本常量；补 connect mode 测试。

## [0.2.1] - 2026-10-29

- 补 UNIQUE/KICK connect mode 测试。

## [0.2.0] - 2026-10-29

- Broker SPI：AuthManager、AclManager、EventSink、MetricsManager、Interceptor。
- ConnectMode UNIQUE/KICK、连接/订阅/retained 限额、连接速率限制。
- Metrics counter 与生命周期回调（connect/disconnect/lost/ping）。

## [0.1.0] - 2026-10-29

- 初始单节点 MQTT 3.1.1 broker：TCP listener、CONNECT/SUBSCRIBE/PUBLISH/PING/DISCONNECT、QoS 0/1/2 基础流程、retained、wildcard 订阅、will 与 keepalive。

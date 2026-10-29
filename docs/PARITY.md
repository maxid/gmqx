# gmqx ↔ jmqx 对等矩阵与路线

> 参考实现：`/Users/maxid/Develop/IdeaProjects/github/jmqx`（Java，`dev`，1.4.22-SNAPSHOT，约 2,257 文件 / 25.9 万行，四个模块）
> 本文档是 gmqx 的功能事实源：**先对齐语义，再追求 API 相似**。状态以 gmqx 当前发布版本为准（v0.2.0）。

## 1. Java 版模块与定位

| 模块 | 规模 | 说明 |
|---|---|---|
| `jmqx-broker` | 124 文件 / 13,970 行 | 可嵌入 MQTT Broker：TCP/TLS/WS/WSS、鉴权/ACL/生命周期 SPI、指标、限额、拦截器 |
| `jmqx-cluster` | 13 文件 / 2,682 行 | 基于 ScaleCube 的集群：gossip、会话路由、订阅感知扩散、故障检测 |
| `jmqx-client` | 135 文件 / 11,895 行 | MQTT 3.1.1/5 客户端：Reactor/Async/Blocking、重连、离线缓存、背压 |
| `jmqx-example` | 9 文件 / 569 行 | Spring Boot 集成示例 |

## 2. Broker 能力矩阵

图例：✅ 已实现（v0.2.0）· 🟡 部分/差异 · ⬜ 计划 · ❌ 不做或 Go 不需要

### 2.1 传输与协议

| 能力 | Java jmqx | gmqx v0.2.0 | 计划 |
|---|---|---|---|
| MQTT 3.1.1 TCP | ✅ | ✅ | — |
| MQTT 3.1（旧版本） | ✅ | ⬜ | v0.3 视现网设备决定 |
| MQTTS（TLS） | ✅ | ⬜ | **v0.3** |
| MQTT over WebSocket | ✅ | ⬜ | **v0.3** |
| MQTT over WSS | ✅ | ⬜ | **v0.3** |
| WS 子协议 `Sec-WebSocket-Protocol` 校验 | ✅ | ⬜ | v0.3 |
| MQTT 5（线格式 + 部分语义） | 🟡 | ⬜ | v0.3 先握手/ReasonCode，v0.4 补 Properties |
| 同进程多实例/namespace | ✅ | 🟡（可多 Broker 实例；namespace 字段已留） | v0.3 收口 |
| 端口占用自动顺延 | ✅ | ⬜ | v0.3（Go 直接 `:0` 由部署层处理，可做辅助） |

### 2.2 MQTT 语义

| 能力 | Java jmqx | gmqx v0.2.0 | 计划 |
|---|---|---|---|
| CONNECT/CONNACK、SUBSCRIBE/SUBACK、UNSUBSCRIBE、PUBLISH、PING、DISCONNECT | ✅ | ✅ | — |
| QoS 0/1（PUBACK） | ✅ | ✅ | — |
| QoS 2（PUBREC/PUBREL/PUBCOMP） | ✅ | 🟡（握手级） | v0.3 完整状态机 |
| Retained 消息 | ✅ | ✅（含数量上限） | — |
| Will 消息 | ✅ | ✅（基础） | v0.3 延迟/属性（MQTT5） |
| 通配符 `+`/`#` | ✅ | ✅ | — |
| 共享订阅 `$share` | ❌ | ❌ | 与 Java 一致，不做 |
| 定向投递 by clientId | ✅ | ⬜ | v0.4（需客户端管理与 ACL） |
| KICK/UNIQUE 连接模式（session takeover） | ✅ | ✅（v0.2.0 新增） | — |
| 会话持久化/Clean Session 语义 | 🟡（内存） | 🟡（内存；Clean Session 位已读） | v0.4 内存会话恢复 |
| 离线消息队列 | 🟡 | ⬜ | v0.4 |
| 消息拦截器链 | ✅ | 🟡（单接口） | v0.4 链式 |
| MQTT 5 User Property/ReasonCode | 🟡 | ⬜ | v0.4 |

### 2.3 SPI（对齐 jmqx）

| jmqx SPI | gmqx | 状态 |
|---|---|---|
| `AuthManager.auth(clientId, username, password)` + `requiresOffload` | `AuthManager.Authenticate(ctx, ConnectInfo) error` + `RequiresOffload()` | ✅（Go 每连接 goroutine，offload 语义天然满足） |
| `AclManager.check(session, topic, action)` + `requiresOffload` | `AclManager.Check(ctx, SessionInfo, topic, AclAction)` + `RequiresOffload()` | ✅ |
| `PlatformDispatcher.onConnect/onDisconnect/onConnectionLost/onPublish/onPing` | `EventSink` 同名语义 | ✅ |
| `MetricsManager`（连接/发布/订阅/丢弃/队列） | `MetricsManager` 同名（v0.2.0 新增） | ✅ |
| `Interceptor` SPI | `Interceptor.OnPublish`（返回 false 丢弃） | 🟡（单拦截器；v0.4 链式） |
| `ClusterRegistry` | `ClusterRegistry` 接口占位（未实现） | ⬜ v0.5 |
| `DynamicLoader`（ServiceLoader） | Go 显式注入（构造函数/config），无动态加载 | ❌（Go 惯例；编译期组合） |
| Netty 线程池/watermark 配置 | 不适用（Go runtime 调度） | ❌ |

### 2.4 限额与运行参数

| jmqx 配置 | gmqx Config | 状态 |
|---|---|---|
| `port`/`securePort`/`websocketPort`/`websocketSecurePort`/`websocketPath` | `Port`/`SecurePort`/`WebsocketPort`/`WebsocketSecurePort`/`WebsocketPath` | 🟡 仅 TCP 生效，TLS/WS 字段保留 |
| `sslEnable`/`sslCa`/`sslCrt`/`sslKey` | `TLS{Enabled,CA,Cert,Key}` | ⬜ v0.3 |
| `connectMode`/`notKickSeconds` | `ConnectMode`/`NotKickSeconds` | ✅ |
| `messageMaxSize` | `MaxPacketSize` | ✅ |
| `maxConnections`/`maxOfflineQueueSize`/`maxTotalOfflineMessages` | `MaxConnections`（离线队列 v0.4） | 🟡 |
| `maxRetainMessageCount` | `MaxRetainMessages` | ✅ |
| `maxTopicSubscriptions` | `MaxTopicSubscriptions` | ✅ |
| `maxInflightQos2` | `MaxInflightQoS2` | ⬜ v0.3 QoS2 |
| `connectionRateLimit` | `ConnectionRateLimit`（令牌桶） | ✅ |
| `metricsEnabled` | `Metrics` 注入即启用 | ✅ |
| `cluster.*` | `ClusterConfig`（占位） | ⬜ v0.5 |
| boss/work/business/dispatch/cluster 线程池 | 不适用（Go） | ❌ |

### 2.5 集群（jmqx-cluster）

与 Java 版对齐的目标：

- 节点发现：种子列表/gossip（Go 可用 Redis Pub/Sub、Kafka 或自研 SWIM；Go 生态无 ScaleCube 直接等价）；
- 会话路由：`clientId → node`；
- 订阅感知扩散：`topicFilter → nodes`，只有匹配节点收到 PUBLISH；
- 定向投递、KICK 广播、节点故障清理。

gmqx v0.2.0 仅定义 `ClusterConfig`/`ClusterRegistry` 接口；**v0.5** 实现（优先 Redis 作为路由/广播总线，其次考虑 gossip）。

### 2.6 客户端（jmqx-client）

gmqx 当前没有 client。计划 **v0.6** 提供最小可用客户端（3.1.1：Connect/Publish/Subscribe/Ping/Disconnect、自动重连、QoS0/1），API 采用 Go 风格（context + channel/回调），不追求 Reactor 三套 API；MQTT5 与背压后续。

## 3. 与 Java 版的设计差异（有意为之）

1. **并发模型**：Go 每连接 goroutine + 共享 map 锁；没有 Netty EventLoop/业务线程池，也没有 `requiresOffload` 的真实卸载（保留接口仅为 SPI 兼容）。
2. **配置**：Go Config struct + 环境变量/平台配置；无 DynamicLoader/ServiceLoader，模块通过显式注入组合。
3. **无持久化/无规则引擎**：与 jmqx 文档一致；离线消息后续仅内存队列。
4. **集群实现**：不直接复刻 ScaleCube；用 Go 生态的 Redis/Kafka 或轻量 gossip，接口保持 `ClusterRegistry` 语义。
5. **性能目标**：需自建 benchmark 对标 jmqx README 的压测口径（连接数/消息吞吐/P99），未达标前不做性能结论。

## 4. 路线与验收

| 阶段 | 内容 | 验收 |
|---|---|---|
| **v0.2.0（已发布）** | 配置/SPI 对齐（Auth/ACL/Dispatcher/Metrics）、ConnectMode、限额、指标、拦截器接口 | 现有 broker 测试 + ConnectMode/限额测试 |
| v0.3 | TLS/WS/WSS、MQTT5 握手与 ReasonCode、QoS2 完整、inflight 限额 | interop 测试（mosquitto/paho、浏览器 WS） |
| v0.4 | 会话恢复、离线队列、定向投递、拦截器链、MQTT5 Properties、ACL ReasonCode | 场景测试 + 压测 |
| v0.5 | ClusterRegistry 实现（Redis 路由/广播；订阅感知扩散；故障清理） | 双节点/三节点故障注入、KICK 广播 |
| v0.6 | client 库（3.1.1 最小集 + 重连） | 客户端互连/断线重连 |
| v1.0 | API 稳定、性能报告、与 jmqx 特性矩阵逐项签收 | 矩阵全绿或明确豁免 |

## 5. 当前不做

- 消息持久化（与 jmqx 一致）；
- 规则引擎（与 jmqx 一致）；
- 共享订阅（jmqx 亦不支持）；
- 动态 SPI 加载（Go 侧显式注入）；
- Netty 线程池/watermark 调参（Go runtime 调度）。

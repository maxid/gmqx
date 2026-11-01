[English](PERSISTENCE.md) | 简体中文

# 持久化与可用性

本文回答 broker 的确认语义立刻引出的两个运维问题：

1. **一次 QoS 1/2 的 PUBACK 承诺了什么？** broker 已接受该消息，并对"把它投递给每一个匹配
   的会话"负责。本文明确说明：这份责任里哪一部分能跨进程重启、跨节点丢失、跨存储故障存活，
   哪一部分不能。
2. **在保证上述语义的同时，部署如何保持可用？** 会话状态放进共享存储、集群走 gossip 之后，
   丢一个节点付出的是重连，而不是会话。下面说明原因与取舍。

接缝是 `SessionStore`，本包交付的实现是 Redis 版本；本文其余部分是契约、部署规则、另外两份
蓝图（SQL，以及缓存与会话存储的边界）、可用性模型与容量估算表。

## 必须跨重启存活的状态

**持久会话**是 broker 在连接之外唯一保留的东西。MQTT 3.1.1 用"清除 Clean Session"表达，
MQTT 5 用"清除 Clean Start + 非零 Session Expiry Interval"表达。没有连接持有它时，它的状态
位于 `SessionStore` 之后，而 `SessionState` 就是完整清单：

| 状态 | 字段 | 必须跨重启存活？ | 原因 |
|---|---|---|---|
| 订阅及其 QoS、No Local、Retain As Published、Retain Handling | `SessionState.Subscriptions` | **必须** | 恢复后的会话若丢掉过滤器，就会静默地什么都不投递。CONNACK 里的 `Session Present` 就是对它们的承诺。 |
| Subscription Identifier | `SessionState.Subscriptions[*].SubscriptionIdentifiers` | **必须** | 它们是订阅的状态而非连接的状态（MQTT 5 §3.8.3.1）：恢复的会话必须继续给投递打上它们。 |
| 会话离线期间排队的消息 | `SessionState.Queue` | **必须** | 它们已经对发布方确认过。这就是离线队列，在下次 CONNACK 之后按序排空。 |
| 未收到 PUBREL 的入站 QoS 2 packet id | `SessionState.InboundQoS2` | **必须** | 重连后重传其中之一的发布方必须得到同样的 PUBREC，且不会被二次投递（MQTT §4.4）。 |
| 未确认的出站 QoS 1/2 交换（含 packet id 与阶段） | `SessionState.Inflight` | **必须** | 恢复时带 DUP（PUBLISH）或作为 PUBREL、用原始 packet id 重发（MQTT §4.4）。 |
| 会话过期截止时间 | `SessionState.ExpiresAt` | **必须** | 它是状态的终点。丢掉它的存储会让会话永远存活。 |
| 连接本身：socket、读写协程、keepalive、Topic Alias | — | **不必** | 连接不是 broker 能搬运的状态；客户端会重连。 |
| inflight 窗口、作为结构的 pending 队列、packet id 分配 | — | **不必** | 窗口属于连接并会被重建。但窗口里的**消息**不会丢：持久会话的 pending 投递会在会话被存储之前并入 `SessionState.Queue`（见 [inflight 队列不是丢失点](#inflight-队列不是丢失点)）。 |
| 协商出的限制（Receive Maximum、Maximum Packet Size）、认证会话 | — | **不必** | 由下一次 CONNECT 重新协商。 |
| 保留消息 | `Broker.retained` | **不必，按设计保留至过期** | 保留存储是节点本地且进程内的；保留消息被保留到其 MQTT 5 Message Expiry Interval 到期或被清除。重启即丢失。 |
| 等待 Will Delay Interval 的延迟遗嘱 | `Broker.wills` | **不必** | `Stop` 会丢弃仍待发送的遗嘱：没有地方可发布，也没有定时器可以活过 broker。 |
| 集群成员与路由摘要 | 集群后端 | **不必** | 由后端重建（gossip 握手，或 Redis 里带 TTL 的记录）。 |

表背后的规则是：**客户端要求保留、且发布方被告知已接受的状态必须在存储里；属于连接或由节点
独占的状态则不必。** 凡是 broker 已对发布方确认过的消息，都不能只活在一个可能重启的进程内存里。

### inflight 队列不是丢失点

会占满连接 inflight 窗口的 QoS 1/2 投递会在该连接的 `pending` 队列（`MaxInflightQueueSize`）
中等待。当**持久**会话的 pending 队列溢出时，broker 不丢弃投递：它把会话转为离线——连接按
丢失连接的方式关闭，常规 unregister 路径存储会话，连接仍欠的一切（未确认的交换、剩余离线
队列、以及后面的 pending 投递）随之一起存储——下一次 CONNECT 再排空。该事件计为
`RecordOverflow("inflight_overflow")`，并以 Warn 级别、带 client id 与 reason
`inflight_overflow` 记录日志；drop 计数不变。

持久连接单纯消失（链路丢失、慢速消费者被断开）时同理：它的 pending 投递会在会话被存储之前
并入会话队列。

**clean** 会话没有存储条目也没有队列——它的状态随连接结束，这正是声明 clean 的含义——因此
在那条唯一的显式路径上仍然丢弃，并计为
`RecordDroppedMessage("inflight_overflow_clean")`。clean 会话用持久性换无状态：这是取舍，
不是缺陷。

持久会话唯一剩下的丢失点是离线队列自身的上限：`MaxOfflineQueueSize`（按会话，丢弃最旧）与
`MaxTotalOfflineMessages`（整个 broker，拒绝最新），分别计为 `offline_queue_full` 与
`offline_queue_total`。它们是容量限制，也正是[容量估算表](#容量估算表)要算的数字。

## 接缝

`SessionStore` 是四个方法和一个类型，刻意做得很小：

```go
type SessionStore interface {
	Load(key string) (SessionState, bool)
	Save(key string, state SessionState)
	Delete(key string)
	Keys() []string
}
```

* `Load` 返回 `key` 下的状态，或 `false`。无法解码的值按"没有会话"上报——绝不 panic，也绝不
  返回解码了一半的状态。
* `Save` 替换整个值。它在 broker 持有会话锁时被调用，因此单个节点不会让存储同时看到同一
  key 的两次 `Save`。
* `Delete` 删除 key；删除不存在的 key 是空操作。
* `Keys` 枚举已存储的会话。路由从不使用它：broker 在保存会话时就索引它的订阅，因此 publish
  永远不碰存储。`Keys` 只在两处使用：`New`（为部署在 broker 之前恢复进存储的会话建立索引）
  与过期清扫器（它必须枚举自己要执行的截止时间）。无法枚举自身会话的存储满足不了这个接缝。

broker 不会在存储内部回调自己，存储也绝不能回调 broker。`SessionState` 是交接而非共享：
存储必须返回调用方可以长期持有的状态，绝不能交出自己仍在写的 map 或 slice。排队消息的
payload 与属性**字节**是不可变的，共享它们没有问题。

### 原子取走

被多个节点共享的存储还需要一个操作，以可选接口表达：

```go
type sessionTaker interface {
	Take(key string) (SessionState, bool)
}
```

（`sessionTaker` 未导出，因为只有 broker 决定何时取走会话；方法名导出，因此包外的实现也能
提供它。）`Take` 必须在一个原子步骤里读取**并**删除。`Load` 后接 `Delete` 是两次往返，于是
两个节点同时重连同一个 client id 时可能都看到会话、都去服务它——一个 client id 两条活动连接、
重复投递、QoS 2 状态的两个方向各走一边。共享存储必须实现 `Take`；只有节点本地的存储才会退化为
`Load` + `Delete`，因为那种情形下 broker 自己的会话锁已经串行化了竞争。

`Take` 与 `Load` 都必须遵守会话过期截止时间：`ExpiresAt` 已过的会话绝不恢复，无论清扫器是否
已经删除它。broker 自己会检查截止时间，而能在存储侧执行它的实现（TTL，见下文）既省了清扫器
一趟扫描，也让过期会话在没有 broker 运行时同样不可达。

### SharedSessionStore

```go
type SharedSessionStore interface {
	SharedAcrossNodes() bool
}
```

broker 只在一个判断里用这个答案：被对端 kick 顶掉的连接可以对存储做什么。共享存储下，现在
持有该 client id 的对端同时拥有已存储的会话，因此被顶掉的节点既不能把自己的副本写回（那会
复活对端正在服务的会话，让两个节点服务一个会话），也不能删除该 key（那会擦掉对端保存的状态）。
节点本地存储下保留原有行为：被 kick 的会话状态留在原地，因为只有该节点能到达它。

### 后端实现必须满足的规则

1. **原子取走。** `Take` 一步完成读取与删除（Lua 脚本、事务、或 compare-and-delete）。没有它，
   两个节点可以同时拥有一个会话。
2. **按会话过期的每键 TTL。** `SessionState.ExpiresAt` 是截止时间而非提示：把它写为该 key 的
   TTL（或写成清扫器能执行的列）。截止时间已过的状态是删除而不是写入。没有截止时间的会话
   （MQTT 3.1.1，协议本身无 Session Expiry Interval）不带 TTL 写入，直到 broker 删除它。
3. **键带命名空间。** broker 传入的是 `namespaceKey(Config.Namespace, ClientID)`，已经带上
   限定，并期望存储在自己的前缀之后原样使用它。两个命名空间绝不能相撞；被两个不同命名空间的
   broker 共享的存储必须把它们的会话分开。`Keys` 返回 broker 传入时的键形式（不含存储自己的
   前缀），broker 会跳过其他命名空间的键。
4. **精确往返。** 取回的值必须与写入的值相同：每个订阅选项、每个 Subscription Identifier、
   每条排队消息的完整 payload 与 MQTT 5 属性块、入站 QoS 2 packet id、带 packet id 与阶段的
   出站交换、client id 与截止时间。请给存储格式加版本，以便把将来的字段与旧构建写入的值区分开。
5. **无法解码的值被丢弃并计数，绝不致命。** 无法解码的值被删除并计为
   `RecordDroppedMessage("session_store_corrupt")`，broker 继续服务其他会话。失败的命令计为
   `"session_store_error"` 并退化为"没有会话"——这是 at-most-once 行为，绝不 panic，也绝不
   因为存储而让 CONNACK 失败。
6. **调用有界。** broker 持有会话锁调用存储，因此每个操作都必须是有界往返。请配置连接超时与
   I/O 超时：不可达的存储必须付出有界等待，而不是无界等待。
7. **不回调 broker。** 存储是叶子。

### 自定义存储

接缝小到一页代码就能实现。下面这个就是内存参考形态——四个方法、可选的原子取走、以及复制规则；
把 map 换成你选用的客户端即可：

```go
package mypolicy

import (
	"sync"

	gmqx "github.com/maxid/gmqx"
)

// The store satisfies the seam, and provides the atomic take a shared store needs.
var _ interface {
	gmqx.SessionStore
	Take(string) (gmqx.SessionState, bool)
} = (*mapStore)(nil)

// mapStore keeps stored sessions in this process. It is node-local, so it must
// NOT report itself shared (SharedSessionStore): a peer's kick then leaves the
// stored session where it is.
type mapStore struct {
	mu       sync.Mutex
	sessions map[string]gmqx.SessionState
}

func (s *mapStore) Load(key string) (gmqx.SessionState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[key]
	if !ok {
		return gmqx.SessionState{}, false
	}
	return copyState(state), true
}

func (s *mapStore) Save(key string, state gmqx.SessionState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions == nil {
		s.sessions = make(map[string]gmqx.SessionState)
	}
	s.sessions[key] = copyState(state)
}

func (s *mapStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, key)
}

func (s *mapStore) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.sessions))
	for key := range s.sessions {
		keys = append(keys, key)
	}
	return keys
}

// Take hands one session to exactly one caller: the load and the delete are one
// critical section, which is the property a shared store has to provide across
// processes (a Lua script, a transaction or a compare-and-delete).
func (s *mapStore) Take(key string) (gmqx.SessionState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[key]
	if !ok {
		return gmqx.SessionState{}, false
	}
	delete(s.sessions, key)
	return copyState(state), true
}

// copyState detaches a state from the caller: the mutable maps and slices are new,
// the payload and property bytes are shared because nothing mutates them once a
// message is queued.
func copyState(state gmqx.SessionState) gmqx.SessionState {
	if len(state.Subscriptions) > 0 {
		subs := make(map[string]gmqx.SessionSubscription, len(state.Subscriptions))
		for filter, sub := range state.Subscriptions {
			sub.SubscriptionIdentifiers = append([]uint32(nil), sub.SubscriptionIdentifiers...)
			subs[filter] = sub
		}
		state.Subscriptions = subs
	}
	state.Queue = append([]gmqx.QueuedMessage(nil), state.Queue...)
	state.InboundQoS2 = append([]uint16(nil), state.InboundQoS2...)
	state.Inflight = append([]gmqx.SessionInflight(nil), state.Inflight...)
	return state
}
```

然后 `Config.SessionStore: &mapStore{}`。真正被多个进程共享的存储还要实现
`SharedAcrossNodes() bool { return true }`，并且它的 `Take` 必须在后端系统里原子，而不是靠
互斥锁。

## 蓝图 A —— Redis（已交付）

`NewRedisSessionStore(RedisSessionStoreConfig)` 是本包交付的实现；
`NewRedisClusterWithSessionStore(RedisClusterConfig)` 用一份配置同时构建它与 Redis 集群后端，
因此二者共用同一个地址、数据库与 `ChannelPrefix`：

```go
redisCluster, sessions, err := gmqx.NewRedisClusterWithSessionStore(gmqx.RedisClusterConfig{
	Addr:          "redis.internal:6379",
	Password:      os.Getenv("REDIS_PASSWORD"),
	ChannelPrefix: "gmqx:prod",
})
if err != nil {
	return err
}
defer sessions.Close()

broker, err := gmqx.New(gmqx.Config{
	NodeID:         "n1",
	Namespace:      "tenant-a",
	ClusterBackend: redisCluster,
	SessionStore:   sessions,
	SessionSweepInterval: 30 * time.Second,
	MaxOfflineQueueSize:  1000,
	MaxTotalOfflineMessages: 100000,
})
```

这个存储实现了可选接口：`Take` 是一次 Lua 脚本往返读删，`SharedAcrossNodes` 返回 true。存储
值是显式的、带版本的 JSON 记录（`{v, client, expires, subs, queue, in_qos2, inflight}`，键为
`<prefix>:session:<namespaceKey(ns, clientID)>`），因此 Go 字段改名不会改变存储布局。

### 推荐部署

| 关注点 | 建议 | 原因 |
|---|---|---|
| 持久性 | `appendonly yes`，`appendfsync everysec` | Redis 重启会重放 AOF，已确认的会话与排队消息随之回来。`everysec` 把丢失限定在约一秒的写入；`always` 让每次 `Save` 都付延迟；`no` 会在崩溃时丢掉 AOF 尾部。 |
| 复制 | 每个分片至少一个副本 | 会话状态的可用性取决于存储。单机 Redis 是**持久性**的单点，而不是投递的单点（见[可用性模型](#可用性模型)）。 |
| 故障切换 | 在配置的地址前放 sentinel 或 cluster 模式 | 存储对每个节点只是一个地址，自身不做 sentinel 发现。把懂 sentinel 的代理或懂 cluster 的客户端端点放在那里，让主节点迁移。 |
| 键前缀 | 每个环境与集群一个 `ChannelPrefix`，各节点一致 | 它同时隔离集群自身的键（`node`、`owner`、`member`、`subs`、`client`）与会话键（`session`），因此两个部署可以共用一个 Redis 而绝不互相寻址。 |
| 命名空间 | 每租户一个 `Config.Namespace`，或每租户一套 broker | 存储的键已包含 broker 的命名空间键，因此同一前缀内租户不会相撞。隔离仍是策略决定：一部署一命名空间最简单，多命名空间也受支持。 |
| TTL | 交给存储：它以 `ExpiresAt` 作为 key 的 TTL | 过期会话在没有 broker 运行时也会在截止时间从 Redis 消失。不要给这个 key 再套一个更短的、缓存式的 TTL：那等于数据丢失（见[蓝图 C](#蓝图-c--缓存库作为存储)）。 |
| 内存 | `会话数 ×（队列长度 × payload + 订阅 + 开销）`，再加余量 | 下文估算表把"每会话消息数"和"payload"变成数字。存储所在库请用 `maxmemory-policy noeviction`：会话键被淘汰就是静默数据丢失，而存储无法得知。 |
| 超时 | `DialTimeout`/`IOTimeout`（各默认 2 s） | broker 持有会话锁调用存储，不可达的 Redis 必须付出有界等待。 |
| 可观测 | 对 `session_store_error` 与 `session_store_corrupt` 告警 | `session_store_error` 上升说明会话正在被静默退化为"没有会话"；`session_store_corrupt` 说明存储格式与读取端的构建不一致。 |

### 它买到了什么

* **跨节点恢复。** 重连到集群任一节点的客户端从 Redis 取回会话：订阅、Subscription Identifier、
  离线队列、入站 QoS 2 packet id 与未确认的出站交换。CONNACK 报告 `Session Present`，恢复过程
  用原始 packet id 重发未确认的交换（MQTT §4.4）。
* **跨 broker 重启存活。** 重启的进程（发布、panic、OOM 被杀）在原处找到它的会话——包括它已
  确认并排队的消息，以及那些因 inflight 队列溢出而被转为离线的消息。
* **把"节点丢失"变成"重连"。** 在前面有复制与故障切换时，丢一个 broker 节点的代价是它的每个
  客户端各重连一次，而不是它们的会话。

### 失效模式

| 失效 | 会发生什么 | 看什么指标 |
|---|---|---|
| 节点启动时 Redis 不可达 | 配置了 Redis 集群后端时 `Start` 失败：它在声明成员身份前 ping Redis。未配置集群后端时 broker 仍会启动，而从第一条 CONNECT 起每次存储调用都失败。 | `Start` 的错误；随后是 `session_store_error` |
| 运行期 Redis 不可达 | 每次 `Load`/`Save`/`Delete`/`Take`/`Keys` 都失败、计为 `session_store_error`，并退化为"没有会话"：重连的客户端得到新会话（`Session Present` 为 false），该会话已排队的消息留在 Redis 里直到 TTL。集群后端持续重试；路由退化为本节点索引已知的部分。 | `session_store_error`、集群自身的 `redis unreachable` 警告 |
| Redis 丢失数据（没有持久化、卷被清、有延迟的副本被提升） | 丢失窗口内的会话没了：那些客户端得到新会话，存储尚未持久化的一切丢失。broker 在 Redis 最后一次持久化写入之后确认的消息随之丢失——这就是 AOF 设置是持久性边界的原因。 | Redis 自身指标；`Session Present` 为 false 的尖峰 |
| 值损坏或格式不兼容 | 该 key 被删除并计为 `session_store_corrupt`；客户端得到新会话，broker 继续服务。 | `session_store_corrupt` |
| 两个节点同时向同一个已存储会话追加 | `Save` 替换整条记录，而 broker 的锁只串行化单个节点，因此两条排队发布中的一条可能被覆盖（最后写入者胜）。离线会话的队列实际上由保存它的节点独占。 | 无法直接观测——缓解手段是下文的定向路由 |
| 过期 | key 的 TTL 在截止时间删除会话；每个节点的清扫器也执行自己命名空间的截止时间，CONNECT 也会检查截止时间。 | `session_expired` |

**第一部分对存储的要求边界。** inflight 溢出的交接是 broker 会话锁下对方寸会话状态的一次
`Save`，因此不需要新的原子操作：pending 投递在内存里并入状态（并在那里执行离线上限），随后
一次 `Save` 把会话存下。Redis 存储因此仍然只有一段 Lua 脚本——那次取走——Redis 里也不维护
任何计数器。另一条追加路径（`enqueueOffline`，即投递到达一个已存储的会话）依旧是
`Load` → 追加 → `Save`，并沿用存储文档里的单写者假设：如果部署需要两个节点同时向同一个离线
会话追加，那么存储需要一次原子追加（一段追加一条消息并按上限裁剪的 Lua 脚本），broker 也需要
把该会话的发布路由到同一个节点——这是有该需求的部署要做的改动，而不是本构建的承诺。

## 蓝图 B —— SQL 存储

当会话必须与运维方其余数据放在一起、且"broker 丢了什么"必须能用一条查询而不是一次 Redis 导出
来回答时，关系数据库是自然选择。这是**设计速写**，不是实现：不新增依赖，而 `SessionState`
已经包含了这些行所需的全部字段。

### 表结构

```sql
-- One row per stored session. The primary key is the key the broker passes in
-- (namespaceKey(Config.Namespace, clientID)), used verbatim.
CREATE TABLE mqtt_session (
    session_key   VARCHAR(255) NOT NULL PRIMARY KEY,
    client_id     VARCHAR(255) NOT NULL,
    namespace     VARCHAR(64)  NOT NULL,
    expires_at    TIMESTAMP(6) NULL,          -- NULL = MQTT 3.1.1: never expires
    updated_at    TIMESTAMP(6) NOT NULL,
    KEY idx_mqtt_session_expiry (expires_at)  -- the sweeper's scan
);

-- The subscriptions of a session, one row per filter.
CREATE TABLE mqtt_subscription (
    session_key             VARCHAR(255) NOT NULL,
    filter                  VARCHAR(512) NOT NULL,
    qos                     TINYINT      NOT NULL,
    no_local                BOOLEAN      NOT NULL,
    retain_as_published     BOOLEAN      NOT NULL,
    retain_handling         TINYINT      NOT NULL,
    subscription_ids        JSON         NULL, -- []uint32
    PRIMARY KEY (session_key, filter),
    FOREIGN KEY (session_key) REFERENCES mqtt_session (session_key) ON DELETE CASCADE
);

-- The queue: one row per message, in publication order. The auto-increment column
-- IS the FIFO order; nothing else may be used for it.
CREATE TABLE mqtt_queued_message (
    id           BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    session_key  VARCHAR(255) NOT NULL,
    topic        VARCHAR(512) NOT NULL,
    payload      BLOB         NOT NULL,
    qos          TINYINT      NOT NULL,
    retain       BOOLEAN      NOT NULL,
    props        BLOB         NULL,           -- MQTT 5 property block, no length prefix
    enqueued_at  TIMESTAMP(6) NOT NULL,
    KEY idx_mqtt_queue_session (session_key, id),
    FOREIGN KEY (session_key) REFERENCES mqtt_session (session_key) ON DELETE CASCADE
);

-- The unconfirmed outbound exchanges: they carry a packet id, so rows are the only
-- shape that can round-trip them.
CREATE TABLE mqtt_inflight (
    session_key  VARCHAR(255) NOT NULL,
    packet_id    SMALLINT UNSIGNED NOT NULL,
    topic        VARCHAR(512) NOT NULL,
    payload      BLOB         NOT NULL,
    qos          TINYINT      NOT NULL,
    retain       BOOLEAN      NOT NULL,
    props        BLOB         NULL,
    stage        TINYINT      NOT NULL,       -- 0 PUBACK, 1 PUBREC, 2 PUBCOMP
    PRIMARY KEY (session_key, packet_id),
    FOREIGN KEY (session_key) REFERENCES mqtt_session (session_key) ON DELETE CASCADE
);

-- Inbound QoS 2 packet ids waiting for their PUBREL.
CREATE TABLE mqtt_inbound_qos2 (
    session_key  VARCHAR(255) NOT NULL,
    packet_id    SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (session_key, packet_id),
    FOREIGN KEY (session_key) REFERENCES mqtt_session (session_key) ON DELETE CASCADE
);
```

### 原子取走

`Take` 必须在一个事务里读取并删除状态，否则两个节点会服务同一个会话。下面两种写法都正确，
第二种不需要交互式事务。

```sql
-- (a) Lock the row, read the children, delete the parent (children cascade).
BEGIN;
SELECT session_key, client_id, expires_at
  FROM mqtt_session
 WHERE session_key = ? AND (expires_at IS NULL OR expires_at > NOW(6))
   FOR UPDATE;
SELECT filter, qos, no_local, retain_as_published, retain_handling, subscription_ids
  FROM mqtt_subscription WHERE session_key = ?;
SELECT id, topic, payload, qos, retain, props
  FROM mqtt_queued_message WHERE session_key = ? ORDER BY id;
SELECT packet_id, topic, payload, qos, retain, props, stage
  FROM mqtt_inflight WHERE session_key = ?;
SELECT packet_id FROM mqtt_inbound_qos2 WHERE session_key = ?;
DELETE FROM mqtt_session WHERE session_key = ?;
COMMIT;
```

```sql
-- (b) Compare-and-delete: claim the row by moving a generation counter, then read
-- and delete. A loser sees rowcount 0 and reports "no session" without reading a
-- byte of state.
UPDATE mqtt_session
   SET generation = generation + 1, updated_at = NOW(6)
 WHERE session_key = ? AND generation = ?;      -- rowcount 0 => someone else took it
-- ... read the children of the claimed row, then delete it ...
```

`Save` 是镜像：在一个事务里 upsert 会话行，然后替换它的子行（`DELETE` + `INSERT`，或做差量）。
想省写入的存储可以只写变化的行，但整个状态必须原子提交——一个只有一半队列的会话比一个不存在
的会话更糟。

### 为什么队列应当是行而不是一个大 blob

离线队列是会话中最大的部分，也是唯一会在会话已存储时被追加的部分。做成行意味着：

* **追加不需要读改写。** 每条排队发布一次 `INSERT`（外加下面的裁剪），而不是读出一个可能上兆
  的 blob、追加、再写回——那既与任何其他写者竞争，又让每条消息产生整个队列的写放大；
* **裁剪走索引。** 每会话上限就是
  `DELETE FROM mqtt_queued_message WHERE session_key = ? ORDER BY id LIMIT ?`，
  这正是 `MaxOfflineQueueSize` 丢弃最旧策略的落点；
* **按序排空。** `SELECT ... ORDER BY id LIMIT n` 就是排空所需的 FIFO，部分排空只删掉已发送的
  那些行；
* **可观测。** "哪些会话持有的消息最多"就是
  `SELECT session_key, COUNT(*) ... GROUP BY session_key`，这正是运维方在定容量上限时想看的；
* **每行有界。** 单条 payload 不会把会话行顶出数据库的最大行长度。

每会话一个大 blob 只对本来就把整个会话放在内存里的存储成立（Redis 记录就是作为一个值写入的，
并沿用上文的单写者假设）。

### 给队列设上界

三个上界，都在存储侧执行，而不只在 broker 侧：

1. **按会话** —— 每次插入后，删掉超出 `MaxOfflineQueueSize` 的最旧行（上面的 `LIMIT` 删除），
   计为 `offline_queue_full`。
2. **整个 broker** —— broker 自己的 `MaxTotalOfflineMessages` 检查是快路径；被多个 broker 共享
   的存储还应执行一个总量，例如 `SELECT COUNT(*) FROM mqtt_queued_message`（有索引时是
   index-only 扫描，很便宜）或一张在同一事务里更新的计数表，并在超过上限时拒绝最新的插入
   （`offline_queue_total`）。
3. **过期** —— 会话的队列随会话删除（`ON DELETE CASCADE`），清扫器只需在过期索引上执行
   `DELETE FROM mqtt_session WHERE expires_at < NOW(6)` 一条语句。若希望在没有 broker 运行时也
   执行截止时间，用定时任务跑同一条语句即可。

## 蓝图 C —— 缓存库作为存储

`gcache` 不再是脚手架：它已实现并有测试覆盖，其中两个端口正是已存储会话所需要的。
`RemoteStore` 是按 key 寻址、带每键 TTL 的存储（`Get`/`Set`/`Delete`，已实现于 Redis 与内嵌
Olric 集群），`Locker` 是带"比对 token 再释放/续期"的租约。现成的适配器是
[`contrib/sessionstore-gcache`](../contrib/sessionstore-gcache)：它在 `RemoteStore` 之上
实现 `SessionStore`，用 gcache 可选的 `Scanner` 端口实现 `Keys`，用 `Locker.Acquire` 加
compare-and-delete 释放让 `Take` 原子（三次往返加一个锁 TTL），并声明自己是共享的。
`gmqx` 中没有任何文件导入 gcache：适配器是独立模块，不使用它的部署根本不会构建 gcache。

**不是**会话存储的是 `gcache.Cache`——建在这些端口之上的两级缓存。部署绝不能把它放在已存储
会话前面：它的本地层可能返回比另一节点刚写入的更旧的副本，它会在内存压力下淘汰，它把 TTL 收敛到
自己的上限，它可能在存储故障后返回过期条目（stale-if-error），它还会缓存"不存在"。以上每一条
都是设计上的数据丢失——对缓存数据可以接受，对会话是致命的。因此适配器直接读写 `RemoteStore`。
接缝的契约与缓存相反：写入的值在"被取走、被删除或过期"之前一直存在。

### 其他 KV 存储必须提供什么

像 Ignite（或其他任何键值系统）这样的存储，只有同时具备下列条件才适配这个接缝；选型时请逐条
核实：

1. **每键 TTL 或过期**，且查询能看到并执行它，从而让 `ExpiresAt` 是截止时间而不是建议。
2. **原子 compare-and-delete**（或效果相同的事务）用于 `Take`，保证一个会话只有一个属主。
3. **按键前缀扫描**用于 `Keys`，不加载 value，并且便宜到每轮清扫几百个会话也无压力。
   （在 gcache 里这是可选的 `Scanner` 端口；`redis.Store` 用 `SCAN` 实现。）
4. **在 broker 持有会话锁时仍低且可预测的延迟**——存储在 CONNECT 与 DISCONNECT 路径上被调用，
   因此 p99 50 ms 的存储会让重连变慢，而无界长尾会让它挂住。
5. **没有可能丢 key 的淘汰策略**（或容量大到不可能触发），以及节点丢失时的持久性故事：具备
   明确一致性模型的复制，或一个只追加日志。
6. **值字节精确往返**（payload 与属性块），没有编码意外。
7. **包含这些 key 的备份/恢复方案**——因为"会话在 KV 存储里"就是把备份义务转移给了该 KV 存储。

若某个候选方案回答不了第 1–3 条，它就是缓存；需要在它前面放蓝图 A 或 B。

## 可用性模型

把会话放进共享存储（蓝图 A 或 B），并运行集群后端（默认是 gossip；Redis 或 TCP 网格是替代
方案）。于是：

* **丢节点付出的是重连，不是会话。** 丢失节点的每个客户端都会重连，各自从共享存储取回会话
  ——订阅、队列、QoS 2 状态——并在它到达的任一节点上恢复。客户端感受到的是重连；它们保住的是
  会话。
* **吞吐靠加节点扩展。** 投递是按节点进行的：一条发布在本节点自己的订阅索引上匹配、由本节点
  的连接写出，而"哪个节点有匹配会话"的路由依据是对端交换的摘要（gossip 传播摘要，Redis 为每个
  节点维护带 TTL 的摘要）。因此一条发布的代价是一次本地匹配加一次集群分发，而不是穿过每个
  节点的扇出；加一个节点增加的是投递能力，而不是给每个对端增加工作。
* **分区中的集群仍能本地投递。** 与对端失联的节点继续服务它持有的会话：本地匹配、本地投递、
  本地排队。它失去的是跨节点投递与摘要的新鲜度，直到分区恢复；对另一侧 client id 的定向发布
  会落入被计数的 fire-and-forget 路径，而不是陷入热循环。

取舍照直说：

| 属性 | 代价 |
|---|---|
| 恢复延迟 | 存储只在 CONNECT（取走）、DISCONNECT（保存）与清扫器中被访问，**不在 publish 路径上**。发布永不触碰存储，因此共享存储的延迟表现为更慢的重连与断连，而不是更低的吞吐。 |
| **持久性**的可用性 | 存储是一个依赖：它不可达时，重连得到新会话，断连无法保存队列。投递本身继续工作。 |
| 集群分区 | 本地投递继续，但队列位于客户端不可达节点上的会话，在对端不会被恢复，直到分区恢复并从存储取走该会话。 |
| 一致性 | 两个节点向同一个**已存储**会话追加时可能丢一次追加（最后写入者胜）。一个 client id 同一时刻只有一条活动连接（连接时的 kick 保证），因此这只影响被顶替会话的狭窄窗口。 |
| 内存 | 队列上限就是内存边界。提高它们买到更长的持久窗口，代价是每个持有会话的节点的 RAM（以及存储字节）。 |

## 容量估算表

两个配置数字决定一次断连可以付出多少代价：`MaxOfflineQueueSize`（按会话）与
`MaxTotalOfflineMessages`（整个 broker）。它们是容量，不是延迟调参项：满了之后到达的消息会被
丢弃（`offline_queue_full` / `offline_queue_total`）。

**第 1 步 —— 持久窗口。** 客户端可以断连多久，broker 才开始丢弃它最旧的消息？

```
W = peak disconnect duration (seconds)
```

来源：你的重连策略与退避上限、客户端的 keepalive、部署的滚动升级/故障切换时间，以及移动或
现场设备可能离网多久。取你愿意承受的最坏情况，而不是平均值。

**第 2 步 —— 速率。** 单个会话的订阅在峰值时每秒携带多少条消息？

```
R = messages per second for one session at peak
N = W × R          # messages the session will accumulate
```

**第 3 步 —— 每会话上限。**

```
MaxOfflineQueueSize ≈ N × safety (1.5–2×)
```

例：一个客户端可能离网 120 s，其订阅每秒 5 条 → `N = 600` →
`MaxOfflineQueueSize ≈ 1000`（默认值）。离网 10 分钟、每秒 50 条 → `N = 30000`：要么提高上限
（并付出内存），要么接受它最旧的消息被丢弃，并对 `offline_queue_full` 告警。

**第 4 步 —— broker 级上限。** 同一时刻最多有多少会话处于离线？丢一个节点会让它的所有客户端
同时离线，因此要按这种情况算，而不是按平均值：

```
S = sessions per node × fraction offline at peak (0.1–1.0 for a node loss)
MaxTotalOfflineMessages ≈ S × MaxOfflineQueueSize × sharing (0.1–0.5)
```

sharing 系数表示每个会话的上限实际被用掉多少：只有流量大的会话队列才会满，因此总量等于
`S × MaxOfflineQueueSize` 是很少被触达的最坏情况。默认的 100000 约等于 100 个装满 1000 条消息
的会话。

**第 5 步 —— 内存与存储字节。**

```
payload ≈ message size (bytes) + ~64 B of framing (topic, QoS, retain, props, headers)
broker RAM ≈ MaxTotalOfflineMessages × payload × 1.3
store bytes ≈ the same, plus replication factor and AOF size
```

例：64 B payload、`MaxTotalOfflineMessages = 100000` → 约 100000 × 128 B × 1.3 ≈ 17 MB 的 broker
内存，Redis 里也大致相同（乘副本数，再加 AOF）。同样上限下 1 KiB 的 payload → 各约 140 MB。

**第 6 步 —— 存储自身的数字。**

| 问题 | 它决定什么 |
|---|---|
| `会话数 ×（队列 × payload + 订阅 + 开销）` | Redis `maxmemory`（配 `noeviction`）或数据库的表容量 |
| `Save` 速率 = 断连速率 + `enqueueOffline` 速率 | Redis ops/s 或数据库的写 IOPS。丢一个节点会在一个突发里产生每个断连会话一次 `Save`。 |
| `Take` 速率 = 重连速率 | 存储在 CONNECT 路径上的读延迟 |
| 每个 `SessionSweepInterval` 的 `Keys`/过期扫描 | Redis `SCAN` 开销，或数据库的过期索引扫描。让已存储会话数落在一次清扫能走完的范围内（broker 每轮限制 512 个已存储会话；更大的存储需要更多轮次，而不是更大的一轮）。 |

最后，盯住本文提到的计数器：`offline_queue_full`、`offline_queue_total`、`inflight_overflow`
（会话因 inflight 队列满而转离线——消费端停读时这是正常的，持续攀升才是问题）、
`inflight_overflow_clean`（clean 会话丢了投递）、`session_store_error` 与
`session_store_corrupt`。前两个是容量，后两个是存储。

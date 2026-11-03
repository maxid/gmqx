[English](PERSISTENCE.md) | 简体中文

# 持久化与可用性

本文回答三个问题：broker 在一条连接之外保留的状态里，**什么必须跨重启存活**、**它存在哪里**、以及
**把它变成共享状态时部署放弃了什么**。设计是**一个持久化接缝**同时承载两类状态——持久会话与保留
消息——所以部署选的是**后端**，而不是分别选"会话存储"和"保留消息存储"。

## 必须跨重启存活的状态

| 状态 | 进存储？ | 原因 |
|---|---|---|
| 订阅（QoS、No Local、Retain As Published、Retain Handling） | **是** | 恢复后忘了过滤器会静默不投递；CONNACK 的 `Session Present` 就是对它们的承诺。 |
| 订阅标识符 | **是** | 它是订阅的状态而不是连接的状态（MQTT 5 §3.8.3.1）：恢复的会话必须继续给投递打标。 |
| 离线期间排队的消息 | **是** | 它们已经向发布方确认过；这是离线队列，下一次 CONNACK 之后按顺序排空。 |
| 未收到 PUBREL 的入站 QoS 2 packet id | **是** | 重连后重传的发布方必须拿到同一个 PUBREC，且不被二次投递（MQTT §4.4）。 |
| 未确认的出站 QoS 1/2 exchange（packet id 与 stage） | **是** | 恢复时按原 packet id 重发（PUBLISH 置 DUP，PUBREL 照发）。 |
| 会话过期截止时间 | **是** | 它决定状态何时结束；丢失它会让会话永不过期。 |
| 保留消息（QoS、属性、Message Expiry） | **是** | 它是迟到订阅者收到的值；没有它，重启会静默忘掉每个主题的最后状态。 |
| 连接本身：socket、读写协程、keepalive、topic alias | — | 连接不是 broker 能搬走的状态；客户端会重连。 |
| inflight 窗口、pending 队列本身、packet id 分配 | — | 窗口属于连接、会被重建。**消息**不会丢：持久会话的 pending 投递在存储前并入队列。 |
| 协商限额（Receive Maximum、Maximum Packet Size）、认证会话 | — | 下一次 CONNECT 重新协商。 |
| 等待 Will Delay 的 will | — | `Stop` 丢弃未到期的 will：没有地方可发布，也不允许定时器活过 broker。 |
| 集群成员与路由摘要 | — | 由后端重建（gossip 握手，或 Redis 里带 TTL 的记录）。 |

规则：**客户端要求保留、且已向发布方确认接受的状态必须进存储；属于连接或按节点归属的状态不能进。**

## 接缝

一个字段：`Config.Store` 是 [`gcache.RemoteStore`](https://github.com/maxid/gcache)
（`Get`、`Set(ctx, key, value, ttl)`、`Delete`、`Close`），多节点共享时再加 `Config.Locker`。
`Config.KeyPrefix`（默认 `gmqx:`）给一个 broker 的键加命名空间；同进程的两个 broker 只要前缀（或
store 实例）不同，就可以共用一个后端。

```go
import (
    gmqx "github.com/maxid/gmqx"
    "github.com/maxid/gcache/memory"
)

broker, err := gmqx.New(gmqx.Config{
    Store: memory.NewStore(),   // 默认值：进程内，重启丢失
})
```

`Config.Store` 为 nil 时安装 `gcache/memory`：不配置的 broker 与早期版本的内存 broker 行为完全一致
——状态节点本地、重启丢失。这个默认值也是唯一没有外部依赖的 store。

### 键、TTL 与编码

| 键 | 值 | TTL |
|---|---|---|
| `<prefix>session:<broker 会话键>` | JSON 记录 v1：client id、订阅及标识符、队列、入站 QoS 2 id、出站 exchange（packet id、载荷、QoS、retain、属性块、stage）、截止时间 | 会话剩余寿命；不过期的会话没有 TTL |
| `<prefix>retained:<broker 保留键>` | JSON 记录 v1：namespace、topic、载荷、QoS、属性块、截止时间 | 剩余 Message Expiry；没有则为无 TTL |

broker 会话键是 `namespaceKey(Config.Namespace, ClientID)`，所以共用一个 store 的两个命名空间不会
相撞——与路由表、集群用的是同一套隔离。

TTL 是状态自己的截止时间，不是缓存寿命：后端在 broker 停止使用它的那一刻删除键。已经过期的状态
**删除而不是写入**。packet id 集合按升序写入，所以会话的存储形态可复现。

编码带版本。版本不认识的、解不出的值、不可能是 QoS 1/2 PUBLISH 的 exchange，都会被丢弃并计数
（`session_store_corrupt`、`retained_store_corrupt`、`offline_queue_corrupt`），而不是让 CONNECT
失败；命令失败计 `session_store_error`/`retained_store_error` 并按 miss 降级——与集群本身一样的
at-most-once。两种情况下 broker 都继续服务。

### 原子取走

重连会从 store 取走会话，两个节点不能都拿到。配置了 `Config.Locker` 时，取走是：对每键锁做
`TryAcquire`（gcache 的 `Locker`，锁 TTL 约束 get+delete 临界区）→ get → delete；拿不到锁的节点把
会话当作不存在，从空会话开始。没有 Locker 时是 Load 后 Delete，对"只有一个进程写"的 store 是正确的
——默认的进程内 store，或只属于一个节点的 store。

`Config.Locker` 同时是"该 store 跨节点共享"的信号：被 peer kick 顶替的连接既不会把会话副本写回
（会让 peer 正在服务的会话复活），也不会删键（会抹掉 peer 保存的状态）。没有 Locker 就是单写者，
沿用原有行为。

### 读回别的进程写下的状态

`gcache.Scanner` 是可选能力。store 实现 `Scan` 时，broker 在启动时载入上一个进程写下的状态：会话
索引进路由表成为离线目标，保留消息填进内存热表。没有 Scanner 就跳过载入——对没有别的进程写入的
store，这是正确的。

枚举**不在**过期路径上：路由索引保存了每个已存储会话的截止时间与队列条数，清扫器走索引；即使后端
提前驱逐了过期值，队列计数仍然正确。发布路径从不访问 store：路由由索引回答，重放由保留消息热表回答。

### 保留消息热表

broker 在内存里保存保留消息并写穿透到 store。SUBSCRIBE 扫描热表（不是 store），重放路径不付一次
往返；store 是跨重启、跨节点的那一份。重放遇到过期条目时从两处删除；即使没人订阅，后端的 TTL 也会
删掉它。

## 选择后端

| 后端 | 跨重启 | 跨节点共享 | 可枚举 | 说明 |
|---|---|---|---|---|
| `gcache/memory`（默认） | 否 | 否 | 否 | 零配置；与内存 broker 行为一致。 |
| `gcache/redis` | 是 | 是 | 是（`Scanner`） | 一套 Redis；键在 `Config.KeyPrefix` 下。多节点取走要配 `Config.Locker`。 |
| `gcache/olric` | 是 | 是 | 是（`Scanner`） | 读写、TTL 与枚举都可用；DMap 遍历要走遍每个分区并用正则过滤每个 key，启动载入是一次全表遍历。`Config.Locker` 需要你自己提供——gcache 只发布 Olric 的 store，不发布 locker。 |
| 自实现的 `gcache.RemoteStore` | 取决于你 | 取决于你 | 实现 `Scanner` 即可 | 满足端口的任何实现（SQL 表、文件、别的缓存）。 |

同进程的两个 broker 各拿各的 store——没有全局实例。与别的应用同进程嵌入 gmqx（或跑两个隔离域不同
的 broker）时，给各自一个 gcache 实例或各自的 `KeyPrefix`。

## 集群不会替你选后端

集群后端与持久化后端是两条轴，启用一条永远不会顺带启用另一条：

- `Cluster` / `ClusterBackend` 共享的是**路由与事件**——哪个节点有订阅者、踢人、保留消息发布；
  它不在节点之间搬运会话或保留消息；
- `Store` 决定这两类状态**是否共享**。不配置时仍是 `gcache/memory`，即节点本地——开不开集群
  都一样。

这是刻意的：由 `Config` 起出来的 broker 从不替调用方选 store。同进程的两个 broker、两个隔离域、
两个测试都必须能各拿各的 gcache 实例；隐式选一个还意味着由库来决定一个部署的数据落在哪里。所以
后果也是明写的：

- 开了集群但没配 `Store` 的部署，会话与保留消息**留在本节点**：客户端连到另一个节点会开新会话
  （没有 Session Present、订阅不恢复、没有离线队列），A 节点发布的保留消息在 B 节点看不见；
- 要让会话能在别的节点续上、保留消息全局可见，把 `Store` 配成 `gcache/redis` 或 `gcache/olric`，
  并配 `Locker`（不配就是 load-then-delete，只对"单节点写入"的 store 安全）。

`internal/engine/cluster_persistence_integration_test.go` 就是这句话两半的验收用例：真实两节点
集群、逐个后端跑（`go test -tags=integration ./internal/engine`）；同目录的 benchmark 给出共享
后端的代价。

### 取走会话的节点会告诉 peer

取走本身是不可见的：B 把 key 从 store 里读走，只有 B 知道。写过这条会话的 A 还留着离线路由项，
会继续把发布塞进一个再没人读的 key。所以取走会话的节点会广播 `ClusterSessionReleased`（只带
client id）。收到的一方**只有在自己 store 里也已经没有这条会话、且没有活连接持有该 client id 时**
才删离线路由——判断依据是 store 而不是某个标志位，因此节点本地 store（peer 各有一份）会保留自己
的路由。跑旧版本的 peer 会丢掉这个未知事件并留着陈旧项，与事件出现之前一致。

有一个后果值得知道：会话在节点间迁移本身就是 takeover，`Config.NotKickSeconds`（默认 30 秒）会
拒绝抢占一个建立不足该秒数的连接。设备正常漫游不会撞上（重连时旧连接早已消失）；故意迁移会话的
部署（负载均衡摘节点、测试驱动）可以设 `ConnectMode: gmqx.ConnectModeKick` 与
`NotKickSeconds: -1`，或者接受拒绝后重试。

### 两条值得知道的集群语义

两者都由"状态存在哪里"决定，不引入新机制就不会变：

- **节点崩溃会带走它上面所有连接的 Will。** Will 存在持有它的那条连接的内存里，所以"连接丢失时
  发遗嘱"的节点只能是当时服务它的那个节点。干净的 `Stop` 会发延迟已到的 will、丢弃还在等待的；
  崩溃的进程什么都不发。把 Will 复制到 peer 是一个新特性，不是修 bug。
- **`MaxTotalOfflineMessages` 是每节点上限。** `offlineQueued` 只统计本进程知道的部分——自己存的
  会话加上自己载入的会话——因此 N 个节点共享一个 store 时，实际允许的总量是配置值的 N 倍。每会话的
  `MaxOfflineQueueSize` 是精确的，因为它写在记录里。

### 一句话版本

集群部署要让会话能在别的节点续上，就配 `Store` 与 `Locker`；其余状态刻意留在节点本地。

## inflight 队列不是丢失点

会话的 pending 队列满时，broker 不丢弃投递：它把会话转为离线——像连接丢失那样关闭连接，正常
unregister 路径保存会话，连接还欠的一切（inflight 窗口中的消息、pending 队列）在存储前并入会话
队列。下一次 CONNECT 排空它。

**clean** 会话没有 store 条目也没有队列——状态随连接结束——所以溢出会以专属原因
（`inflight_overflow_clean`）丢弃投递。clean 会话用这个换durability。

持久会话仅剩的丢失点是离线队列自身的上限：`MaxOfflineQueueSize`（每会话，丢最旧）与
`MaxTotalOfflineMessages`（全局，拒最新），两者都有计数（`offline_queue_full`、`offline_queue_total`）。

## 可用性模型

- **单节点 + 进程内 store（默认）**：重启丢会话、队列与保留消息，与内存 broker 一直以来的行为一致。
- **单节点 + 共享后端**：重启后一切都在——会话恢复、保留消息重放；节点停机期间仍是单点。
- **多节点 + 共享后端**：连到另一个节点的客户端能恢复会话（原子取走），任何节点发布的保留消息在
  所有节点重放。节点需要后端，以及（取走与 kick 语义）一个 `Locker`；集群后端仍负责实时路由、
  presence 与定向投递。
- **多节点 + 无共享后端**：会话与保留消息节点本地——换节点重连从空开始；保留状态只靠集群的 retained
  事件复制（那不是持久化）。吞吐最高、耐久最弱。

## 容量估算

| 量 | 公式 | 默认 |
|---|---|---|
| 最坏排队消息数 | 会话数 × `MaxOfflineQueueSize` | 每会话 1000 |
| 最坏排队字节 | 消息数 ×（载荷 + 主题 + 属性块） | 取决于载荷 |
| 全局消息上限 | `MaxTotalOfflineMessages` | 100 000 |
| 保留消息数 | `MaxRetainMessages` | 0（不设上限） |
| 保留字节 | 消息数 ×（载荷 + 主题 + 属性） | 取决于载荷 |
| 每次 CONNECT 的 store 往返（持久） | 1 次取走（lock + get + delete）或 1 次 load | — |
| 每次断开的 store 往返（持久） | 1 次 save | — |
| 每次发布的 store 往返 | 路由 0 次；被保留或排队时 1 次 save | — |
| 每次 SUBSCRIBE 的 store 往返 | 0（热表回答） | — |

这些数字无法有界时应先约束队列上限，而不是指望 store 吸收无界队列。

## 这次替换了什么

`SessionStore` SPI、`MemorySessionStore`、`RedisSessionStore` 与
`contrib/sessionstore-gcache` 已删除：它们是同一个想法的三份实现，按特性而不是按后端切分。原先用
`RedisSessionStore` 的部署现在把 `gcache/redis` 的 store 作为 `Config.Store`、把 locker 作为
`Config.Locker`；原先用 contrib 模块的部署直接把已经构建好的同一个 gcache store 传进来。

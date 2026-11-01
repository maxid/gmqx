[English](README.md) | 简体中文

# sessionstore-gcache

基于 [`github.com/maxid/gcache`](https://github.com/maxid/gcache) 的现成
`gmqx.SessionStore`，让 gmqx 部署的多个节点把持久 MQTT 会话放在同一个共享 Redis 里：
重连到另一个 broker 的客户端恢复它的订阅、离线队列与 QoS 2 状态，而不是从零开始。

```go
import (
	gmqx "github.com/maxid/gmqx"
	"github.com/maxid/gcache/redis"
	gcachestore "github.com/maxid/gmqx/contrib/sessionstore-gcache"
)

remote, _ := redis.NewStore(client)          // 基于 go-redis 的 gcache.RemoteStore
locker, _ := redis.NewLocker(client, "gmqx:fencing")
sessions, err := gcachestore.New(gcachestore.Config{
	Store:  remote,
	Locker: locker,
	Prefix: "gmqx:session:",
})
if err != nil {
	return err // 无法枚举的存储、或缺少 Locker：在这里就被拒绝
}
defer client.Close()

broker, err := gmqx.New(gmqx.Config{
	SessionStore: sessions,
	// ...
})
```

## 为什么是独立模块

`contrib/sessionstore-gcache` 是一个独立的 Go 模块
（`github.com/maxid/gmqx/contrib/sessionstore-gcache`），它把
`github.com/maxid/gmqx` 替换为 `../..`、把 `github.com/maxid/gcache` 替换为
`../../../gcache`。因此核心 broker 从不依赖 gcache 或 go-redis：不需要这个存储的部署
两者都不构建，库自身的 `go.mod` 保持原样。`github.com/maxid/gmqx` 中没有任何文件导入本包。

## 它实现了什么

| gmqx 接缝 | 实现方式 |
|---|---|
| `Load(key)` | `RemoteStore.Get` + 带版本的 JSON 解码。已过截止时间的值被删除并计数。 |
| `Save(key, state)` | 编码后 `RemoteStore.Set`，key 的 TTL 就是会话自己的截止时间。截止时间已过的状态被删除而不是写入。 |
| `Delete(key)` | `RemoteStore.Delete`。 |
| `Keys()` | 用 `gcache.Scanner.Scan(prefix, ...)` 扫描本存储的 key 前缀，返回时去掉前缀。 |
| `Take(key)` | 下文的原子取走。 |
| `SharedAcrossNodes()` | 恒为 `true`：存储是共享的，所以被对端顶掉的连接不得把自己的副本写回。 |

`New` 会在构造时拒绝三种配置，而不是等到某次重连才失败：

| 错误 | 何时返回 |
|---|---|
| `ErrStoreRequired` | 未配置 `RemoteStore` |
| `ErrEnumerationUnsupported` | 存储未实现 `gcache.Scanner`。`Keys` 有两处必需：`gmqx.New` 用它索引已存储的会话，使发给离线会话的发布进入队列而不是被丢弃；过期清扫器用它枚举截止时间。 |
| `ErrLockerRequired` | 未配置 `Locker`。该存储声明自己是共享的，而共享存储没有 `Take` 就可能把一个会话交给两个节点。 |
| `ErrInvalidPrefix` | 前缀包含 SCAN 通配字符，会让 `Keys` 扫描到本存储不拥有的 key。 |

## 取走：三次往返与一个锁 TTL

取走必须删除已存储的会话并把它只交给一个调用方：

1. 用 `Locker.Acquire` **获取**会话 key 的锁（`SET key token NX PX ttl`，在 `LockWait`
   内重试）；
2. 持锁期间用 `RemoteStore.Get` **读取**值；
3. 仍然持锁时用 `RemoteStore.Delete` **删除**它。

随后用 `Lock.Unlock` 释放锁，也就是 locker 的"比对 token 再删除"（过期后被别的节点接管的锁
绝不会被错误的属主释放）。这是第四次往返；上面三次是取走的关键路径，也是它的成本边界。

在步骤 2、3 期间一直持锁，才让"读"与"删"成为同一个临界区：第二个节点无法在两者之间读到
该 key 再删一次，因此两个节点不可能同时恢复同一个会话。

**锁 TTL。** `LockTTL`（默认 5 s）限定临界区且不续期，所以它必须大于存储 `Get` + `Delete`
的 p99。若一次停顿超过它，保证会从 exactly-once 降级为 at-most-once：释放仍然是
compare-and-delete，过期锁不会被错误的属主释放，但第二个节点可能已进入同一临界区，两者都
可能交出同一份状态。Redis 停顿数秒本就是 Redis 的问题；请把 TTL 调到正常运行时租约不会过期的
长度。

**如果锁拿不到**，说明另一个节点（或同一 client id 的另一条 CONNECT）正在取走该会话。
`Take` 最多等待 `LockWait`（默认 500 ms），然后返回 `(SessionState{}, false)`——调用方开始
一个新会话，而不是阻塞或让 CONNECT 失败。此时不会删除任何东西，会话留在存储里，事件计为
`session_take_contended`。存储或锁命令失败计为 `session_store_error`，并以同样方式降级
（at-most-once，绝不 panic）。

`Take` 不额外往返就处理了其余规则：key 不存在即"没有会话"；无法解码的值被删除并计为
`session_store_corrupt`；超过截止时间的值被删除并计为 `session_expired`。

## L1 注意事项：不要在外面套 `gcache.Cache`

适配器直接使用 `gcache.RemoteStore`，绝不在它前面放两级 `gcache.Cache`。这是刻意的，
部署也不应这么做：

- `gcache.Cache` 的读可能由**本地层（L1）**作答，而它可能是比另一节点刚写入的更旧的副本
  ——失效是尽力而为的 pub/sub，丢一条消息就留下一个陈旧本地值；
- L1 会在内存压力下**淘汰**且容量有界；被淘汰的会话就是悄悄消失的会话；
- `Cache` 会把 TTL **收敛**到它的 `MaxTTL` 并套用默认 TTL，而会话的 `ExpiresAt` 是截止
  时间，不是新鲜度提示；
- `stale-if-error` 可能在存储故障后返回过期条目，对会话而言就是两个节点、两段历史；
- 负缓存记住"这个 key 不存在"，正是存储 `Load` 返回 `false` 所必须表达之意的反面。

`RemoteStore` 是 gcache 中行为像存储的那部分：写入的值在"被覆盖、被删除或过期"之前一直存在。
这正是会话需要的契约。

## 存储布局

会话 key 写为 `<Prefix><key>`，其中 `<key>` 就是 broker 传入的原样（已带命名空间限定）。
取走锁位于 `take-lock:<Prefix><key>`，在数据前缀之外，因此 `Keys` 永远不会返回锁，
`"<Prefix>*"` 也永远不会匹配到锁。

值是带版本的 JSON 记录（`{"v":1,...}`），字段名与版本和 gmqx 自带的 `RedisSessionStore`
相同，因此部署在两者之间迁移时，一方写入的值另一方可以读取（只有 key 前缀不同）。payload 与
MQTT 5 属性字节在 JSON 里是 base64，逐字节往返（包括 NUL）；nil 与空集合保持区别。其他版本的
记录按无法解码处理：丢弃并计数。

`Keys` 通过 `gcache.Scanner` 分页扫描 `<Prefix>*`，大存储不会在一次回复里全部返回。其他
命名空间的 key 由 gmqx 自己过滤；扫描本身已经限定在本存储的前缀内。

## 配置

| 字段 | 默认值 | 含义 |
|---|---|---|
| `Store` | — | `gcache.RemoteStore`；必需，且必须实现 `gcache.Scanner` |
| `Locker` | — | `gcache.Locker`；必需 |
| `Prefix` | `gmqx:session:` | 本存储的 key 命名空间；不得含 SCAN 通配字符 |
| `LockTTL` | 5 s | 锁租约，同时是取走临界区的上界 |
| `LockWait` | 超时 500 ms | 取走重试锁多久后报告"已被取走" |
| `Timeout` | 2 s | 单次存储操作，以及整个 `Keys` 遍历 |
| `Metrics` | nil | 通过 `RecordDroppedMessage` 接收 `session_store_corrupt`、`session_store_error`、`session_expired`、`session_take_contended` |
| `Logger` | `slog.Default()` | 命令、解码与锁失败 |

broker 在自己的会话锁下调用存储，因此每个操作都受 `Timeout` 约束；不可达的 Redis 付出的是
有界等待，而不是挂住。

## 部署

这个存储的持久性等于它背后 Redis 的持久性。真实部署至少需要：

```sh
docker run --name gmqx-redis -p 6379:6379 -v gmqx-redis:/data redis:7 \
  redis-server --appendonly yes --appendfsync everysec \
               --maxmemory-policy noeviction
```

`noeviction` 不是可选项：被淘汰的会话 key 就是一个悄悄消失的会话，broker 甚至无法知道发生了。
`appendonly yes` 让已确认的会话挺过 Redis 重启；`everysec` 把丢失限定在约一秒的写入。每个分片
再加一个副本（或在地址前放 Sentinel/cluster 端点）以获得可用性，并按"会话数 × 队列长度"估算
`maxmemory`。

完整指导——AOF 与复制的取舍、sentinel/cluster 故障切换、key 前缀、命名空间、内存估算以及
失效模式表——见 gmqx 的 [docs/PERSISTENCE.zh-CN.md](../../docs/PERSISTENCE.zh-CN.md)。
适配器自身的失效模式就是那张表的 Redis 行：指标上的 `session_store_error`、at-most-once 降级、
绝不 panic。

## 测试

测试跑在 miniredis 和真实 broker 上，覆盖跨节点恢复（持久客户端在 broker A 订阅、断开、
离线期间发布消息、再用同一个存储重连到 broker B，带着 `sessionPresent` 按序收到全部消息）、
clean 连接丢弃共享会话、两个 broker 争抢同一个会话、会话过期即 key 的 TTL、损坏值，以及
限定命名空间的 `Keys`：

```sh
cd libs/gmqx/contrib/sessionstore-gcache
go test ./...
go test -race ./...
go vet ./...
```

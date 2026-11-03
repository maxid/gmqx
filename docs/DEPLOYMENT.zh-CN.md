[English](DEPLOYMENT.md) | 简体中文

# 部署

怎么跑一个 broker 集群、要配什么。每个开关背后的契约在 [PERSISTENCE.zh-CN.md](PERSISTENCE.zh-CN.md)；
这一页是配方。

## 三节点集群

每个节点跑同一个二进制、给同一份 seed 列表；列表里包含节点自己的集群地址，节点只拨"地址排序在自己之后"
的 seed，因此对称的列表恰好得到每对一条连接。

```sh
# 节点 a
gmqx -node a -addr 0.0.0.0:1883 -cluster-addr 10.0.0.1:7901 \
     -seeds 10.0.0.1:7901,10.0.0.2:7901,10.0.0.3:7901
```

`examples/cluster-node` 就是同一套配方的可运行版本，`examples/README.zh-CN.md` 演示两个节点并排启动。

三个容易踩的点：

- **seed 列表必须对称、且包含每个节点**（包括自己）。只列自己的节点永远入不了群。
- **`cluster-addr` 必须显式**，绝不能用 `:0`：peer 需要一个稳定地址，临时端口不是选项。
- **集群成员不是会话状态。** 开集群只共享路由并复制保留消息，不会让会话能在别的节点续上；那是
  `Config.Store` 的事（见下），`docs/PERSISTENCE.zh-CN.md` 解释了为什么 broker 从不自己选 store。

## 持久化

| 后端 | 什么时候用 | 配什么 |
|---|---|---|
| `gcache/memory`（默认） | 单节点，或"设备重连到别的节点就丢会话"可接受的集群 | 什么都不用配 |
| `gcache/redis` | 已经有 Redis，且要会话在节点间迁移 | `gcache/redis` 的 `Store` + `Locker` |
| `gcache/olric` | 不想用 Redis，持久化跟着 broker 走 | `gcache/olric` 的 `Store` + `Locker` |

共享 store 不配 `Locker` 时，取会话是 load-then-delete，只对"单节点写入"的 store 安全。两个随包后端
都带 locker；`gcache/olric` 的是分区属主上的单键锁，不是 Redlock 的多数派——见它的 README。

Olric 有两种形态，代码完全一样：**嵌入式**节点跑在 broker 进程里（不增加部署单元），**客户端**形态
连你运维的集群（可独立伸缩与升级）。一个部署只选一种，不要混用，否则两个 tier 会各存一份会话。

## 连接抢占

`NotKickSeconds`（默认 30）会拒绝抢占一个建立不足该秒数的连接：同一个 client id 的两端不能互相
反复抢占。设备正常漫游撞不上——重连时旧连接早已消失；但故意迁移会话的负载均衡会。这种部署设
`ConnectMode: ConnectModeKick` 与 `NotKickSeconds: -1`，或者接受拒绝（MQTT 3.1.1 返回码 3、
MQTT 5 原因码 0x9F）后重试。

## 监听器与限制

- TCP `Addr`、TLS `SecureAddr` + `TLS`、WebSocket `WebsocketAddr` + `WebsocketPath`、
  WSS `WebsocketSecureAddr`。`PortFallback`（默认 16）让绑定在端口被占用时试下一个端口；`Addrs()`
  报告真正绑上的地址，读它，别假设。
- `MaxConnections`、`ConnectionRateLimit`、`NotKickSeconds`、`ConnectTimeout`、
  `AuthTimeout`、`MaxPacketSize` 与字节速率整形是把恶意 peer 关在笼子里的旋钮。唯一会把慢消费者
  变成故障的是不设上限的队列：`MaxOfflineQueueSize` 与 `MaxTotalOfflineMessages` 必须设。
- `MaxTotalOfflineMessages` 是按节点计数的，不是跨集群（见 PERSISTENCE.zh-CN.md）。

## 可观测性

`Metrics` 是钩子而不是端点：broker 调用它，部署方决定数字去哪（Prometheus、OTLP、日志）。`Sink` 对
发布同理，`Logger`（`*slog.Logger`）承接成员、抢占与 store 的决定。没有 `$SYS` 树，也没有内置健康
端点——两者都只差一个钩子，而一个擅自开端口的库更糟。

## 升级

滚动升级两个方向都安全：不认识某个集群事件类型的 peer 会丢弃该帧继续跑（`ClusterSessionReleased`
与定向发布的降级行为就是这么来的）。**没有**版本化的是 store 的编码：不同构建的两个节点可能对一条
记录的版本理解不同，而本构建不认识的版本会被丢弃、不会被误读。让写 store 的节点一起升级，或在重大
变更之间清空 store。

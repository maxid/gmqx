# 互操作矩阵

本目录是 gmqx 的 MQTT broker 与 client 包的互操作矩阵。它用可复现的实验回答一个
问题，而不是靠推断：**broker 和 client 能否与它们之外的实现互通？**

矩阵包含三个方向：

| 方向 | 被测对象 | 参考实现 | 用例 |
|---|---|---|---|
| paho → broker | 进程内的 gmqx broker，监听临时端口 | [`github.com/eclipse/paho.mqtt.golang`](https://github.com/eclipse/paho.mqtt.golang)，第三方 MQTT 客户端 | QoS 0/1/2 发布/订阅往返（QoS 2 断言只投递一次）、后连接的订阅者收到保留消息、客户端 socket 被强杀时发布遗嘱而正常 `DISCONNECT` 时不发布、持久会话重连后收到离线期间排队的消息、256 KiB 载荷往返、超过 broker 最大报文长度的发布被拒绝且客户端看到断连 |
| client → mosquitto | gmqx 的 `client` 包 | 容器中运行的 eclipse-mosquitto 2 | MQTT 3.1.1 **和** MQTT 5 两个版本上的 QoS 0/1/2 往返、后连接的订阅者收到保留消息、socket 被强杀时发布遗嘱而正常关闭时不发布 |
| 参考客户端 → broker | 宿主机上的 gmqx broker | eclipse-mosquitto 镜像里的 `mosquitto_pub` / `mosquitto_sub`，通过 `docker exec` 运行 | 参考客户端的 QoS 2 发布到达宿主机订阅者、宿主机发布到达参考订阅者、参考客户端发布的保留消息回放给后连接的宿主机订阅者 |

## 为什么是独立模块

`interop/` 是一个独立的 Go module（`github.com/maxid/gmqx/interop`），它把
`github.com/maxid/gmqx` 替换为紧邻其上的源码树（`replace github.com/maxid/gmqx => ..`）。
因此第三方客户端和 docker 相关工具不会进入库本身的 `go.mod`：库保持零第三方依赖，
矩阵可以增加一个实现而不改变库自身的契约。本目录里的任何代码都不会被库 import。

## 如何运行

```sh
cd libs/gmqx/interop

go test ./...                  # 运行整个矩阵
go test -v -count=1 ./...      # 逐用例输出，可以看到哪些运行、哪些跳过
go test -run TestPaho -v ./... # 只跑第三方客户端用例，不需要 docker
go test -run 'Mosquitto|Reference' -v ./...  # 只跑容器相关用例
```

环境要求：

- Go 1.27。
- Docker，仅用于 mosquitto 相关用例。镜像 `eclipse-mosquitto:2` 按需拉取（拉取有
  三分钟上限，每个镜像只会拉一次），每个容器在测试结束时删除，宿主机端口在运行时从
  空闲的回环端口里挑选。除此之外无需安装任何东西：参考 **broker** 和参考
  **客户端** 都来自该镜像。
- paho 用例不需要 docker：它们在临时端口上启动进程内的 broker。

镜像已存在时整个矩阵几秒内结束；冷启动拉取镜像是唯一较慢的步骤。所有等待都有上限，
运行不会挂死：就绪探测有截止时间（容器 30 秒、握手 10 秒），消息等待 10 秒超时，
docker 命令会被 context 终止，参考订阅客户端自身在 20 秒后退出。

## 哪些情况会跳过，以及原因

这套用例在任何机器上都可以安全运行：缺少前提条件时用例会带着原因跳过，而不会让整轮
运行失败。

| 情况 | 结果 |
|---|---|
| 未安装 docker 或 daemon 未运行 | `TestClientAgainstMosquitto` 与 `TestBrokerAgainstReferenceClients` 跳过："docker is unavailable, so the reference broker and its clients are skipped: ..." |
| 无法拉取 `eclipse-mosquitto:2` 镜像（无网络） | 同样两个测试跳过："the eclipse-mosquitto image cannot be pulled ..." |
| 容器已启动，但 30 秒内没有接受 TCP 连接或没有回应 MQTT `CONNECT` | 用例跳过，并打印容器日志的最后若干行 |
| 第三方客户端模块无法构建（离线机器且模块缓存为空） | 用 `go test -tags interop_without_paho ./...` 即可在不带该依赖的情况下构建，并把六个 paho 用例报告为跳过并附原因。默认构建会编译 paho，因此需要该模块；仓库已提交 `go.sum` |

paho 用例始终逐条报告，因此 `-v` 输出会明确写出哪些运行了、哪些跳过了。

## 用例为什么是确定性的

- broker 在进程内监听临时端口；容器里的 broker 用一次真实的 MQTT 握手探测
  （端口转发接受了 TCP 连接并不代表 mosquitto 已经在监听）。
- 持久会话用例不靠 sleep：测试把自己实现的 `SessionStore` 传给 broker，轮询到会话已
  保存、离线队列长度符合预期后再重连。
- 方向 B 的订阅者通过 broker 自己的 `MetricsManager`（`IncrementSubscribeEvents`）
  确认，而不是读取参考客户端的 stdout —— 后者的输出在管道上是块缓冲的，进程退出前不会
  到达测试。
- 遗嘱通过复位客户端 socket（`SO_LINGER` 0）并在客户端背后关闭来触发，因此不会发送
  `DISCONNECT`；正常关闭的用例则调用客户端自己的关闭接口。两者唯一的差别就在这里。
- “不应收到消息”的断言监听固定的静默窗口；两个正常关闭的遗嘱用例还会在之后发送一条
  对照消息，因此订阅者已经失效不会让否定断言误通过。

## 文件

| 文件 | 内容 |
|---|---|
| `go.mod`、`go.sum` | 矩阵模块：库（用 `..` 替换）、paho 以及它们的校验和 |
| `helpers_test.go` | broker 启动、有界等待、投递记录器、socket 强杀、gmqx client 辅助函数 |
| `paho_test.go` | paho → gmqx broker 的用例 |
| `paho_skip_test.go` | 在 `interop_without_paho` 构建标签下把同样的用例报告为跳过 |
| `mosquitto_test.go` | 容器测试框架，以及 gmqx client → mosquitto、参考客户端 → gmqx broker 的用例 |

容器名以 `gmqx-interop-` 开头；运行结束后 `docker ps -a` 里不应出现它们。

## 本矩阵不覆盖的内容

TLS、WebSocket 与 WSS 监听、认证与 ACL、集群、MQTT 3.1（`MQIsdp`）、共享订阅与订阅
标识符、broker 重启后仍存活的会话过期，以及吞吐与时延。这些属于库自身的测试套件或压测
基线，而不是互操作门槛。

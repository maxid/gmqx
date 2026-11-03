[English](README.md) | 简体中文

# 示例

四个可直接运行的程序，每个回答一个问题。每个都是自包含的 `main` 包，在仓库根目录
`go run ./examples/<name>` 即可，不需要第三方 MQTT 服务器，也不需要配置文件。

- [broker-demo](#broker-demo) —— 在自己的进程里嵌一个 broker 有多简单、保留消息对
  "后连上来的订阅者"意味着什么，以及怎么用 `-serve` 把它留给真正的 MQTT 客户端。
- [client-demo](#client-demo) —— 怎么用 `github.com/maxid/gmqx/client`。
- [cluster-node](#cluster-node) —— 怎么跑一个加入集群的节点。
- [plugin-codec](#plugin-codec) —— 应用侧插件怎么注册 codec 并转码载荷。

## 统一约定

- 每个步骤一行 —— `[n/N] 动作 … 结果` —— 结束时一行 `[done] …`。`cluster-node` 是例外：
  它是长期运行的节点，日志走 `slog`。`broker-demo -serve` 是另一种长期运行模式：
  它跳过脚本步骤，只把 broker 挂着。
- 出错打到 stderr，格式 `<示例名>: <动作>: <错误>`，退出码非 0。
- Ctrl-C 都能停：示例打印 `[stop] interrupted`，`cluster-node` 打印
  `[stop] node <id> stopped`，`broker-demo -serve` 打印 `[stop] broker stopped`，
  退出码都是 0 —— 示例不是服务。
- `-h` 打印这个示例回答什么问题，外加可以直接复制的命令。
- broker 自己的日志保持安静（示例把 logger 设成 `warn` 级别，进度自己打印）；
  想看 broker 的调试日志用 `cluster-node -v`。

## broker-demo

**回答：** 嵌入一个 broker 要多少代码，以及保留消息为什么能活过发布者。示例在本进程内起
一个 broker，用库自带的 client 发一条 **retained** 消息，然后让**第二个** client 才连上来、
才订阅：最后一步打印出来的内容全部来自保留消息重放。

```sh
go run ./examples/broker-demo
go run ./examples/broker-demo -topic demo/retained -message "hello from gmqx"
go run ./examples/broker-demo -addr 127.0.0.1:1883 -qos 0 -version 4
go run ./examples/broker-demo -serve                        # 把 broker 留给 MQTTX
```

```
[1/6] broker listening on 127.0.0.1:60653
[2/6] publisher "broker-demo-pub" connected (MQTT level 5)
[3/6] published retained "hello from the gmqx broker demo" to demo/retained (QoS 1)
[4/6] subscriber "broker-demo-sub" subscribed to demo/retained and demo/retained_reply (granted [1 0])
[5/6] replayed demo/retained to a subscriber that connected later (QoS 1, retain true): hello from the gmqx broker demo
[6/6] the broker's own publish reached demo/retained_reply: hi!
[done] retained message replayed to a later subscriber; broker stopped
```

第 2、4 步还演示了另一半：broker 有个 `EventSink`，看到每个被接受的连接就回一条自己的问候——
`<topic>_reply` 上一条 **retained** 的 `hi!`，通过 `Broker.PublishMessage` 注入，再由 `Dispatcher`
像普通发布一样路由。之所以 retained：`OnConnect` 触发时客户端还没订阅任何东西，普通发布无人可送。

脚本演示里 `-addr` 默认 `127.0.0.1:0`，端口由系统分配，所以不会撞上你本机已有的 broker。
客户端本身的细节（QoS 2、重连、MQTT 3.1.1 与 5 的区别）属于 `client-demo` 的话题。

### 留给真正的 MQTT 客户端连

脚本演示跑完就退出，MQTTX 没有进程可连。`-serve` 跳过演示、只把 broker 挂着：

```sh
go run ./examples/broker-demo -serve                        # 127.0.0.1:1883
go run ./examples/broker-demo -serve -addr 127.0.0.1:1884   # 指定端口
```

```
[serve] broker listening on 127.0.0.1:1883
[serve] every accepted connection gets a retained "hi!" on demo/retained_reply
[serve] connect an MQTT client to it; Ctrl-C stops the broker
```

把 MQTTX（或 `mosquitto_pub`、另一个 `gmqx` 客户端）指向这个地址即可。broker 是空的：
没有脚本客户端，也没有预置的 retained 消息。但 `EventSink` 问候照常生效——每个被接受的
连接都会触发 `<topic>_reply` 上一条 retained `hi!`（`-topic` 决定这个前缀），外部客户端
订阅过去就能看到 broker 自己的发布。

`127.0.0.1:1883` 也是 `cluster-node` 的默认端口，所以很可能已被占用：示例会打印并顺延到
下一个空闲端口，显式传 `-addr` 也一样处理。Ctrl-C 排空后打印 `[stop] broker stopped`，
退出码 0。

## client-demo

**回答：** 用 `github.com/maxid/gmqx/client` 订阅、发布、收消息是什么样子。不给 `-addr`
时它自己起一个进程内 broker，然后跑一次往返并打印收到的消息。

```sh
go run ./examples/client-demo
go run ./examples/client-demo -topic demo/hello -message "hello" -qos 2 -version 5
go run ./examples/client-demo -version 3      # MQTT 3.1
go run ./examples/client-demo -addr 127.0.0.1:1883 -topic demo/hello
```

```
[1/5] in-process broker listening on 127.0.0.1:61027
[2/5] connected as "client-demo" (MQTT level 4)
[3/5] subscribed to demo/hello (granted [1])
[4/5] published "hello from the gmqx MQTT client" to demo/hello (QoS 1)
[5/5] received "hello from the gmqx MQTT client" on demo/hello (QoS 1, retain false)
[done] publish/subscribe round trip complete; client closed
```

第 3 步的 granted 是**服务端授予**的 QoS，不是你请求的：`-qos 2` 遇到只授予 1 的服务端会显示
`granted [1]`。`-version 3` 用 MQTT 3.1 跑同一趟往返。

handler 是**挂在订阅上**给的（`client.Subscription.Handler`），这就是客户端的分发模型：发布按注册
过的 filter 匹配，命中的每个都会跑，`Config.OnMessage` 只接"没有任何 filter 认领"的那些。
`Client.AddRoute` 可以在不订阅的情况下注册同样的东西。这与 paho 一致——映射表见
[`docs/CLIENT.zh-CN.md`](../docs/CLIENT.zh-CN.md)。

## cluster-node

**回答：** 怎么跑一个加入集群的 broker 节点，使得在一个节点上发布、在另一个节点上的订阅者
能收到。它不是一次性示例而是长期运行的节点：起两个进程、给同一份 seeds，然后用 MQTT 客户端
分别连上去验证。

```sh
go run ./examples/cluster-node -node a -addr 127.0.0.1:1883 -cluster-addr 127.0.0.1:7901 -seeds 127.0.0.1:7901,127.0.0.1:7902
go run ./examples/cluster-node -node b -addr 127.0.0.1:1884 -cluster-addr 127.0.0.1:7902 -seeds 127.0.0.1:7901,127.0.0.1:7902
```

```
node a
  mqtt    127.0.0.1:1883
  cluster 127.0.0.1:7901
  seeds   [127.0.0.1:7901 127.0.0.1:7902]
[stop] node a stopped
```

节点只拨号"地址排序在自己之后"的 seed，所以对称的 seed 列表恰好得到每对一条连接。认证是
allow-all：这是本机验证用的夹具，永远不要当生产部署。Ctrl-C 停止，它会排空后以 0 退出；
加 `-v` 看 broker 的调试日志。

每个被接受的连接都会让该节点在 `<greet>_reply` 上发一条 retained 的 `hi!`
（`-greet`，默认 `cluster/hello`；`-greet ""` 关掉），集群会像复制其它保留消息一样复制它。
所以**在另一个节点**订阅也能拿到——用一个 MQTT 客户端、一条连接就能看见集群在工作的最短路径：

```sh
# 终端 1、2：就是上面两个节点，不用改
# 然后对着节点 b（问候来自节点 a）：
go run ./examples/client-demo -addr 127.0.0.1:1884 -topic cluster/hello_reply
```

## plugin-codec

**回答：** 应用侧插件怎么扩展 broker。broker 本身不知道 JSON、CSV 或二进制帧是什么；插件注册
codec 与转码器，订阅者收到的是转码后的字节。最后一步卸载插件再发一次，于是"透传回来了"也能
看见。

```sh
go run ./examples/plugin-codec
go run ./examples/plugin-codec -format csv
go run ./examples/plugin-codec -topic demo/telemetry -format json
```

```
[1/6] broker listening on 127.0.0.1:62260
[2/6] plugin codec-demo is enabled (codecs [application/json application/octet-stream text/csv])
[3/6] client "plugin-codec" subscribed to demo/telemetry
[4/6] published json, received as application/octet-stream: id 7 state on
[5/6] decoded with the same codec set: map[id:7 state:on]
[6/6] plugin unloaded; the payload passes through unchanged (21 bytes)
[done] codecs registered, transcoded and removed by an application-side plugin
```

第 5 步用 broker 用过的同一个 `hook.CodecSet` 解码收到的字节，这样才证明载荷确实是预期的
媒体类型，而不只是一堆不透明字节。`-format csv` 把同一条往返反过来跑：CSV 进、JSON 出。

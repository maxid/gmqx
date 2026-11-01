[English](PERFORMANCE.md) | 简体中文

# 性能基线

本文记录本仓库 broker 与 client 的基准与浸泡基线。它只回答一个窄问题——"当前代码在这台机器上表现如何"——而刻意不回答"生产环境能扛多少连接"。参见[这些数字的边界](#这些数字的边界)。

下列数字由 [`bench_test.go`](../bench_test.go) 中的基准与 [`soak_test.go`](../soak_test.go) 中的浸泡测试产生。换版本、换机器或换 Go 工具链之前请重新测量：它们是参考值，不是容量承诺。

## 环境

| 项 | 值 |
|---|---|
| 日期 | 2026-11-01 |
| Go | `go1.27.1` |
| `GOOS` / `GOARCH` | `darwin` / `arm64` |
| CPU | Apple M1 Max，10 核（10 逻辑核，`GOMAXPROCS=10`） |
| 内存 | 64 GiB |
| 操作系统 | macOS 14.8.9 (Darwin 23J631) |
| 传输 | 回环 TCP，客户端与 broker 在同一进程内 |

## 方法

下面的顺序基准每次只测一趟发布→投递往返；而 broker 在大量并发连接下的持续速率是另一回事，由独立 harness 测量。200 连接的数字、其设计与前后对比见[并发吞吐](#并发吞吐200-连接)。

`bench_test.go` 里每个基准都跑真实的 `gmqx.Broker`、走真实 TCP：基准自己在 `127.0.0.1:0` 上起 broker，用一个基于 `gnety/codec/mqtt` 的小型 MQTT 3.1.1 线级客户端连上去，端到端测量发布。没有任何 mock，没有两个基准共用一个 broker，每个基准在函数返回时停掉自己的 broker。

所有用例的形态一致：

- 准备阶段（起 broker、建连接、订阅、灌 retained）在 `b.ResetTimer()` 之前完成，因此不计入 `ns/op`；
- 订阅端由专门的 drain 协程读取并确认每一条投递，使 broker 不会因为写队列或 inflight 窗口满而停住——不排空的基准测到的多半是自己的背压；
- 发布循环是窗口化的：每若干条发布就等订阅端收到，计时器仍在跑。因此 `ns/op` 覆盖发布→投递路径，而不只是"broker 读到了字节"。只有在 `b.StopTimer()` 之后才排空尾部（不足一个窗口）；
- `b.ReportAllocs()` 报告的是整个基准进程的 `B/op` 与 `allocs/op`，含 drain 协程；
- 载荷默认 16 字节；测量路径上没有固定 sleep，等待是 channel 阻塞而非轮询。

| 基准 | 拓扑 | `ns/op` 的含义 |
|---|---|---|
| `BenchmarkPublishQoS0` | 1 发布者 → 1 订阅者，订阅 QoS 0 | 一条 QoS 0 发布，从写入到投递 |
| `BenchmarkPublishQoS1` | 1 发布者 → 1 订阅者，订阅 QoS 1 | 一条 QoS 1 发布：PUBLISH、broker 路由、投递、PUBACK |
| `BenchmarkFanout/subscribers=N` | 1 发布者 → N 订阅者（N = 10、100） | 一条发布扇出给全部 N 个；单次投递成本为 `ns/op ÷ N` |
| `BenchmarkManySubscriptions` | 1 发布者，一个连接上 N 条订阅，其中一条匹配 | 一条发布命中 N 条订阅中的恰好一条 |
| `BenchmarkRetainedReplay` | broker 持有 1000 条 retained，每次操作一个新订阅者 | 连接 + SUBSCRIBE + 重放 1000 条 retained + 断开 |
| `BenchmarkClientPublishQoS1` | `client` 包 → broker → 1 个线级订阅者 | 一次 `client.Publish` 调用（QoS 1），即到 PUBACK 的完整往返 |

`BenchmarkManySubscriptions` 发布的主题本身就在订阅集合里，因此每个用例都恰好只有一个目标。`exact/subs=100` 是为了对比 `exact/subs=10000`：如果路由索引会随订阅数退化，两者就会分叉。`wildcard=100` 额外加入 100 条**不匹配**发布主题的通配 filter；索引中只要存在通配 filter 就会走通用路由路径，即对每条不同的通配 filter 跑 `matchTopic` 并按会话合并结果，因此这个用例暴露的正是那次扫描的既定成本。

## 实测基线

一次运行：

```sh
go test -run '^$' -bench . -benchtime=2s ./...
```

结果（真实输出，整理成表）。这些是本文的起点基线；下文的吞吐优化工作重新测量了其中四项（见[改了什么](#改了什么以及每项的价值)中的顺序基准前后对比表）：

| 基准 | ns/op | B/op | allocs/op | 折算速率 |
|---|---:|---:|---:|---:|
| `BenchmarkPublishQoS0` | 5,931 | 1,024 | 10 | ≈169k 发布/s |
| `BenchmarkPublishQoS1` | 31,925 | 2,344 | 19 | ≈31k 发布/s |
| `BenchmarkFanout/subscribers=10` | 29,492 | 6,128 | 46 | ≈34k 发布/s，单次投递 2.95 µs |
| `BenchmarkFanout/subscribers=100` | 235,682 | 57,912 | 406 | ≈4.2k 发布/s，单次投递 2.36 µs |
| `BenchmarkManySubscriptions/exact/subs=100` | 6,759 | 1,026 | 9 | ≈148k 发布/s |
| `BenchmarkManySubscriptions/exact/subs=10000` | 5,133 | 1,026 | 9 | ≈195k 发布/s |
| `BenchmarkManySubscriptions/wildcard=100/subs=10000` | 7,301 | 1,073 | 10 | ≈137k 发布/s |
| `BenchmarkRetainedReplay` | 7,609,712 | 3,840,692 | 7,880 | ≈131 次加入/s，每次 1000 条 retained |
| `BenchmarkClientPublishQoS1` | 41,642 | 3,801 | 29 | ≈24k 次 `client.Publish` 往返/s |

同一次运行还执行了包内原有的路由索引基准。它们把 `Broker.LocalTargets` 从 socket 路径中隔离开，是"索引不随订阅数退化"最清楚的证据：

| 基准 | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `BenchmarkLocalTargets/subs=10/wildcards=0` | 150.0 | 96 | 1 |
| `BenchmarkLocalTargets/subs=1000/wildcards=0` | 145.4 | 96 | 1 |
| `BenchmarkLocalTargets/subs=10000/wildcards=0` | 138.3 | 96 | 1 |
| `BenchmarkLocalTargets/subs=10/wildcards=64` | 1,384 | 144 | 2 |
| `BenchmarkLocalTargets/subs=1000/wildcards=64` | 1,413 | 144 | 2 |
| `BenchmarkLocalTargets/subs=10000/wildcards=64` | 2,211 | 144 | 2 |
| `BenchmarkSharedTargetSelection/members=1` | 79.21 | 0 | 0 |
| `BenchmarkSharedTargetSelection/members=10` | 83.39 | 0 | 0 |
| `BenchmarkSharedTargetSelection/members=1000` | 93.19 | 0 | 0 |

这台机器上的运行间波动是真实存在的：上述基准多次 2 秒运行中，`BenchmarkPublishQoS0` 在 4.7–7.3 µs 之间移动，`BenchmarkPublishQoS1` 在 31.9–53.3 µs，`BenchmarkFanout/subscribers=10` 在 29.3–64.0 µs，`BenchmarkClientPublishQoS1` 在 40.8–53.4 µs，而 `BenchmarkRetainedReplay` 稳定在 7.5–7.9 ms。分配量每次操作的浮动不超过一个对象。单次运行足以比较数量级、也足以发现这份基线存在的意义所在的那类回归；但不足以宣称 10% 的提升，扇出类用例在把差异称为真实之前需要重复运行。

## 测量说明了什么

- **路由索引是平的。** `BenchmarkManySubscriptions/exact` 在 100 与 10,000 条在线订阅下测得相同成本（5.1–6.8 µs，处于噪声带内），而只测查找的 `BenchmarkLocalTargets` 从 10 到 10,000 条订阅都稳定在约 140–150 ns/op。端到端数字由两次回环 socket 跳转主导，而不是路由。
- **索引的成本乘数是通配 filter 数，不是订阅数。** 通用路由路径会对每条不同的通配 filter 跑 `matchTopic`。在索引层面，64 条不匹配的通配 filter 把约 145 ns 的查找变成约 1.4–2.2 µs；端到端基准中 100 条不匹配的通配 filter 每条发布增加大约 1–2 µs。这是 `subscriptionIndex` 既定的设计取舍（见 `routing_index.go`），也正是"通配 filter 很多的部署应按通配 filter 数量而非订阅数做容量规划"的原因。
- **每条发布的分配在 QoS 0 下不到 1 KB，QoS 1 下约 1.7 KB。** 剖析（`go test -bench ... -memprofile`）把 QoS 0 的分配归因于线格式解码（`gnety/codec/mqtt.decodePublish` 每包分配载荷与主题字符串）以及 broker 的出站发布构造（`(*client).writePublishLocked`）。内置 dispatcher 原先每条发布一次的 `LocalTarget` 切片已被吞吐优化移除（公开的 `Broker.LocalTargets` 仍返回切片）。其余分配都不随订阅数增长。
- **QoS 1/2 投递不再为每条分配一个定时器。** 连接的 `(*client).retransmitLoop` 现在在整个连接生命周期内只持有一个定时器，并且只在最早重传截止时间前移时被唤醒；在吞吐优化之前，它每次投递都会创建 `time.Timer`、全表重扫 inflight 并唤醒循环。顺序 QoS 1 基准因此从 2,344 B/op、19 allocs/op 降到 1,667 B/op、13 allocs/op。
- **QoS 1 约为 QoS 0 的 5 倍成本。** QoS 1 发布要等 PUBACK，而 broker 把 PUBACK 交给发布连接的写协程处理（一次 channel 交接加一次协程唤醒），不是直接写 socket。单向与往返的差别正来自这次交接；写队列合并（见下文）降低了往返时间，但没有消除交接。
- **扇出随投递数而非发布者数增长。** 写合并之后，10 到 100 个订阅者的单次投递成本约 1.6–1.8 µs（此前约 2.4–3.0 µs）；发布者自身的成本相比之下可以忽略。
- **retained 重放与 retained 存储规模成线性。** 每次加入在 1000 条 retained 下约 7.6 ms（含连接、SUBSCRIBE 与 QoS 1 确认，平均每条重放消息约 7.6 µs）。broker 在每次订阅时扫描整个 retained map（`sendRetained`），因此扫描与投递都随 retained 条数增长。

## 并发吞吐（200 连接）

`BenchmarkPublishQoS1` 每次只做一趟发布→投递往返，因此它报告的是延迟，而不是数百连接同时发布时 broker 的持续消息速率。`throughput_test.go` 测的就是那个速率。它复现了这一 broker 家族的参考实现所报告的口径（200 个发布者连接、固定 5 秒窗口、按秒区间的 msg/s），使两个数字可以比较；同时补充了该口径不报告的指标：投递速率与 p50/p99 投递延迟。

### harness

`TestThroughput` 由 `GMQX_THROUGHPUT=1` 开启；未设置时该测试跳过，常规套件不受影响。

- **拓扑。** 每个用例一个 broker，监听 `127.0.0.1:0`。200 个发布者连接（`GMQX_THROUGHPUT_CLIENTS`），各占一条 TCP 连接，发布到各自的主题 `bench/throughput/p<shard>/<publisher>`。4 个订阅者连接（`GMQX_THROUGHPUT_SUBS`）；第 *s* 个订阅者订阅 `bench/throughput/p<s>/#`，发布者按键分片到订阅者上，每条发布恰好投递给其中一个——发布数与投递数之间没有扇出放大。
- **负载。** 发布者在窗口内（5 秒，`GMQX_THROUGHPUT_DURATION`）全速发布，循环内没有逐条往返。在 QoS 1/2 下每个发布者还维护一条有界的未确认流水线（256，`GMQX_THROUGHPUT_UNACKED`），按 PUBACK/PUBCOMP 释放，使发送速率不受请求/响应约束。
- **闭环。** 只有当先前一条发布被投递或丢弃后，新的发布才进入 harness（共享节流器，尺寸为每个订阅者的 inflight 窗口加 4096 条余量；`GMQX_THROUGHPUT_INFLIGHT` 可覆盖）。若不闭环，QoS 0 用例会填满内核 socket 缓冲——在这台机器上写操作最终报 "no buffer space available"——而 QoS 1/2 用例会把 broker 有界的 inflight 队列压满，测到的只是它丢弃消息的速度。闭环把队列留在发布者一侧，逐秒采样也把预热过程显示出来而不是藏在平均值里。
- **载荷与延迟。** 载荷为 64 B 与 1 KiB（`GMQX_THROUGHPUT_PAYLOAD`）。前 8 字节是每发布者的序号，后 8 字节是发送者的 Unix 纳秒时钟；订阅者据此计算投递延迟，因此 p50/p99 覆盖同一进程内（同一时钟）完整的发布→投递路径。
- **统计口径。** `published/s` 是发布者完成写入的量，`acked/s` 是它们收到的 PUBACK（QoS 1）或 PUBCOMP（QoS 2），`accepted/s` 是 broker 路由的量，`delivered/s` 是订阅者收到的量。标题数字取窗口首个与末个采样之间，因此窗口关闭时发布者手里尚未发出的部分不计入。
- **客户端。** harness 复用 `bench_test.go` 的线级客户端而非 `client` 包：`client` 包的投递协程与重连机制在 200 连接下会挡在 broker 前面。发布者的热循环把预编码好的 PUBLISH 批量直写 socket。
- **harness 配置。** 关闭字节速率整形，放宽出站 inflight 窗口与其队列（256 / 8192），写背压队列设为 4096 包，使测量结果是 broker 的吞吐而不是它某个既定的流控默认值。丢弃仍然被计数并打印，其余限制保持默认。

运行方式（在 `libs/gmqx` 下）：

```sh
# 完整矩阵：QoS 0/1/2 × 64 B / 1 KiB，200 连接，每个用例 5 秒（约 35 秒）。
GMQX_THROUGHPUT=1 go test -run TestThroughput -v .

# 单个用例（下文剖析的那个）。
GMQX_THROUGHPUT=1 GMQX_THROUGHPUT_QOS=1 GMQX_THROUGHPUT_PAYLOAD=64 \
    go test -run TestThroughput -cpuprofile /tmp/cpu.out \
    -memprofile /tmp/mem.out .
go tool pprof -top /tmp/cpu.out
```

### 结果

与本文其余部分同一台机器、同一条命令、200 个发布者连接、4 个订阅者连接、5 秒窗口。"之前"是本次测量起点的 broker（顺序基线取自的那份代码）；"之后"是同一 broker 加上下文所列改动之后。

| QoS | 载荷 | | published/s | acked/s | delivered/s | p50 | p99 | 丢弃 |
|---:|---:|---|---:|---:|---:|---:|---:|---|
| 0 | 64 B | 之前 | 545,441 | — | 350,296 | 40.9 ms | 102.1 ms | 1,040,017 |
| 0 | 64 B | 之后 | 1,391,076 | — | 1,382,662 | 6.8 ms | 59.1 ms | 45,767 |
| 0 | 1 KiB | 之前 | 486,631 | — | 343,195 | 46.0 ms | 85.6 ms | 739,026 |
| 0 | 1 KiB | 之后 | 986,969 | — | 983,664 | 9.9 ms | 87.6 ms | 无 |
| 1 | 64 B | 之前 | 145,798 | 145,700 | 140,650 | 115.5 ms | 280.6 ms | 9,067 |
| 1 | 64 B | 之后 | 627,855 | 627,728 | 617,757 | 24.2 ms | 73.8 ms | 40,358 |
| 1 | 1 KiB | 之前 | 137,999 | 137,780 | 132,678 | 122.3 ms | 305.6 ms | 10,284 |
| 1 | 1 KiB | 之后 | 477,730 | 477,476 | 471,028 | 35.0 ms | 94.5 ms | 17,520 |
| 2 | 64 B | 之前 | 74,792 | 74,376 | 71,126 | 234.3 ms | 479.2 ms | 1,130 |
| 2 | 64 B | 之后 | 175,380 | 175,126 | 168,885 | 87.4 ms | 242.0 ms | 15,687 |
| 2 | 1 KiB | 之前 | 71,278 | 71,175 | 67,796 | 230.9 ms | 444.6 ms | 无 |
| 2 | 1 KiB | 之后 | 136,881 | 136,707 | 133,032 | 109.7 ms | 329.0 ms | 1,995 |

在这个负载下，运行间波动是真实存在的：QoS 0 / 64 B 重复运行投递量此前为 350k、358k、392k msg/s，之后为 1.38M、1.57M、1.61M、1.63M、1.70M、1.76M；QoS 1 / 64 B 此前为 139k、141k、143k，之后为 582k、597k、618k、685k、733k、749k。取中位数，QoS 0 由约 358k 提到约 1.63M msg/s（≈4.5×），QoS 1 由约 141k 提到约 617k msg/s（≈4.4×），QoS 2 提升约 2×。QoS 1/2 的投递速率是**持续端到端**速率：发布者的 PUBACK 约束流水线，订阅者的 inflight 窗口约束投递，少量的 `inflight_overflow` 是 broker 对"投递时发现有序队列已满"的既定溢出策略。（harness 的订阅者是 clean 会话，因此在“把**持久**会话转为离线而不是丢弃”的构建里这些丢弃以 `inflight_overflow_clean` 上报；clean 会话的策略与上表数字不变。）

被剖析用例（QoS 1、64 B、之后）的逐秒曲线即为 harness 打印的内容，显示窗口内是稳定的而非突发：

```
case qos=1 payload=64B clients=200 subscribers=4 window=5s
  t         published/s      acked/s   accepted/s  delivered/s
  t+1.0s         738171       737119       737429       718531
  t+2.0s         679143       678907       679377       666380
  t+3.0s         684331       685498       684760       678784
  t+4.0s         684430       683944       684185       682466
  t+5.0s         684369       683964       684381       680042
  total          694090       693887       694027       685241   (p50=21740µs p99=75108µs n=3444572)
```

### 剖析：200 连接负载的成本构成

同一负载下 QoS 1 / 64 B 用例的 CPU 剖析。百分比基于剖析总样本（10 核机器上 5.7–6.6 核繁忙——harness 与本进程共享 CPU，其占用也计入其中）。

之前（顺序基线取自的那份代码）：

| 成本 | 占比 |
|---|---:|
| 每包在连接写协程里一次 `write(2)` | 52 % |
| 入站 PUBLISH 的读取与解码（载荷/主题分配） | 27 % |
| 路由：每发布的目标切片与通配扫描 | 4.3 % |
| `windowFullLocked`：每投递全表扫描 inflight | 4.3 % |
| 分配与 GC（`mallocgc`、`scanObject`、`sweep`） | 约 8 % |

窗口内分配量为 2.97 GB（594 MB/s），归因于解码的 `decodePublish`（33 %）、类型化 PUBACK（27 %）、路由目标切片与合并 map（12 %）、待投递切片（10 %）与解码体缓冲（9 %）。

之后：

| 成本 | 占比 |
|---|---:|
| 读系统调用与解码 | 读 45 %，其中 34 % 落在 broker 的 `readMessage`/`Decode` |
| 写系统调用 | 合计 37 %，其中 17 % 在 broker 的写协程（`flushBatch`），其余在 harness |
| broker 的发布 → 路由 → 投递路径 | 约 10 % |
| 分配与 GC | 约 6 % |

写协程现在**每次 write 携带 12.4 个包**（窗口内实测），写系统调用数比"每包一次"低一个数量级。窗口内分配仍有 13.3 GB，但同一窗口投递了 5 倍的消息：按已投递消息计每条大致不变（约 4 KB），主要来自解码器每个报文结构体内嵌的约 400 字节 `mqtt.Properties` 值，以及 pending/inflight 表对它的复制。

**剩余的主要成本**（按顺序）：

1. **入站读路径。** 每个发布者报文都要一次 `read(2)` 支撑的 `bufio` 填充，加上解码器逐包分配载荷与主题。这已是最大的单项成本，也是下一个杠杆：一个 broker 侧的解码器把 PUBLISH 体读入可复用缓冲，可消除解码器的逐包分配。
2. **投递表的分配。** pending 或 inflight 投递会复制解码器约 400 字节的 `mqtt.Properties` 值，尽管 MQTT 3.1.1 流量从不携带它。把属性块改成指针可减少约 45% 的字节，但速率没跑出噪声带，因此已回退（见下文）；剩下可选的是对 `mqtt.Publish`／`inflightMessage` 做池化。
3. **订阅者连接的互斥锁。** 200 个发布者压在 4 个订阅者上时，`sendPublish` 的墙钟时间大部分花在等同一个连接的锁：mutex 剖析在 3 秒窗口内归因了 116 秒的等待。增加订阅者分片或减少每分片的发布者会更好；结构性解法是每连接的无锁投递队列。
4. **QoS 1/2 投递窗口。** 每个订阅者连接的投递速率 = 窗口 / 确认往返时间。harness 配置了 256 深窗口；在 200 个发布者下它就是上限，约 1% 的 `inflight_overflow` 丢弃（以 `inflight_overflow_clean` 上报：harness 的订阅者是 clean 会话）正是它的既定溢出。

### 改了什么，以及每项的价值

每项改动都保持 `go test -race ./...` 绿色与既定语义（有界队列、不丢 QoS 1/2 消息、FIFO 顺序、每连接一个写协程）。

| 改动 | 实测效果 |
|---|---|
| **写合并。** 写协程把已入队的包取尽并用一次 `write(2)` 写出，上限 64 包 / 64 KiB，保持同一写超时与同一队列顺序。 | QoS 0 投递 247k → 1.37M msg/s（5.5×）；QoS 1 确认 486k → 639k（+31%），以该步骤的 harness 测得。 |
| **O(1) inflight 窗口。** `windowFullLocked`/`receiveMaximumFullLocked` 读取与 inflight map 同步维护的计数，而不是每次投递扫描整张表——那次扫描在 200 个发布者共享的连接锁内执行。 | QoS 1 确认 639k → 1.59M msg/s（+148%）；投递 22k → 80k（3.6×），同一 harness。 |
| **紧凑确认。** 原因码为 0 且无属性的 PUBACK/PUBREC/PUBREL/PUBCOMP 只以包 ID 的形式传输：由写协程直接构造四字节帧，没有类型化报文也没有分配。 | PUBACK 路径此前占窗口分配量的 2.52 GB（27%），之后约为 0；在窗口扫描消除后，吞吐变化落在噪声带内。 |
| **复用重传定时器。** 每连接一个定时器，只在最早重传截止时间前移时重置；一次投递不再唤醒循环、重扫整张 inflight 表并分配 `time.Timer`。 | 吞吐变化落在噪声带内（该步骤 harness 中投递 79.7k → 80k），但消除了每次投递的定时器分配与整表扫描，两者都在连接锁内。 |
| **路由零分配。** 内置 dispatcher 把目标收集到栈上数组而不是每条发布新建切片；命中恰好一条通配 filter 的主题跳过按会话合并的 map。目标仍在索引读锁内收集、释放锁之后投递（一次投递可能关闭连接并注销其会话，那需要该锁的写权限）。 | 路由占 CPU 从 4.3% 降到 0.7%；其分配从剖析的 1.15 GB 降到 0.1%。`BenchmarkLocalTargets` 不变（公开的 `LocalTargets` 仍返回切片，一次分配）；`BenchmarkFanout/subscribers=100` 因写合并由 345 µs 降到 203 µs（41%）。 |

顺序基准的变化体现在分配量上（可复现），而 `ns/op` 仍处于这台机器的运行间噪声内（前述 30% 带宽，扇出可达 2×）。两次成对的 `go test -run '^$' -bench 'PublishQoS|Fanout' -benchtime=2s` 前后对比：

| 基准 | 之前 | 之后 |
|---|---|---|
| `BenchmarkPublishQoS0` | 1,024 B/op，9–10 allocs/op | 928 B/op，8–9 allocs/op |
| `BenchmarkPublishQoS1` | 2,344 B/op，19 allocs/op | 1,667 B/op，13 allocs/op |
| `BenchmarkClientPublishQoS1` | 3,800 B/op，28 allocs/op | 3,121 B/op，22 allocs/op |
| `BenchmarkFanout/subscribers=10` | 59.5 µs 与 20.7 µs | 32.3 µs 与 18.0 µs |
| `BenchmarkFanout/subscribers=100` | 345.3 µs 与 265.2 µs | 203.3 µs 与 121.3 µs |

扇出用例在每一对比较中都有改善（写合并把每订阅者一批从"每次投递一次写"变为"一次写"）；单发布者用例的移动小于噪声，而 `BenchmarkClientPublishQoS1`——一次请求/响应往返——保持持平。改动真正体现价值的地方是并发 harness：200 个连接持续发布，于是"每包一次写"或"每投递一次 map 扫描"每秒被支付数百万次，而不是每个 `ns/op` 一次。

试过、测量过、随后回退或否决的：

- **pending 队列里的指针化属性块。** pending 表的分配（窗口内 5.7 GB）主要是把解码器约 400 字节的 `mqtt.Properties` 值复制进每条入队投递；改成指针消除了这次复制。投递速率仅在 ±10% 的运行间噪声内移动，因此回退，而不是留下额外复杂度。
- **从读协程直写微小控制报文。** 合并后实测每次 write 携带 12.4 个包，直写 PUBACK 会把"每 12 次确认一次写"变成"每次确认一次写"（系统调用多一个数量级），并破坏既定的单写者 FIFO 顺序。不采用。
- **更大的写批次。** 在已有的 64 包 / 64 KiB 上限之外没有额外效果；平均批次为 12.4 包。
- **增加订阅者分片。** 在同一批 200 个发布者下用 8 与 50 个订阅者连接并未提高投递总量（机器受系统调用与锁限制）；默认保留 4，与分片主题空间一致。

做这件事时 harness 发现并在此之前修掉了一个缺陷：批量 drain 循环把紧凑确认（其 `msg` 字段为 nil）当作"队列已空"，从而静默丢弃，表现为 QoS 1 发布者流水线填满并卡死。

### 并发数字的边界

除本文其余部分的边界外，这个 harness 还有两条特定限制：负载生成器与 broker 共享本进程的 CPU（参考实现的口径也是这样测的），且数字取决于订阅者分片数与 harness 放宽的流控窗口，二者都已在上文说明。这是单机、回环、单进程的容量读数——与参考实现的口径可比正是由于同形（200 个并发发布者连接、固定采样窗口、msg/s），也可跨本 broker 版本用同一 harness 对比——而不是部署承诺。

## 浸泡测试

`TestSoak` 默认关闭，用 `GMQX_SOAK=1` 开启；运行时长由 `GMQX_SOAK_DURATION` 决定（Go duration，默认 `10s`）。四个 worker 各自拥有一条发布连接与一条订阅连接，循环执行：发布一批 16 条 QoS 在 0、1、2 之间轮转的消息；等该批中每一条已确认的 QoS 1/2 消息都被投递；每 64 批重连一次——用 DISCONNECT，或每三个周期用直接中止 socket 的方式，即 broker 必须善后的意外断链。只有前一批完全排空后才发下一批，这才使投递断言是精确的：不存在"已被确认、却被测试自己拆除连接时丢掉"的消息。

在上述机器上运行：

```sh
GMQX_SOAK=1 GMQX_SOAK_DURATION=10s go test -run TestSoak -v ./...
```

结果：

```
soak teardown: clients=0 stored_sessions=0 indexed_sessions=0 retained=0 pending_wills=0 offline_queued=0
soak delivery ledger: acknowledged=261249 delivered_qos12=261249 duplicates=0
soak resources: goroutines=2 (baseline 2, bound 10) heap_before=428928 heap_after=651640 heap_bound=67966720 total_alloc_delta=1046284304
soak result: 380 generations, 768 connections, 24492 batches, 391872 messages (130623 QoS 0, 130626 QoS 1, 130623 QoS 2), 261249 acknowledged, 261249 delivered
```

同一条命令在同一台机器上的第二次运行产出 358 代、724 条连接、368,576 条消息、245,719 条确认 / 245,719 条投递，表同样为空，goroutine 基线同为 2。计数随调度浮动，边界没有浮动。

| 浸泡参数 / 结果 | 值 |
|---|---|
| 时长 | 10 s |
| worker 数 | 4（各一条发布 + 一条订阅） |
| 批大小 | 16 条，QoS 0/1/2 混合 |
| 每连接批数 | 64 |
| 连接代数 | 380 |
| 打开的连接数 | 768 |
| 批数 | 24,492 |
| 发布消息数 | 391,872（QoS 0 130,623、QoS 1 130,626、QoS 2 130,623） |
| QoS 1/2 已确认 | 261,249 |
| QoS 1/2 已投递 | 261,249（缺失 0，重复 0） |
| 运行后 broker 客户端 / 会话 / 索引表 | 0 / 0 / 0 |
| 运行后 retained / 待发 will | 0 / 0 |
| 运行后 goroutine | 2，基线 2，上限 10 |
| 运行后存活堆（两次 GC 之后） | 651,640 B，之前 428,928 B，上限 67,966,720 B |

边界刻意保守，理由如下：

- **goroutine：基线 + 8。** 一处泄漏——broker 的连接协程、写协程、重传定时器或 drain 协程——至少增加每连接一个 goroutine，而该轮运行打开数百条连接；每 worker 一处泄漏则至少增加四个。`Broker.Stop` 会 join broker 启动的所有协程（专门的泄漏测试断言其为 `baseline+2`），因此 8 足以覆盖 runtime 与 testing 包的噪声，又不会掩盖真实泄漏。
- **堆：测试前存活堆的 2 倍 + 64 MiB**，在两次强制 GC 后用 `runtime.ReadMemStats` 测量。只用倍数在小基线上过紧，只用下限在大基线上又会忽略真实增长；两者并用才能同时约束。上述 10 秒运行总共分配约 1.05 GB（全是垃圾，已被回收），而存活堆始终低于 1 MiB。
- **表必须为空，而不是有界。** 浸泡中的每个会话都是 clean session，因此 `Stop` 之后残留的会话或客户端条目是 bug，而非容量效应。断言检查 broker 的客户端表、会话存储、路由索引、retained 存储与待发 will。
- **投递账本。** 每批中，worker 收到确认的每一条 QoS 1/2 消息都必须出现在该批的已投递集合中；最终总量须满足 `delivered_qos12 >= acknowledged`。重复会被报告但不判失败，因为至少一次语义允许重复；上文每次运行的重复均为 0。

## 复现

在仓库根（`libs/gmqx`）下：

```sh
# 快速冒烟：每个基准跑一次迭代，总共几秒。
go test -run '^$' -bench . -benchtime=1x ./...

# 上文实测基线。
go test -run '^$' -bench . -benchtime=2s ./...

# 单个用例，带 CPU 剖析。
go test -run '^$' -bench 'BenchmarkPublishQoS1$' -benchtime=2s \
    -cpuprofile /tmp/cpu.out -memprofile /tmp/mem.out .
go tool pprof -top /tmp/cpu.out

# 并发吞吐矩阵（200 连接，每个用例 5 秒）。
GMQX_THROUGHPUT=1 go test -run TestThroughput -v .

# 单个吞吐用例，带 CPU 与堆剖析。
GMQX_THROUGHPUT=1 GMQX_THROUGHPUT_QOS=1 GMQX_THROUGHPUT_PAYLOAD=64 \
    go test -run TestThroughput -cpuprofile /tmp/cpu.out -memprofile /tmp/mem.out .
go tool pprof -top /tmp/cpu.out

# 浸泡测试（默认 10 秒）。
GMQX_SOAK=1 GMQX_SOAK_DURATION=10s go test -run TestSoak -v ./...

# 更短的浸泡，适合合并前检查。
GMQX_SOAK=1 GMQX_SOAK_DURATION=3s go test -run TestSoak -v ./...
```

`scripts/bench.sh` 与 `scripts/soak.sh` 封装了上述两条命令（`BENCHTIME=2s ./scripts/bench.sh`、`GMQX_SOAK_DURATION=30s ./scripts/soak.sh`）。

完整的 `-benchtime=2s` 运行在这台机器上约需一分钟：大约一半花在包内原有的路由索引基准上，另一半花在产生这份基线的基准函数上。

## 这些数字的边界

这些结果是**参考值，不是容量承诺**。它们来自一台机器、一个 Go 版本、一个进程与回环 TCP：

- 两端共享同一 CPU 与内核；没有网络往返时间、没有丢包、没有 TLS 握手与记录开销，broker 前面也没有代理或负载均衡；
- broker 以默认限制运行（inflight 窗口 64、写背压与 inflight 队列 256 包、每连接 8/16 MiB/s 与全局 256 MiB/s 的字节速率整形），且未配置认证、ACL、sink、interceptor、会话存储或集群后端；retained 重放用例只把 `MaxInflightQueueSize` 提到 4096，以免 1000 条重放被满的 inflight 队列丢弃；
- 基准使用单一 16 字节载荷、单一主题形态、每个用例一个发布者；真实流量混合载荷大小、主题基数、QoS 等级与连接生命周期，并且有慢消费者；
- `ns/op` 是单线程发布者的墙钟时间，不是经过调优的多发布者部署的吞吐；结果表里的折算速率只是倒数，仅用于比较；
- 这台机器的运行间波动对 socket 受限用例通常是 30%，扇出用例达到 2×，因此只有明显更大的变化才能在单次运行中看出来。

## 生产容量规划还需要什么

- **有代表性的硬件与网络。** 目标 CPU 级别、真实网卡及其中断分布、客户端→broker 的 RTT 与带宽、连接建立速率（含 TLS），并在部署实际使用的拓扑上测量。
- **生产配置。** TLS（含会话恢复）、配置的 inflight 窗口、队列大小与字节速率限制、认证与 ACL 钩子、事件 sink、interceptor，以及支撑持久会话的那个 `SessionStore`（含其延迟与磁盘）。
- **真实流量构成。** 载荷大小与主题基数分布、QoS 组合、持久会话与 clean 会话的比例、离线队列深度、共享订阅、retained 条数，以及慢消费者的数量与行为。通配 filter 数量应在计划中单列一行，因为是它而非订阅数决定路由成本。
- **集群。** 多节点扇出与集群后端（gossip、TCP mesh、Redis）、分区与故障转移行为，以及远程发布多出的跳数。
- **持续负载证据，而不是一次基准。** 数小时的运行配合 CPU、堆、goroutine、文件描述符与连接数看板，并在负载下采集 CPU 与堆剖析，使泄漏与缓慢漂移可见——这里的 10 秒浸泡是正确性与粗粒度回归闸门，不是泄漏证明。
- **操作系统与主机调优。** 文件描述符与临时端口限制、TCP accept 与 TIME_WAIT 行为、CPU 绑核或 cgroup 限制，并在部署预期的连接数下测量。
- **带余量的容量模型。** 由实测的每消息与每连接成本加上预期峰值与突发流量推导容量，并为故障转移与慢消费者造成的劣化显式留出余量，而不是把基准的 `ns/op` 当作服务上限来读。

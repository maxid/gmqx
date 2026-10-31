package interop

// The reference-broker direction, driven through docker:
//
//	direction A: the gmqx client package against eclipse-mosquitto;
//	direction B: the gmqx broker against the mosquitto_pub/mosquitto_sub
//	             clients bundled in the eclipse-mosquitto image.
//
// Neither the reference broker nor its clients can be installed on the host, so
// both come from the image: -p maps the broker to a free loopback port, and
// docker exec runs the clients inside the same container against
// host.docker.internal:<port> for direction B.
//
// Every docker interaction is bounded. When docker is unavailable, the image
// cannot be pulled or the container cannot be started, each case skips with the
// reason instead of failing; the paho matrix does not need docker at all.

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
	"github.com/maxid/gnety/codec/mqtt"
)

const (
	// mosquittoImage carries both the reference broker (its entrypoint) and the
	// reference clients used by direction B (/usr/bin/mosquitto_pub, _sub).
	mosquittoImage = "eclipse-mosquitto:2"
	// containerPrefix names every container this suite creates, so a leftover is
	// recognizable and a human can find it with docker ps -a.
	containerPrefix = "gmqx-interop-"
	// containerPort is the listener inside the container; the host port is
	// picked fresh per run.
	containerPort = "1883"
	// hostAlias is how a container reaches services on the host. Docker Desktop
	// resolves it by itself; the explicit host-gateway entry makes the same name
	// work against a Linux daemon (see the docker run call).
	hostAlias = "host.docker.internal"

	// dockerProbeTimeout bounds a docker command that answers immediately
	// (version, inspect, rm, ps, logs).
	dockerProbeTimeout = 20 * time.Second
	// dockerStartTimeout bounds docker run, which includes the daemon creating
	// the container.
	dockerStartTimeout = 120 * time.Second
	// dockerPullTimeout bounds docker pull. A cold pull of the image is the one
	// slow step of the matrix, and it is bounded too.
	dockerPullTimeout = 180 * time.Second
	// containerReadyTimeout bounds the wait for the mapped port and for a first
	// MQTT handshake against the reference broker.
	containerReadyTimeout = 30 * time.Second
	// referenceClientTimeout bounds one docker exec of a reference client.
	referenceClientTimeout = 30 * time.Second

	// mosquittoConfig is the configuration of the reference broker: one plain
	// listener that accepts anonymous clients (mosquitto 2.x refuses anonymous
	// access unless it is enabled), no persistence.
	mosquittoConfig = `listener 1883 0.0.0.0
allow_anonymous true
persistence false
log_dest stdout
`
)

// referenceBroker is a running eclipse-mosquitto container.
type referenceBroker struct {
	name string
	// addr is the mapped listener as the host sees it: 127.0.0.1:<port>.
	addr string
}

// startMosquitto starts the reference broker and registers its removal.
func startMosquitto(t *testing.T) *referenceBroker {
	t.Helper()
	requireDocker(t)
	ensureImage(t, mosquittoImage)

	dir := t.TempDir()
	configPath := filepath.Join(dir, "mosquitto.conf")
	if err := os.WriteFile(configPath, []byte(mosquittoConfig), 0o644); err != nil {
		t.Fatalf("write the mosquitto config: %v", err)
	}

	name := fmt.Sprintf("%s%d-%08x", containerPrefix, os.Getpid(), rand.Uint32())
	port := freePort(t)
	addr := net.JoinHostPort("127.0.0.1", port)

	t.Cleanup(func() { removeContainer(t, name) })

	out, err := runDocker(dockerStartTimeout, "run", "-d", "--rm",
		"--name", name,
		"--add-host", hostAlias+":host-gateway",
		"-p", addr+":"+containerPort,
		"-v", configPath+":/mosquitto/config/mosquitto.conf:ro",
		mosquittoImage)
	if err != nil {
		t.Skipf("the reference broker container could not be started: %v", err)
	}
	t.Logf("reference broker container %s (id %.12s) listens on %s", name, strings.TrimSpace(string(out)), addr)

	waitDial(t, name, addr, containerReadyTimeout)
	waitMQTT(t, name, addr, containerReadyTimeout)
	return &referenceBroker{name: name, addr: addr}
}

// execReference runs one of the clients bundled in the image inside the
// container.
func (m *referenceBroker) execReference(ctx context.Context, argv ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "docker", append([]string{"exec", m.name}, argv...)...).CombinedOutput()
}

// publish runs mosquitto_pub inside the container against target.
func (m *referenceBroker) publish(t *testing.T, target, topic string, payload []byte, qos byte, retain bool, protocol string) {
	t.Helper()
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		t.Fatalf("target %q: %v", target, err)
	}
	argv := []string{
		"mosquitto_pub",
		"-h", host, "-p", port,
		"-t", topic,
		"-m", string(payload),
		"-q", strconv.Itoa(int(qos)),
	}
	if retain {
		argv = append(argv, "-r")
	}
	if protocol != "" {
		argv = append(argv, "-V", protocol)
	}
	ctx, cancel := context.WithTimeout(context.Background(), referenceClientTimeout)
	defer cancel()
	out, err := m.execReference(ctx, argv...)
	if err != nil {
		t.Fatalf("mosquitto_pub to %s on %q at QoS %d: %v\n%s", target, topic, qos, err, out)
	}
}

// referenceSubscriber is one mosquitto_sub process running inside the container.
type referenceSubscriber struct {
	buf      *syncBuffer
	cancel   context.CancelFunc
	finished chan struct{}
	err      error
}

// subscribe starts mosquitto_sub inside the container against target. It waits
// for exactly one message (-C 1) and gives up after 20 s (-W 20), so the process
// cannot outlive the case by much; the test still kills it at cleanup. The -d
// trace is collected for failure messages, not as a readiness signal: the output
// is block-buffered on a pipe and only reaches the test when the process exits,
// which is why the subscription is confirmed through the broker (subscribeCounter).
func (m *referenceBroker) subscribe(t *testing.T, target, topic string, qos byte, protocol string) *referenceSubscriber {
	t.Helper()
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		t.Fatalf("target %q: %v", target, err)
	}
	argv := []string{
		"mosquitto_sub",
		"-h", host, "-p", port,
		"-t", topic,
		"-q", strconv.Itoa(int(qos)),
		"-C", "1",
		"-W", "20",
		"-d",
	}
	if protocol != "" {
		argv = append(argv, "-V", protocol)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "docker", append([]string{"exec", m.name}, argv...)...)
	buf := &syncBuffer{}
	cmd.Stdout = buf
	cmd.Stderr = buf
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start mosquitto_sub: %v", err)
	}
	sub := &referenceSubscriber{buf: buf, cancel: cancel, finished: make(chan struct{})}
	go func() {
		sub.err = cmd.Wait()
		close(sub.finished)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-sub.finished:
		case <-time.After(5 * time.Second):
			t.Logf("mosquitto_sub of %q did not stop in time; output so far:\n%s", topic, sub.buf.String())
		}
	})
	return sub
}

// subscribeCounter is a MetricsManager that counts accepted subscriptions and
// nothing else.
//
// It is the readiness signal for a reference subscriber. The client's own
// protocol trace (-d) is block-buffered on a pipe and only reaches the test when
// the process exits, so the broker is the only reliable observer of "the
// subscription is live"; counting IncrementSubscribeEvents makes the following
// publication deterministic instead of a race against a slow container.
type subscribeCounter struct {
	gmqx.NoopMetrics
	count atomic.Int64
}

func (c *subscribeCounter) IncrementSubscribeEvents() { c.count.Add(1) }

func (c *subscribeCounter) subscribed() int64 { return c.count.Load() }

// waitExit waits for the reference client to finish and returns its output.
func (s *referenceSubscriber) waitExit(t *testing.T) (string, error) {
	t.Helper()
	select {
	case <-s.finished:
		return s.buf.String(), s.err
	case <-time.After(referenceClientTimeout):
		t.Fatalf("mosquitto_sub did not finish within %s; output so far:\n%s", referenceClientTimeout, s.buf.String())
		return "", nil
	}
}

// syncBuffer collects the interleaved stdout/stderr of a reference client.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// requireDocker skips the test when no daemon answers.
func requireDocker(t *testing.T) {
	t.Helper()
	out, err := runDocker(dockerProbeTimeout, "version", "--format", "{{.Server.Version}}")
	if err != nil {
		t.Skipf("docker is unavailable, so the reference broker and its clients are skipped: %v", err)
	}
	t.Logf("docker server %s", strings.TrimSpace(string(out)))
}

// ensureImage makes the reference image present, pulling it when needed.
func ensureImage(t *testing.T, image string) {
	t.Helper()
	if _, err := runDocker(dockerProbeTimeout, "image", "inspect", image); err == nil {
		return
	}
	t.Logf("pulling %s (bounded to %s)", image, dockerPullTimeout)
	if _, err := runDocker(dockerPullTimeout, "pull", image); err != nil {
		t.Skipf("the %s image cannot be pulled, so the reference implementations are unavailable: %v", image, err)
	}
}

// runDocker runs one bounded docker command and returns its combined output.
func runDocker(timeout time.Duration, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return out, fmt.Errorf("docker %s: %w (%s)", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return out, nil
}

// removeContainer removes the container and checks that docker no longer lists
// it, so a leftover is reported by the suite itself.
func removeContainer(t *testing.T, name string) {
	t.Helper()
	out, err := runDocker(dockerProbeTimeout, "rm", "-f", name)
	if err != nil && !strings.Contains(string(out), "No such container") {
		t.Errorf("container %s could not be removed: %v", name, err)
	}
	out, err = runDocker(dockerProbeTimeout, "ps", "-a", "--filter", "name="+name, "--format", "{{.Names}}")
	if err != nil {
		return
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if strings.TrimSpace(line) == name {
			t.Errorf("container %s is still listed by docker ps -a after removal", name)
		}
	}
}

// freePort reserves a loopback port and releases it for the mapping. The window
// between close and docker run is a few milliseconds, which is why the port is
// picked at the last moment.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a free port: %v", err)
	}
	port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	if err := ln.Close(); err != nil {
		t.Fatalf("release the reserved port: %v", err)
	}
	return port
}

// waitDial waits until the mapped port accepts a TCP connection.
func waitDial(t *testing.T, name, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			return
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Skipf("the mapped port %s did not accept a TCP connection within %s: %v\n%s",
		addr, timeout, lastErr, containerLogs(t, name))
}

// waitMQTT waits until the reference broker completes an MQTT handshake. A TCP
// accept is not proof that mosquitto is listening (the port forwarder answers
// first), so the probe speaks the protocol and retries until the deadline.
func waitMQTT(t *testing.T, name, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
		c, err := client.Dial(ctx, client.Config{
			Addr:           addr,
			ClientID:       "interop-readiness",
			CleanStart:     true,
			ConnectTimeout: connectTimeout,
			KeepAlive:      10 * time.Second,
		})
		cancel()
		if err == nil {
			_ = c.Close()
			return
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Skipf("the reference broker did not answer an MQTT CONNECT on %s within %s: %v\n%s",
		addr, timeout, lastErr, containerLogs(t, name))
}

// containerLogs returns the last lines of the container log for a skip report.
func containerLogs(t *testing.T, name string) string {
	t.Helper()
	out, err := runDocker(dockerProbeTimeout, "logs", "--tail", "20", name)
	if err != nil {
		return fmt.Sprintf("container logs unavailable: %v", err)
	}
	return "container log:\n" + string(out)
}

// TestClientAgainstMosquitto is direction A: the gmqx client package against the
// reference broker.
func TestClientAgainstMosquitto(t *testing.T) {
	ref := startMosquitto(t)

	t.Run("QoS round trips", func(t *testing.T) {
		for _, version := range []byte{mqtt.Version311, mqtt.Version5} {
			version := version
			for _, qos := range []byte{0, 1, 2} {
				qos := qos
				t.Run(mqttVersionName(version)+"/QoS"+strconv.Itoa(int(qos)), func(t *testing.T) {
					topic := fmt.Sprintf("interop/gmqx/qos/%d/%d", version, qos)
					payload := []byte(fmt.Sprintf("gmqx-%s-qos%d", mqttVersionName(version), qos))

					sub := newSink()
					subClient := dialGMQX(t, client.Config{
						Addr:       ref.addr,
						ClientID:   fmt.Sprintf("gmqx-sub-%d-%d", version, qos),
						CleanStart: true,
						Version:    version,
						KeepAlive:  10 * time.Second,
						OnMessage:  sinkHandler(sub),
					})
					subscribeGMQX(t, subClient, topic, qos)

					pubClient := dialGMQX(t, client.Config{
						Addr:       ref.addr,
						ClientID:   fmt.Sprintf("gmqx-pub-%d-%d", version, qos),
						CleanStart: true,
						Version:    version,
						KeepAlive:  10 * time.Second,
					})
					publishGMQX(t, pubClient, topic, payload, qos, false)

					got := sub.next(t, fmt.Sprintf("the QoS %d delivery from mosquitto", qos))
					if got.topic != topic {
						t.Fatalf("delivered topic = %q, want %q", got.topic, topic)
					}
					if string(got.payload) != string(payload) {
						t.Fatalf("delivered payload = %s, want %q", describe(got.payload), payload)
					}
					if got.qos != qos {
						t.Fatalf("delivered QoS = %d, want %d", got.qos, qos)
					}
					if got.retain {
						t.Fatal("a live delivery carried the retain flag")
					}
					if qos == 2 {
						sub.expectNone(t, "mosquitto delivered the QoS 2 message exactly once", quietWindow)
					}
				})
			}
		}
	})

	t.Run("Retained message", func(t *testing.T) {
		topic := "interop/gmqx/retained"
		payload := []byte("gmqx-retained-payload")

		pubClient := dialGMQX(t, client.Config{
			Addr:       ref.addr,
			ClientID:   "gmqx-retained-pub",
			CleanStart: true,
			Version:    mqtt.Version5,
			KeepAlive:  10 * time.Second,
		})
		publishGMQX(t, pubClient, topic, payload, 1, true)
		t.Cleanup(func() {
			// Clear the retained state of the reference broker.
			ctx, cancel := context.WithTimeout(context.Background(), deliveryTimeout)
			defer cancel()
			_ = pubClient.Publish(ctx, topic, []byte{}, 1, true)
		})

		// This subscriber connects after the publication.
		sub := newSink()
		subClient := dialGMQX(t, client.Config{
			Addr:       ref.addr,
			ClientID:   "gmqx-retained-sub",
			CleanStart: true,
			Version:    mqtt.Version5,
			KeepAlive:  10 * time.Second,
			OnMessage:  sinkHandler(sub),
		})
		subscribeGMQX(t, subClient, topic, 1)

		got := sub.next(t, "the retained message from mosquitto")
		if string(got.payload) != string(payload) {
			t.Fatalf("retained payload = %s, want %q", describe(got.payload), payload)
		}
		if !got.retain {
			t.Fatal("the replayed retained message did not carry the retain flag")
		}
		sub.expectNone(t, "the retained message replayed once", quietWindow)
	})

	t.Run("Will", func(t *testing.T) {
		t.Run("abrupt disconnect publishes the will", func(t *testing.T) {
			topic := "interop/gmqx/will/abrupt"
			willPayload := "gmqx-will-abrupt"

			sub := newSink()
			subClient := dialGMQX(t, client.Config{
				Addr:       ref.addr,
				ClientID:   "gmqx-will-sub-abrupt",
				CleanStart: true,
				Version:    mqtt.Version5,
				KeepAlive:  10 * time.Second,
				OnMessage:  sinkHandler(sub),
			})
			subscribeGMQX(t, subClient, topic, 1)

			tap := &connTap{}
			willClient := dialGMQX(t, client.Config{
				Addr:       ref.addr,
				ClientID:   "gmqx-will-abrupt",
				CleanStart: true,
				Version:    mqtt.Version5,
				KeepAlive:  10 * time.Second,
				Dialer:     tap.dial,
				Will: &client.Will{
					Topic:   topic,
					Payload: []byte(willPayload),
					QoS:     1,
				},
			})
			if !willClient.Connected() {
				t.Fatal("the will client is not connected")
			}

			tap.kill(t)

			got := sub.next(t, "the will published by mosquitto")
			if got.topic != topic {
				t.Fatalf("will topic = %q, want %q", got.topic, topic)
			}
			if string(got.payload) != willPayload {
				t.Fatalf("will payload = %s, want %q", describe(got.payload), willPayload)
			}
			sub.expectNone(t, "the will published exactly once", quietWindow)
		})

		t.Run("clean disconnect publishes nothing", func(t *testing.T) {
			topic := "interop/gmqx/will/clean"

			sub := newSink()
			subClient := dialGMQX(t, client.Config{
				Addr:       ref.addr,
				ClientID:   "gmqx-will-sub-clean",
				CleanStart: true,
				Version:    mqtt.Version5,
				KeepAlive:  10 * time.Second,
				OnMessage:  sinkHandler(sub),
			})
			subscribeGMQX(t, subClient, topic, 1)

			willClient := dialGMQX(t, client.Config{
				Addr:       ref.addr,
				ClientID:   "gmqx-will-clean",
				CleanStart: true,
				Version:    mqtt.Version5,
				KeepAlive:  10 * time.Second,
				Will: &client.Will{
					Topic:   topic,
					Payload: []byte("gmqx-will-clean"),
					QoS:     1,
				},
			})
			if err := willClient.Close(); err != nil {
				t.Fatalf("close the will client: %v", err)
			}

			sub.expectNone(t, "a will after a clean DISCONNECT", quietWindow)

			// The subscriber is still live: a normal publication reaches it.
			pubClient := dialGMQX(t, client.Config{
				Addr:       ref.addr,
				ClientID:   "gmqx-will-clean-pub",
				CleanStart: true,
				Version:    mqtt.Version5,
				KeepAlive:  10 * time.Second,
			})
			publishGMQX(t, pubClient, topic, []byte("after-clean-disconnect"), 1, false)
			got := sub.next(t, "the control message")
			if string(got.payload) != "after-clean-disconnect" {
				t.Fatalf("control payload = %s", describe(got.payload))
			}
		})
	})
}

// TestBrokerAgainstReferenceClients is direction B: the gmqx broker on the host,
// with the reference clients running in the container. The broker listens on the
// wildcard address so the container can reach it through host.docker.internal;
// the container's own broker is not involved.
func TestBrokerAgainstReferenceClients(t *testing.T) {
	ref := startMosquitto(t)
	metrics := &subscribeCounter{}
	b := startBroker(t, gmqx.Config{Addr: ":0", Metrics: metrics})
	// Two views of one listener: the container reaches the host through
	// host.docker.internal, and a client on this machine uses loopback. The
	// broker's own address is the wildcard, which is not a dialable destination.
	port := brokerPort(t, b.Addr())
	target := net.JoinHostPort(hostAlias, port)
	localhost := net.JoinHostPort("127.0.0.1", port)

	t.Run("reference publisher to host subscriber", func(t *testing.T) {
		topic := "interop/ref/publish"
		payload := []byte("from-mosquitto-pub")

		sub := newSink()
		subClient := dialGMQX(t, client.Config{
			Addr:       localhost,
			ClientID:   "ref-pub-sub",
			CleanStart: true,
			Version:    mqtt.Version5,
			KeepAlive:  10 * time.Second,
			OnMessage:  sinkHandler(sub),
		})
		subscribeGMQX(t, subClient, topic, 2)

		ref.publish(t, target, topic, payload, 2, false, "")

		got := sub.next(t, "the publication of mosquitto_pub")
		if got.topic != topic {
			t.Fatalf("delivered topic = %q, want %q", got.topic, topic)
		}
		if string(got.payload) != string(payload) {
			t.Fatalf("delivered payload = %s, want %q", describe(got.payload), payload)
		}
		if got.qos != 2 {
			t.Fatalf("delivered QoS = %d, want 2", got.qos)
		}
	})

	t.Run("host publisher to reference subscriber", func(t *testing.T) {
		topic := "interop/ref/subscribe"
		payload := []byte("from-gmqx-client")

		before := metrics.subscribed()
		sub := ref.subscribe(t, target, topic, 1, "")
		waitFor(t, containerReadyTimeout, "the reference client's SUBSCRIBE to reach the broker", func() bool {
			return metrics.subscribed() > before
		})

		pubClient := dialGMQX(t, client.Config{
			Addr:       localhost,
			ClientID:   "ref-sub-pub",
			CleanStart: true,
			Version:    mqtt.Version5,
			KeepAlive:  10 * time.Second,
		})
		publishGMQX(t, pubClient, topic, payload, 1, false)

		out, err := sub.waitExit(t)
		if err != nil {
			t.Fatalf("mosquitto_sub exited with an error: %v\n%s", err, out)
		}
		if !strings.Contains(out, string(payload)) {
			t.Fatalf("mosquitto_sub did not receive %q; output:\n%s", payload, out)
		}
	})

	t.Run("retained reference publication to a late host subscriber", func(t *testing.T) {
		topic := "interop/ref/retained"
		payload := []byte("retained-from-mosquitto-pub")

		ref.publish(t, target, topic, payload, 1, true, "mqttv5")
		t.Cleanup(func() {
			// Clear the retained state of the gmqx broker.
			ctx, cancel := context.WithTimeout(context.Background(), referenceClientTimeout)
			defer cancel()
			host, port, _ := net.SplitHostPort(target)
			_, _ = ref.execReference(ctx, "mosquitto_pub", "-h", host, "-p", port, "-t", topic, "-m", "", "-r")
		})

		sub := newSink()
		subClient := dialGMQX(t, client.Config{
			Addr:       localhost,
			ClientID:   "ref-retained-sub",
			CleanStart: true,
			Version:    mqtt.Version5,
			KeepAlive:  10 * time.Second,
			OnMessage:  sinkHandler(sub),
		})
		subscribeGMQX(t, subClient, topic, 1)

		got := sub.next(t, "the retained message of the reference client")
		if string(got.payload) != string(payload) {
			t.Fatalf("retained payload = %s, want %q", describe(got.payload), payload)
		}
		if !got.retain {
			t.Fatal("the replayed retained message did not carry the retain flag")
		}
		sub.expectNone(t, "the retained message replayed once", quietWindow)
	})
}

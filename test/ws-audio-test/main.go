// ws-audio-test exercises Janus AudioBridge plain-RTP and core RTP-over-WebSocket (rtpws).
//
// Usage:
//
//	go run . signaling --janus-http http://127.0.0.1:8088/janus --token '...' --room demo
//	go run . plain-rtp --janus-http ... --token ... --room demo --local-ip 127.0.0.1 --tone
//	go run . ws-stream --janus-http ... --token ... --room demo --tone --expect-rx 10
//	go run . ws-e2e --janus-http ... --token ... --room demo --local-ip 127.0.0.1
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "signaling":
		runSignaling(os.Args[2:])
	case "plain-rtp":
		runPlainRTP(os.Args[2:])
	case "ws-stream":
		runWSStream(os.Args[2:])
	case "ws-e2e":
		runWSE2E(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Janus AudioBridge RTP / RTP-over-WebSocket test CLI

Commands:
  signaling   Create room + join, print media URLs
  plain-rtp   Join as plain RTP UDP; send and receive RTP with stats
  ws-stream   Join via media=websocket and stream binary RTP over WS/WSS
  ws-e2e      Plain-RTP feeder + WS media participant (validates outbound mix)

Janus core config (janus.jcfg):
  rtpws: { enable_rtp_ws = true, port = 8190, path = "/rtp-ws", ... }
  AudioBridge room: allow_ws_participants = true

Examples:
  go run . signaling --janus-http http://127.0.0.1:8088/janus --token "$TOKEN" --room test1 --media websocket
  go run . plain-rtp --janus-http http://127.0.0.1:8088/janus --token "$TOKEN" --room test1 --local-ip 127.0.0.1 --tone --expect-rx 50
  go run . ws-stream --janus-http http://127.0.0.1:8088/janus --token "$TOKEN" --room test1 --tone --expect-rx 50
  go run . ws-e2e --janus-http http://127.0.0.1:8088/janus --token "$TOKEN" --room test1 --local-ip 127.0.0.1

Flags (plain-rtp, ws-stream, ws-e2e):
  --tone           Send 440Hz RTP payload (PT 100, 20ms)
  --expect-rx N    Exit with error if fewer than N RTP packets received (default 0)
  --stats-interval Print tx/rx counters periodically (default 2s)

`)
}

type rtpCounters struct {
	txPkts  atomic.Uint64
	txBytes atomic.Uint64
	rxPkts  atomic.Uint64
	rxBytes atomic.Uint64
	rxBad   atomic.Uint64
}

func (c *rtpCounters) logSnapshot(label string) {
	log.Printf("[%s] tx=%d pkts (%d B) | rx=%d pkts (%d B) | rx_bad=%d",
		label, c.txPkts.Load(), c.txBytes.Load(), c.rxPkts.Load(), c.rxBytes.Load(), c.rxBad.Load())
}

func watchStats(ctx context.Context, c *rtpCounters, label string, interval time.Duration) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.logSnapshot(label)
		}
	}
}

func checkExpectRX(c *rtpCounters, expect uint64, label string) error {
	if expect == 0 {
		return nil
	}
	got := c.rxPkts.Load()
	if got < expect {
		return fmt.Errorf("[%s] expected >= %d inbound RTP packets, got %d", label, expect, got)
	}
	log.Printf("[%s] inbound RTP OK (%d >= %d)", label, got, expect)
	return nil
}

type janusClient struct {
	base       string
	token      string
	client     *http.Client
	pollClient *http.Client
	tx         atomic.Uint64
}

func newJanus(base, token string) *janusClient {
	return &janusClient{
		base:       base,
		token:      token,
		client:     &http.Client{Timeout: 20 * time.Second},
		pollClient: &http.Client{Timeout: 70 * time.Second},
	}
}

func (j *janusClient) nextTx() string {
	return fmt.Sprintf("tx-%d", j.tx.Add(1))
}

func (j *janusClient) post(url string, body map[string]any) (map[string]any, error) {
	body["transaction"] = j.nextTx()
	if j.token != "" {
		body["token"] = j.token
	}
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := j.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode %s: %w", string(raw), err)
	}
	if janus, _ := out["janus"].(string); janus == "error" {
		return nil, fmt.Errorf("janus error: %v", out["error"])
	}
	return out, nil
}

func (j *janusClient) poll(sessionURL string) (any, error) {
	req, err := http.NewRequest(http.MethodGet, sessionURL, nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	q.Set("maxev", "1")
	if j.token != "" {
		q.Set("token", j.token)
	}
	req.URL.RawQuery = q.Encode()
	res, err := j.pollClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode poll %s: %w", string(raw), err)
	}
	return out, nil
}

func pluginEventFromPollItem(item map[string]any, transaction string) map[string]any {
	if janus, _ := item["janus"].(string); janus == "keepalive" {
		return nil
	}
	ev := extractPluginEvent(item)
	if ev == nil {
		return nil
	}
	if transaction != "" {
		if tx, _ := item["transaction"].(string); tx != transaction {
			return nil
		}
	}
	ab, _ := ev["audiobridge"].(string)
	if ab != "joined" && ab != "event" {
		return nil
	}
	return ev
}

func (j *janusClient) resolvePluginEvent(postResp map[string]any, sessionURL string) (map[string]any, error) {
	if ev := extractPluginEvent(postResp); ev != nil {
		return ev, nil
	}
	if janus, _ := postResp["janus"].(string); janus != "ack" {
		return nil, fmt.Errorf("no plugin data: %s", mustJSON(postResp))
	}
	transaction, _ := postResp["transaction"].(string)
	for i := 0; i < 5; i++ {
		payload, err := j.poll(sessionURL)
		if err != nil {
			return nil, err
		}
		items := []map[string]any{}
		switch v := payload.(type) {
		case map[string]any:
			items = append(items, v)
		case []any:
			for _, raw := range v {
				if item, ok := raw.(map[string]any); ok {
					items = append(items, item)
				}
			}
		}
		for _, item := range items {
			if ev := pluginEventFromPollItem(item, transaction); ev != nil {
				return ev, nil
			}
		}
	}
	return nil, fmt.Errorf("join response missing plugin data after long-poll")
}

type session struct {
	j        *janusClient
	id       int64
	handle   int64
	room     string
	media    string
	localIP  string
	useTone  bool
	duration time.Duration
}

func (s *session) setup() error {
	create, err := s.j.post(s.j.base, map[string]any{"janus": "create"})
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	s.id = int64(create["data"].(map[string]any)["id"].(float64))

	attach, err := s.j.post(fmt.Sprintf("%s/%d", s.j.base, s.id), map[string]any{
		"janus":  "attach",
		"plugin": "janus.plugin.audiobridge",
	})
	if err != nil {
		return fmt.Errorf("attach: %w", err)
	}
	s.handle = int64(attach["data"].(map[string]any)["id"].(float64))

	handleURL := fmt.Sprintf("%s/%d/%d", s.j.base, s.id, s.handle)

	_, err = s.j.post(handleURL, map[string]any{
		"janus": "message",
		"body": map[string]any{
			"request":                "create",
			"room":                   s.room,
			"permanent":              false,
			"allow_rtp_participants": true,
			"allow_ws_participants":  true,
		},
	})
	if err != nil {
		log.Printf("create room (may exist): %v", err)
	}

	joinBody := map[string]any{
		"request": "join",
		"room":    s.room,
		"display": "ws-audio-test",
		"codec":   "opus",
		"muted":   false,
	}
	if s.media == "websocket" {
		joinBody["media"] = "websocket"
	}

	joinResp, err := s.j.post(handleURL, map[string]any{
		"janus": "message",
		"body":  joinBody,
	})
	if err != nil {
		return fmt.Errorf("join: %w", err)
	}
	log.Printf("join response: %s", mustJSON(joinResp))

	sessionURL := fmt.Sprintf("%s/%d", s.j.base, s.id)
	ev, err := s.j.resolvePluginEvent(joinResp, sessionURL)
	if err != nil {
		return fmt.Errorf("join: %w", err)
	}
	printMediaInfo(ev)
	return nil
}

func (s *session) joinWebSocket() (string, error) {
	create, err := s.j.post(s.j.base, map[string]any{"janus": "create"})
	if err != nil {
		return "", err
	}
	s.id = int64(create["data"].(map[string]any)["id"].(float64))
	attach, err := s.j.post(fmt.Sprintf("%s/%d", s.j.base, s.id), map[string]any{
		"janus": "attach", "plugin": "janus.plugin.audiobridge",
	})
	if err != nil {
		return "", err
	}
	s.handle = int64(attach["data"].(map[string]any)["id"].(float64))
	handleURL := fmt.Sprintf("%s/%d/%d", s.j.base, s.id, s.handle)

	_, _ = s.j.post(handleURL, map[string]any{
		"janus": "message",
		"body": map[string]any{
			"request":               "create",
			"room":                  s.room,
			"allow_rtp_participants": true,
			"allow_ws_participants":  true,
		},
	})

	joinResp, err := s.j.post(handleURL, map[string]any{
		"janus": "message",
		"body": map[string]any{
			"request": "join",
			"room":    s.room,
			"display": "ws-media-test",
			"codec":   "opus",
			"media":   "websocket",
		},
	})
	if err != nil {
		return "", err
	}
	sessionURL := fmt.Sprintf("%s/%d", s.j.base, s.id)
	ev, err := s.j.resolvePluginEvent(joinResp, sessionURL)
	if err != nil {
		return "", fmt.Errorf("join: %w", err)
	}
	ws, ok := ev["websocket_media"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("joined without websocket_media — enable rtpws::enable_rtp_ws in janus.jcfg and allow_ws_participants on room")
	}
	url, _ := ws["url"].(string)
	if url == "" {
		return "", fmt.Errorf("empty websocket_media.url")
	}
	log.Printf("websocket_media.url = %s", url)
	return url, nil
}

func extractPluginEvent(resp map[string]any) map[string]any {
	plug, ok := resp["plugindata"].(map[string]any)
	if !ok {
		return nil
	}
	data, ok := plug["data"].(map[string]any)
	if !ok {
		return nil
	}
	return data
}

func printMediaInfo(ev map[string]any) {
	if ws, ok := ev["websocket_media"].(map[string]any); ok {
		log.Printf("websocket_media.url = %v", ws["url"])
	}
	if rtpObj, ok := ev["rtp"].(map[string]any); ok {
		log.Printf("rtp ip=%v port=%v pt=%v", rtpObj["ip"], rtpObj["port"], rtpObj["payload_type"])
	}
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func runSignaling(args []string) {
	fs := flag.NewFlagSet("signaling", flag.ExitOnError)
	janusHTTP := fs.String("janus-http", envOr("JANUS_HTTP", "http://127.0.0.1:8088/janus"), "Janus HTTP base URL (env: JANUS_HTTP)")
	token := fs.String("token", envOr("TOKEN", ""), "HMAC Janus token (env: TOKEN)")
	room := fs.String("room", envOr("ROOM", "ws-audio-test"), "AudioBridge room id (env: ROOM)")
	media := fs.String("media", "websocket", "join media: websocket or webrtc")
	_ = fs.Parse(args)

	s := &session{j: newJanus(*janusHTTP, *token), room: *room, media: *media}
	if err := s.setup(); err != nil {
		log.Fatal(err)
	}
}

func runPlainRTP(args []string) {
	fs := flag.NewFlagSet("plain-rtp", flag.ExitOnError)
	janusHTTP := fs.String("janus-http", envOr("JANUS_HTTP", "http://127.0.0.1:8088/janus"), "Janus HTTP base URL (env: JANUS_HTTP)")
	token := fs.String("token", envOr("TOKEN", ""), "HMAC Janus token (env: TOKEN)")
	room := fs.String("room", envOr("ROOM", "ws-audio-test"), "AudioBridge room id (env: ROOM)")
	localIP := fs.String("local-ip", envOr("LOCAL_IP", "127.0.0.1"), "Local IP Janus sends RTP to (env: LOCAL_IP)")
	tone := fs.Bool("tone", false, "Send 440Hz tone as Opus RTP")
	duration := fs.Duration("duration", 30*time.Second, "Stream duration")
	expectRX := fs.Uint64("expect-rx", 0, "Fail if fewer inbound RTP packets received")
	statsInterval := fs.Duration("stats-interval", 2*time.Second, "Stats log interval")
	_ = fs.Parse(args)

	s := &session{
		j:        newJanus(*janusHTTP, *token),
		room:     *room,
		localIP:  *localIP,
		useTone:  *tone,
		duration: *duration,
	}

	conn, janusRTP, err := s.joinPlainRTP()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var counters rtpCounters
	go watchStats(ctx, &counters, "plain-rtp", *statsInterval)
	go readUDPRTP(ctx, conn, &counters, "plain-rtp")

	if *tone {
		go func() {
			if err := writeToneUDPRTP(ctx, conn, janusRTP, &counters, *duration); err != nil && ctx.Err() == nil {
				log.Printf("tx error: %v", err)
			}
		}()
	}

	<-ctx.Done()
	time.Sleep(200 * time.Millisecond)
	counters.logSnapshot("plain-rtp")
	if err := checkExpectRX(&counters, *expectRX, "plain-rtp"); err != nil {
		log.Fatal(err)
	}
}

func (s *session) joinPlainRTP() (*net.UDPConn, net.UDPAddr, error) {
	create, err := s.j.post(s.j.base, map[string]any{"janus": "create"})
	if err != nil {
		return nil, net.UDPAddr{}, err
	}
	s.id = int64(create["data"].(map[string]any)["id"].(float64))
	attach, err := s.j.post(fmt.Sprintf("%s/%d", s.j.base, s.id), map[string]any{
		"janus": "attach", "plugin": "janus.plugin.audiobridge",
	})
	if err != nil {
		return nil, net.UDPAddr{}, err
	}
	s.handle = int64(attach["data"].(map[string]any)["id"].(float64))
	handleURL := fmt.Sprintf("%s/%d/%d", s.j.base, s.id, s.handle)

	_, _ = s.j.post(handleURL, map[string]any{
		"janus": "message",
		"body": map[string]any{
			"request":                "create",
			"room":                   s.room,
			"allow_rtp_participants": true,
		},
	})

	udpAddr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		return nil, net.UDPAddr{}, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, net.UDPAddr{}, err
	}
	localPort := conn.LocalAddr().(*net.UDPAddr).Port
	log.Printf("local UDP %s:%d", s.localIP, localPort)

	joinResp, err := s.j.post(handleURL, map[string]any{
		"janus": "message",
		"body": map[string]any{
			"request": "join",
			"room":    s.room,
			"display": "plain-rtp-test",
			"codec":   "opus",
			"rtp": map[string]any{
				"ip":           s.localIP,
				"port":         localPort,
				"payload_type": 100,
			},
		},
	})
	if err != nil {
		conn.Close()
		return nil, net.UDPAddr{}, err
	}
	sessionURL := fmt.Sprintf("%s/%d", s.j.base, s.id)
	ev, err := s.j.resolvePluginEvent(joinResp, sessionURL)
	if err != nil {
		conn.Close()
		return nil, net.UDPAddr{}, fmt.Errorf("join: %w", err)
	}
	printMediaInfo(ev)
	rtpObj, ok := ev["rtp"].(map[string]any)
	if !ok {
		conn.Close()
		return nil, net.UDPAddr{}, fmt.Errorf("joined without rtp object — enable allow_rtp_participants")
	}
	janusIP := rtpObj["ip"].(string)
	janusPort := int(rtpObj["port"].(float64))
	remote, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", janusIP, janusPort))
	if err != nil {
		conn.Close()
		return nil, net.UDPAddr{}, err
	}
	log.Printf("send RTP to Janus %s", remote.String())
	return conn, *remote, nil
}

func runWSStream(args []string) {
	fs := flag.NewFlagSet("ws-stream", flag.ExitOnError)
	janusHTTP := fs.String("janus-http", envOr("JANUS_HTTP", ""), "Janus HTTP base URL (env: JANUS_HTTP)")
	token := fs.String("token", envOr("TOKEN", ""), "HMAC Janus token (env: TOKEN)")
	room := fs.String("room", envOr("ROOM", "ws-audio-test"), "AudioBridge room id (env: ROOM)")
	wsURL := fs.String("ws-url", "", "WebSocket media URL (optional if --janus-http is set)")
	tone := fs.Bool("tone", true, "Send tone as binary RTP frames")
	duration := fs.Duration("duration", 30*time.Second, "Stream duration")
	expectRX := fs.Uint64("expect-rx", 0, "Fail if fewer inbound RTP packets received")
	statsInterval := fs.Duration("stats-interval", 2*time.Second, "Stats log interval")
	_ = fs.Parse(args)

	url := *wsURL
	if url == "" {
		if *janusHTTP == "" {
			log.Fatal("either --ws-url or --janus-http is required")
		}
		s := &session{j: newJanus(*janusHTTP, *token), room: *room}
		var err error
		url, err = s.joinWebSocket()
		if err != nil {
			log.Fatal("join:", err)
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := runWSBidirectional(ctx, url, *tone, *duration, *expectRX, *statsInterval, "ws-stream"); err != nil {
		log.Fatal(err)
	}
}

func runWSE2E(args []string) {
	fs := flag.NewFlagSet("ws-e2e", flag.ExitOnError)
	janusHTTP := fs.String("janus-http", envOr("JANUS_HTTP", "http://127.0.0.1:8088/janus"), "Janus HTTP base URL (env: JANUS_HTTP)")
	token := fs.String("token", envOr("TOKEN", ""), "HMAC Janus token (env: TOKEN)")
	room := fs.String("room", envOr("ROOM", "ws-audio-test"), "AudioBridge room id (env: ROOM)")
	localIP := fs.String("local-ip", envOr("LOCAL_IP", "127.0.0.1"), "Local IP for plain-RTP feeder (env: LOCAL_IP)")
	duration := fs.Duration("duration", 30*time.Second, "Test duration")
	expectRX := fs.Uint64("expect-rx", 50, "Minimum inbound RTP on WS leg (mix from feeder)")
	statsInterval := fs.Duration("stats-interval", 2*time.Second, "Stats log interval")
	_ = fs.Parse(args)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	feeder := &session{j: newJanus(*janusHTTP, *token), room: *room, localIP: *localIP}
	conn, remote, err := feeder.joinPlainRTP()
	if err != nil {
		log.Fatal("feeder:", err)
	}
	defer conn.Close()

	var feederCounters rtpCounters
	go writeToneUDPRTP(ctx, conn, remote, &feederCounters, *duration)

	wsSess := &session{j: newJanus(*janusHTTP, *token), room: *room}
	wsURL, err := wsSess.joinWebSocket()
	if err != nil {
		log.Fatal("ws join:", err)
	}

	time.Sleep(500 * time.Millisecond)
	if err := runWSBidirectional(ctx, wsURL, false, *duration, *expectRX, *statsInterval, "ws-e2e"); err != nil {
		log.Fatal(err)
	}
}

func runWSBidirectional(ctx context.Context, wsURL string, sendTone bool, dur time.Duration, expectRX uint64, statsInterval time.Duration, label string) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		Subprotocols:     []string{"janus-rtp-ws"},
	}
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	log.Printf("[%s] connected %s", label, wsURL)

	_, msg, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("call_info: %w", err)
	}
	log.Printf("[%s] server: %s", label, string(msg))

	var counters rtpCounters
	go watchStats(ctx, &counters, label, statsInterval)
	go readWSRTP(ctx, conn, &counters, label)

	if sendTone {
		go func() {
			if err := writeToneWS(ctx, conn, &counters, dur); err != nil && ctx.Err() == nil {
				log.Printf("[%s] tx error: %v", label, err)
			}
		}()
	}

	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			counters.logSnapshot(label)
			return checkExpectRX(&counters, expectRX, label)
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	counters.logSnapshot(label)
	return checkExpectRX(&counters, expectRX, label)
}

func readUDPRTP(ctx context.Context, conn *net.UDPConn, c *rtpCounters, label string) {
	buf := make([]byte, 1500)
	var lastSeq uint16
	var haveSeq bool
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
		if !accountRTP(buf[:n], c, &lastSeq, &haveSeq) {
			log.Printf("[%s] invalid RTP %d bytes from %s", label, n, addr)
			continue
		}
		if c.rxPkts.Load()%50 == 1 {
			pkt := &rtp.Packet{}
			if pkt.Unmarshal(buf[:n]) == nil {
				log.Printf("[%s] rx seq=%d ts=%d pt=%d ssrc=%d (%d B) from %s",
					label, pkt.SequenceNumber, pkt.Timestamp, pkt.PayloadType, pkt.SSRC, n, addr)
			}
		}
	}
}

func readWSRTP(ctx context.Context, conn *websocket.Conn, c *rtpCounters, label string) {
	var lastSeq uint16
	var haveSeq bool
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		mt, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
		if mt != websocket.BinaryMessage {
			log.Printf("[%s] rx text: %s", label, string(data))
			continue
		}
		if !accountRTP(data, c, &lastSeq, &haveSeq) {
			log.Printf("[%s] invalid RTP %d bytes on WS", label, len(data))
			continue
		}
		if c.rxPkts.Load()%50 == 1 {
			pkt := &rtp.Packet{}
			if pkt.Unmarshal(data) == nil {
				log.Printf("[%s] rx seq=%d ts=%d pt=%d ssrc=%d (%d B)",
					label, pkt.SequenceNumber, pkt.Timestamp, pkt.PayloadType, pkt.SSRC, len(data))
			}
		}
	}
}

func accountRTP(data []byte, c *rtpCounters, lastSeq *uint16, haveSeq *bool) bool {
	if len(data) < 12 {
		c.rxBad.Add(1)
		return false
	}
	pkt := &rtp.Packet{}
	if err := pkt.Unmarshal(data); err != nil {
		c.rxBad.Add(1)
		return false
	}
	c.rxPkts.Add(1)
	c.rxBytes.Add(uint64(len(data)))
	if *haveSeq {
		exp := *lastSeq + 1
		if pkt.SequenceNumber != exp && c.rxPkts.Load() > 2 {
			log.Printf("rtp seq gap: expected %d got %d", exp, pkt.SequenceNumber)
		}
	}
	*lastSeq = pkt.SequenceNumber
	*haveSeq = true
	return true
}

func writeToneUDPRTP(ctx context.Context, conn *net.UDPConn, remote net.UDPAddr, c *rtpCounters, dur time.Duration) error {
	return writeToneRTP(ctx, dur, func(raw []byte) error {
		n, err := conn.WriteToUDP(raw, &remote)
		if err != nil {
			return err
		}
		c.txPkts.Add(1)
		c.txBytes.Add(uint64(n))
		return nil
	})
}

func writeToneWS(ctx context.Context, conn *websocket.Conn, c *rtpCounters, dur time.Duration) error {
	return writeToneRTP(ctx, dur, func(raw []byte) error {
		if err := conn.WriteMessage(websocket.BinaryMessage, raw); err != nil {
			return err
		}
		c.txPkts.Add(1)
		c.txBytes.Add(uint64(len(raw)))
		return nil
	})
}

func writeToneRTP(ctx context.Context, dur time.Duration, send func([]byte) error) error {
	pkt := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    100,
			SequenceNumber: 1,
			Timestamp:      0,
			SSRC:           0x12345678,
		},
		Payload: make([]byte, 160),
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(dur)
	phase := 0.0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if time.Now().After(deadline) {
				return nil
			}
			for i := range pkt.Payload {
				pkt.Payload[i] = byte(int(math.Sin(phase)*3000) + 128)
				phase += 2 * math.Pi * 440 / 8000
			}
			pkt.Header.SequenceNumber++
			pkt.Header.Timestamp += 160
			raw, err := pkt.Marshal()
			if err != nil {
				return err
			}
			if err := send(raw); err != nil {
				return err
			}
		}
	}
}

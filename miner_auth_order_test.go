package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestHandleAuthorize_AllowsBeforeSubscribe(t *testing.T) {
	conn := &writeRecorderConn{}
	mc := &MinerConn{
		id:           "auth-before-subscribe",
		ctx:          context.Background(),
		conn:         conn,
		cfg:          Config{ConnectionTimeout: time.Hour},
		lastActivity: time.Now(),
	}

	worker := "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa.worker"
	mc.handleAuthorizeID(1, worker, "")

	if !mc.authorized {
		t.Fatalf("expected authorized=true")
	}
	if mc.subscribed {
		t.Fatalf("expected subscribed=false")
	}
	if mc.listenerOn {
		t.Fatalf("expected listenerOn=false before subscribe")
	}
	if conn.closed {
		t.Fatalf("expected connection to remain open")
	}

	out := conn.String()
	if !strings.Contains(out, "\"result\":true") || !strings.Contains(out, "\"error\":null") {
		t.Fatalf("expected true authorize response, got: %q", out)
	}
}

func TestMinerConn_MiningAuthAlias(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	mc := &MinerConn{
		id:           "mining.auth-alias",
		ctx:          context.Background(),
		conn:         server,
		reader:       bufio.NewReader(server),
		cfg:          Config{ConnectionTimeout: time.Hour},
		lastActivity: time.Now(),
	}

	done := make(chan struct{})
	go func() {
		mc.handle()
		close(done)
	}()

	worker := "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa.worker"
	_, err := io.WriteString(client, `{"id":1,"method":"mining.auth","params":[`+strconvQuote(worker)+`,""]}`+"\n")
	if err != nil {
		t.Fatalf("write request: %v", err)
	}

	br := bufio.NewReader(client)
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	var resp StratumResponse
	if err := json.Unmarshal([]byte(line), &resp); err != nil {
		t.Fatalf("unmarshal response: %v; line=%q", err, line)
	}
	if resp.ID == nil {
		t.Fatalf("expected id in response")
	}
	if resp.Result != true || resp.Error != nil {
		t.Fatalf("expected result=true error=nil, got result=%#v error=%#v", resp.Result, resp.Error)
	}

	_ = client.Close()
	_ = server.Close()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatalf("miner conn did not exit")
	}
}

func TestMinerConn_SubscribeAuthorizeSendsInitialNotifyWithinOneSecond(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	jm := NewJobManager(nil, Config{}, nil, nil, nil)
	job := benchmarkSubmitJobForTest(t)
	jm.mu.Lock()
	jm.curJob = job
	jm.mu.Unlock()

	mc := NewMinerConn(
		context.Background(),
		server,
		jm,
		nil,
		Config{ConnectionTimeout: time.Hour, Extranonce2Size: 4, MinDifficulty: 1, DefaultDifficulty: 1},
		nil,
		nil,
		nil,
		nil,
		nil,
		false,
		nil,
	)

	done := make(chan struct{})
	go func() {
		mc.handle()
		close(done)
	}()

	br := bufio.NewReader(client)
	worker := "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa.worker"

	if _, err := io.WriteString(client, `{"id":1,"method":"mining.subscribe","params":["SparkMiner/1.0"]}`+"\n"); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatalf("read subscribe response: %v", err)
	}

	authorizedAt := time.Now()
	if _, err := io.WriteString(client, `{"id":2,"method":"mining.authorize","params":[`+strconvQuote(worker)+`,"x"]}`+"\n"); err != nil {
		t.Fatalf("write authorize: %v", err)
	}

	deadline := authorizedAt.Add(1 * time.Second)
	seenDifficulty := false
	seenNotify := false
	for !seenNotify {
		if err := client.SetReadDeadline(deadline); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("expected mining.notify within 1s of authorize: %v", err)
		}
		if strings.Contains(line, `"method":"mining.set_difficulty"`) {
			seenDifficulty = true
		}
		if strings.Contains(line, `"method":"mining.notify"`) {
			seenNotify = true
		}
		if time.Now().After(deadline) {
			break
		}
	}

	if !seenDifficulty {
		t.Fatalf("expected mining.set_difficulty after authorize")
	}
	if !seenNotify {
		t.Fatalf("expected mining.notify within 1s of authorize")
	}

	_ = client.Close()
	_ = server.Close()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatalf("miner conn did not exit")
	}
}

func strconvQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

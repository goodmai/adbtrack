package adbtrack

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeServer is a minimal adb server: it answers one host service per connection.
type fakeServer struct {
	ln       net.Listener
	mu       sync.Mutex
	requests []string
	// onTrack runs for a track-devices connection; write pushes one frame.
	onTrack func(write func(string), conn net.Conn)
	devices string
	fail    string
	wg      sync.WaitGroup
}

func newFake(t *testing.T) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeServer{ln: ln}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			f.wg.Add(1)
			go func() { defer f.wg.Done(); f.serve(c) }()
		}
	}()
	t.Cleanup(func() { ln.Close(); f.wg.Wait() })
	return f
}

func (f *fakeServer) addr() string { return f.ln.Addr().String() }

func (f *fakeServer) got() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.requests...)
}

func frame(s string) string { return fmt.Sprintf("%04x%s", len(s), s) }

func (f *fakeServer) serve(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	head := make([]byte, 4)
	if _, err := io.ReadFull(r, head); err != nil {
		return
	}
	var n int
	fmt.Sscanf(string(head), "%x", &n)
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return
	}
	f.mu.Lock()
	f.requests = append(f.requests, string(body))
	fail, devices, onTrack := f.fail, f.devices, f.onTrack
	f.mu.Unlock()
	if fail != "" {
		io.WriteString(c, "FAIL"+frame(fail))
		return
	}
	io.WriteString(c, "OKAY")
	switch string(body) {
	case "host:devices-l":
		io.WriteString(c, frame(devices))
	case "host:track-devices-l":
		if onTrack != nil {
			onTrack(func(s string) { io.WriteString(c, frame(s)) }, c)
		}
	}
}

const long = "R5CT1234567 device usb:1-1 product:frankel model:Pixel_10_Pro_XL device:frankel transport_id:3\n" +
	"192.168.18.140:5555 offline\n\nnoise\n"

func TestParseShortAndLong(t *testing.T) {
	got := Parse(long)
	want := []Device{
		{Serial: "R5CT1234567", State: "device", Model: "Pixel 10 Pro XL", Product: "frankel", Device: "frankel", TransportID: "3"},
		{Serial: "192.168.18.140:5555", State: "offline"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
	if s := Parse("abc\tunauthorized\n"); len(s) != 1 || s[0].State != StateUnauthorized {
		t.Fatalf("short format: %+v", s)
	}
	if Parse("") != nil {
		t.Fatal("empty payload must give no devices")
	}
}

func TestDeviceHelpers(t *testing.T) {
	if !(Device{State: "device"}).Ready() || (Device{State: "offline"}).Ready() {
		t.Fatal("Ready")
	}
	for serial, want := range map[string]bool{"192.0.2.5:5555": true, "[::1]:5555": true, "R5CT123": false, "emulator-5554": false, "host.local:5555": false} {
		if got := (Device{Serial: serial}).Wireless(); got != want {
			t.Errorf("Wireless(%q) = %v", serial, got)
		}
	}
}

func TestDiff(t *testing.T) {
	a := []Device{{Serial: "a", State: "device"}, {Serial: "b", State: "offline"}, {Serial: "gone", State: "device"}}
	b := []Device{{Serial: "a", State: "device"}, {Serial: "b", State: "device"}, {Serial: "new", State: "unauthorized"}}
	got := Diff(a, b)
	want := []Change{
		{Kind: StateChanged, Device: b[1], Old: "offline"},
		{Kind: Added, Device: b[2]},
		{Kind: Removed, Device: a[2]},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
	if len(Diff(a, a)) != 0 || len(Diff(nil, nil)) != 0 {
		t.Fatal("identical lists must produce no changes")
	}
	if Added.String() != "added" || StateChanged.String() != "state-changed" {
		t.Fatal("String")
	}
}

func TestDevicesOneShot(t *testing.T) {
	f := newFake(t)
	f.devices = long
	devs, err := (&Client{Addr: f.addr()}).Devices(context.Background())
	if err != nil || len(devs) != 2 || devs[0].Serial != "R5CT1234567" {
		t.Fatalf("%+v %v", devs, err)
	}
	if r := f.got(); len(r) != 1 || r[0] != "host:devices-l" {
		t.Fatalf("requests %v", r)
	}
}

func TestServerFailIsAnError(t *testing.T) {
	f := newFake(t)
	f.fail = "unknown host service"
	_, err := (&Client{Addr: f.addr()}).Devices(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unknown host service") {
		t.Fatalf("%v", err)
	}
}

func TestDevicesUnreachableServer(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	if _, err := (&Client{Addr: addr, DialTimeout: 200 * time.Millisecond}).Devices(context.Background()); err == nil {
		t.Fatal("a closed port must be an error")
	}
}

func recv(t *testing.T, ch <-chan []Device) []Device {
	t.Helper()
	select {
	case d, ok := <-ch:
		if !ok {
			t.Fatal("channel closed early")
		}
		return d
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for a device list")
		return nil
	}
}

func TestTrackPushesEveryChange(t *testing.T) {
	f := newFake(t)
	push := make(chan string)
	f.onTrack = func(write func(string), conn net.Conn) {
		write("R5 offline\n")
		for s := range push {
			write(s)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch := (&Client{Addr: f.addr()}).Track(ctx)
	if d := recv(t, ch); len(d) != 1 || d[0].State != "offline" {
		t.Fatalf("first: %+v", d)
	}
	push <- "R5 device model:Pixel\n"
	if d := recv(t, ch); d[0].State != "device" || d[0].Model != "Pixel" {
		t.Fatalf("second: %+v", d)
	}
	push <- ""
	if d := recv(t, ch); len(d) != 0 {
		t.Fatalf("unplugged must be an empty list: %+v", d)
	}
	cancel()
	close(push)
	for range ch { // drained and closed after cancel
	}
}

func TestTrackReconnectsAfterServerDropsAndReportsDown(t *testing.T) {
	f := newFake(t)
	var mu sync.Mutex
	round := 0
	f.onTrack = func(write func(string), conn net.Conn) {
		mu.Lock()
		round++
		r := round
		mu.Unlock()
		write(fmt.Sprintf("dev%d device\n", r))
		if r == 1 {
			conn.Close() // the adb server restarted
			return
		}
		io.Copy(io.Discard, conn) // hold the stream open until the client hangs up
	}
	down := make(chan error, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &Client{Addr: f.addr(), Backoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond, OnServerDown: func(err error) { down <- err }}
	ch := c.Track(ctx)
	if d := recv(t, ch); d[0].Serial != "dev1" {
		t.Fatalf("%+v", d)
	}
	if d := recv(t, ch); d[0].Serial != "dev2" {
		t.Fatalf("after reconnect: %+v", d)
	}
	select {
	case <-down:
	case <-time.After(time.Second):
		t.Fatal("OnServerDown must fire when the stream drops")
	}
}

func TestTrackStopsPromptlyOnCancelWhileBlockedInRead(t *testing.T) {
	f := newFake(t)
	f.onTrack = func(write func(string), conn net.Conn) { write("a device\n"); io.Copy(io.Discard, conn) }
	ctx, cancel := context.WithCancel(context.Background())
	ch := (&Client{Addr: f.addr()}).Track(ctx)
	recv(t, ch)
	start := time.Now()
	cancel()
	for range ch {
	}
	if time.Since(start) > time.Second {
		t.Fatalf("cancel took %v", time.Since(start))
	}
}

func TestTrackKeepsRetryingWhileServerIsDown(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	var mu sync.Mutex
	calls := 0
	ctx, cancel := context.WithCancel(context.Background())
	ch := (&Client{Addr: addr, DialTimeout: 100 * time.Millisecond, Backoff: 5 * time.Millisecond, MaxBackoff: 10 * time.Millisecond,
		OnServerDown: func(error) { mu.Lock(); calls++; mu.Unlock() }}).Track(ctx)
	time.Sleep(150 * time.Millisecond)
	cancel()
	for range ch {
	}
	mu.Lock()
	defer mu.Unlock()
	if calls < 3 {
		t.Fatalf("expected repeated retries, got %d", calls)
	}
}

func TestBadFramesAreRejected(t *testing.T) {
	for name, in := range map[string]string{"not hex": "zzzz", "too big": "ffffff", "truncated": "0010abc", "empty": ""} {
		if _, err := readFrame(bufio.NewReader(strings.NewReader(in))); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func FuzzParse(f *testing.F) {
	for _, s := range []string{long, "", "a\tdevice\n", "\n\n", "x y model:", "a b c:d:e"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		for _, d := range Parse(s) {
			if d.Serial == "" || d.State == "" {
				t.Fatalf("empty field in %+v from %q", d, s)
			}
		}
	})
}

func FuzzReadFrame(f *testing.F) {
	f.Add("0005hello")
	f.Add("ffff")
	f.Fuzz(func(t *testing.T, s string) { _, _ = readFrame(bufio.NewReader(strings.NewReader(s))) })
}

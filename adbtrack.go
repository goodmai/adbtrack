// Package adbtrack watches Android devices through the adb server protocol, without spawning adb.
//
// It speaks to the adb server (default 127.0.0.1:5037) directly: [Client.Devices] is the one-shot
// `adb devices -l`, and [Client.Track] is `host:track-devices-l`, which pushes a fresh device list
// the moment a phone is plugged in, unplugged, authorised or switches transport. That replaces
// polling `adb devices` in a loop.
package adbtrack

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// DefaultAddr is where the adb server listens unless ADB_SERVER_SOCKET says otherwise.
const DefaultAddr = "127.0.0.1:5037"

// State values adb reports; any other string is passed through unchanged.
const (
	StateDevice       = "device"
	StateOffline      = "offline"
	StateUnauthorized = "unauthorized"
)

// Device is one entry of the adb device list.
type Device struct {
	Serial string
	State  string
	// Model, Product, Device and TransportID come from the `-l` long format; empty when absent.
	Model, Product, Device, TransportID string
}

// Ready reports whether the device is authorised and online.
func (d Device) Ready() bool { return d.State == StateDevice }

// Wireless reports whether the serial looks like an IP endpoint (adb over Wi-Fi), such as 192.0.2.5:5555.
func (d Device) Wireless() bool {
	host, _, err := net.SplitHostPort(d.Serial)
	return err == nil && net.ParseIP(host) != nil
}

// Client talks to an adb server. The zero value uses [DefaultAddr].
type Client struct {
	// Addr of the adb server; empty means DefaultAddr.
	Addr string
	// DialTimeout bounds one connection attempt; zero means 3 seconds.
	DialTimeout time.Duration
	// Backoff is the pause before reconnecting a dropped Track stream; zero means 1 second.
	// It doubles up to MaxBackoff while the server stays unreachable.
	Backoff, MaxBackoff time.Duration
	// OnServerDown is called when the server cannot be reached, before waiting to retry.
	// A typical use is to run `adb start-server`. It must not block for long.
	OnServerDown func(err error)
}

func (c *Client) addr() string {
	if c.Addr == "" {
		return DefaultAddr
	}
	return c.Addr
}

// Devices returns the current device list (`host:devices-l`).
func (c *Client) Devices(ctx context.Context) ([]Device, error) {
	conn, err := c.open(ctx, "host:devices-l")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	payload, err := readFrame(bufio.NewReader(conn))
	if err != nil {
		return nil, err
	}
	return Parse(payload), nil
}

// Track streams the device list: once at start and again on every change. The channel is closed when
// ctx ends. A dropped connection or a stopped adb server is retried with backoff and never closes the
// channel; after a reconnect the fresh list is sent again, so a consumer that diffs with [Diff] sees
// devices that came or went while the link was down.
func (c *Client) Track(ctx context.Context) <-chan []Device {
	out := make(chan []Device)
	go func() {
		defer close(out)
		wait := c.backoff()
		for ctx.Err() == nil {
			err := c.trackOnce(ctx, out)
			if ctx.Err() != nil {
				return
			}
			if err != nil && c.OnServerDown != nil {
				c.OnServerDown(err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			if wait *= 2; wait > c.maxBackoff() {
				wait = c.maxBackoff()
			}
		}
	}()
	return out
}

func (c *Client) trackOnce(ctx context.Context, out chan<- []Device) error {
	conn, err := c.open(ctx, "host:track-devices-l")
	if err != nil {
		return err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() }) // unblock the read on cancel
	defer stop()
	r := bufio.NewReader(conn)
	for {
		payload, err := readFrame(r)
		if err != nil {
			return err
		}
		select {
		case out <- Parse(payload):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *Client) backoff() time.Duration {
	if c.Backoff > 0 {
		return c.Backoff
	}
	return time.Second
}

func (c *Client) maxBackoff() time.Duration {
	if c.MaxBackoff > 0 {
		return c.MaxBackoff
	}
	return 10 * time.Second
}

// open connects, sends a host service request and consumes the OKAY/FAIL status.
func (c *Client) open(ctx context.Context, service string) (net.Conn, error) {
	timeout := c.DialTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", c.addr())
	if err != nil {
		return nil, err
	}
	if _, err := fmt.Fprintf(conn, "%04x%s", len(service), service); err != nil {
		conn.Close()
		return nil, err
	}
	status := make([]byte, 4)
	if _, err := io.ReadFull(conn, status); err != nil {
		conn.Close()
		return nil, err
	}
	switch string(status) {
	case "OKAY":
		return conn, nil
	case "FAIL":
		msg, err := readFrame(bufio.NewReader(conn))
		conn.Close()
		if err != nil {
			return nil, fmt.Errorf("adb: %s failed", service)
		}
		return nil, fmt.Errorf("adb: %s", msg)
	default:
		conn.Close()
		return nil, fmt.Errorf("adb: unexpected reply %q", status)
	}
}

// maxFrame bounds one length-prefixed payload; a real device list is a few kilobytes.
const maxFrame = 1 << 20

// readFrame reads a 4-hex-digit length and that many bytes.
func readFrame(r *bufio.Reader) (string, error) {
	var head [4]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return "", err
	}
	n, err := strconv.ParseUint(string(head[:]), 16, 32)
	if err != nil {
		return "", fmt.Errorf("adb: bad frame length %q", head[:])
	}
	if n > maxFrame {
		return "", errors.New("adb: frame too large")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// Parse reads a device list in either the short (`serial\tstate`) or the long (`-l`) format. Blank and
// malformed lines are skipped.
func Parse(payload string) []Device {
	var devices []Device
	for _, line := range strings.Split(payload, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		d := Device{Serial: f[0], State: f[1]}
		for _, kv := range f[2:] {
			k, v, ok := strings.Cut(kv, ":")
			if !ok {
				continue
			}
			switch k {
			case "model":
				d.Model = strings.ReplaceAll(v, "_", " ")
			case "product":
				d.Product = v
			case "device":
				d.Device = v
			case "transport_id":
				d.TransportID = v
			}
		}
		devices = append(devices, d)
	}
	return devices
}

// ChangeKind says what happened to a device between two snapshots.
type ChangeKind int

const (
	Added ChangeKind = iota + 1
	Removed
	StateChanged
)

func (k ChangeKind) String() string {
	return map[ChangeKind]string{Added: "added", Removed: "removed", StateChanged: "state-changed"}[k]
}

// Change is one difference between two device lists.
type Change struct {
	Kind   ChangeKind
	Device Device // the new device for Added and StateChanged, the old one for Removed
	Old    string // previous state, set for StateChanged
}

// Diff lists what changed from prev to next, in the order of the lists.
func Diff(prev, next []Device) []Change {
	old := make(map[string]Device, len(prev))
	for _, d := range prev {
		old[d.Serial] = d
	}
	seen := make(map[string]bool, len(next))
	var changes []Change
	for _, d := range next {
		seen[d.Serial] = true
		switch o, ok := old[d.Serial]; {
		case !ok:
			changes = append(changes, Change{Kind: Added, Device: d})
		case o.State != d.State:
			changes = append(changes, Change{Kind: StateChanged, Device: d, Old: o.State})
		}
	}
	for _, d := range prev {
		if !seen[d.Serial] {
			changes = append(changes, Change{Kind: Removed, Device: d})
		}
	}
	return changes
}

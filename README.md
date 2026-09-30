# adbtrack

Watch Android devices through the adb server protocol, in pure Go, without spawning `adb`.

`adb devices` in a loop is slow, races with the server and costs a process per poll. `adbtrack`
opens one connection to the adb server (`127.0.0.1:5037`) and uses `host:track-devices-l`, so the
server pushes a new list the moment a phone is plugged in, unplugged, authorised or changes transport.

```go
c := &adbtrack.Client{}                 // zero value: 127.0.0.1:5037
var prev []adbtrack.Device
for devs := range c.Track(ctx) {        // reconnects with backoff, closed when ctx ends
    for _, ch := range adbtrack.Diff(prev, devs) {
        fmt.Println(ch.Kind, ch.Device.Serial, ch.Device.State)
    }
    prev = devs
}
```

* `Client.Track(ctx)` streams the full device list on every change and after every reconnect, so
  a consumer that diffs with `Diff` never misses a device that came or went while the link was down.
* `Client.Devices(ctx)` is the one-shot `adb devices -l`.
* `Client.OnServerDown` fires when the server is unreachable; run `adb start-server` there if you want.
* `Device` has `Serial`, `State`, `Model`, `Product`, `Device`, `TransportID`, `Ready()` and `Wireless()`.
* No dependencies (goleak is used by the tests only). Go 1.24+.

Try it: `go run github.com/goodmai/adbtrack/cmd/adbwatch@latest`, then plug a phone in and out.

## Scope

Only the host services needed to list and track devices. It does not push files, run shell commands
or forward ports; use `adb` or a full client for that. The wire format is documented in the Android
source, `adb/OVERVIEW.TXT` and `SERVICES.TXT`.

## Development

```bash
go test -race -count=3 ./...
go test -run '^$' -fuzz FuzzParse -fuzztime 20s .
```

MIT, see [LICENSE](LICENSE).

// Command adbwatch prints device changes as they happen: plug a phone in and out to see it.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/goodmai/adbtrack"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	c := &adbtrack.Client{OnServerDown: func(err error) { fmt.Fprintln(os.Stderr, "adb server unreachable:", err) }}
	var prev []adbtrack.Device
	for devs := range c.Track(ctx) {
		for _, ch := range adbtrack.Diff(prev, devs) {
			fmt.Printf("%-13s %s %s %s\n", ch.Kind, ch.Device.Serial, ch.Device.State, ch.Device.Model)
		}
		prev = devs
	}
}

package adbtrack_test

import (
	"context"
	"fmt"

	"github.com/goodmai/adbtrack"
)

// Start a session when a phone becomes ready and stop it when it goes away.
func ExampleClient_Track() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var prev []adbtrack.Device
	for devs := range (&adbtrack.Client{}).Track(ctx) {
		for _, ch := range adbtrack.Diff(prev, devs) {
			switch {
			case ch.Kind == adbtrack.Removed:
				fmt.Println("gone:", ch.Device.Serial)
			case ch.Device.Ready():
				fmt.Println("ready:", ch.Device.Serial, ch.Device.Model)
			}
		}
		prev = devs
	}
}

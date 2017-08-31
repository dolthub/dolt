package dht

import (
	"context"
	"testing"
)

func TestNotifieeMultipleConn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d1 := setupDHT(ctx, t, false)
	d2 := setupDHT(ctx, t, false)

	nn1 := (*netNotifiee)(d1)
	nn2 := (*netNotifiee)(d2)

	connect(t, ctx, d1, d2)
	c12 := d1.host.Network().ConnsToPeer(d2.self)[0]
	c21 := d2.host.Network().ConnsToPeer(d1.self)[0]

	// Pretend to reestablish/re-kill connection
	nn1.Connected(d1.host.Network(), c12)
	nn2.Connected(d2.host.Network(), c21)

	if !checkRoutingTable(d1, d2) {
		t.Fatal("no routes")
	}
	nn1.Disconnected(d1.host.Network(), c12)
	nn2.Disconnected(d2.host.Network(), c21)

	if !checkRoutingTable(d1, d2) {
		t.Fatal("no routes")
	}

	for _, conn := range d1.host.Network().ConnsToPeer(d2.self) {
		conn.Close()
	}
	for _, conn := range d2.host.Network().ConnsToPeer(d1.self) {
		conn.Close()
	}

	if checkRoutingTable(d1, d2) {
		t.Fatal("routes")
	}
}

func TestNotifieeFuzz(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d1 := setupDHT(ctx, t, false)
	d2 := setupDHT(ctx, t, false)

	for i := 0; i < 100; i++ {
		connectNoSync(t, ctx, d1, d2)
		for _, conn := range d1.host.Network().ConnsToPeer(d2.self) {
			conn.Close()
		}
	}
	if checkRoutingTable(d1, d2) {
		t.Fatal("should not have routes")
	}
	connect(t, ctx, d1, d2)
}

func checkRoutingTable(a, b *IpfsDHT) bool {
	// loop until connection notification has been received.
	// under high load, this may not happen as immediately as we would like.
	return a.routingTable.Find(b.self) != "" && b.routingTable.Find(a.self) != ""
}

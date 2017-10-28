package connmgr

import (
	"context"
	"sort"
	"sync"
	"time"

	inet "gx/ipfs/QmNa31VPzC561NWwRsJLE7nGYZYuuD2QfpK2b1q9BK54J1/go-libp2p-net"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
)

var log = logging.Logger("connmgr")

type ConnManager interface {
	TagPeer(peer.ID, string, int)
	UntagPeer(peer.ID, string)
	GetTagInfo(peer.ID) *TagInfo
	TrimOpenConns(context.Context)
	Notifee() inet.Notifiee
}

type connManager struct {
	highWater int
	lowWater  int

	gracePeriod time.Duration

	peers     map[peer.ID]*peerInfo
	connCount int

	lk sync.Mutex

	lastTrim time.Time
}

var DefaultGracePeriod = time.Second * 10

func NewConnManager(low, hi int, grace time.Duration) ConnManager {
	return &connManager{
		highWater:   hi,
		lowWater:    low,
		gracePeriod: grace,
		peers:       make(map[peer.ID]*peerInfo),
	}
}

type peerInfo struct {
	tags  map[string]int
	value int

	conns map[inet.Conn]time.Time

	firstSeen time.Time
}

type TagInfo struct {
	FirstSeen time.Time
	Value     int
	Tags      map[string]int
	Conns     map[string]time.Time
}

func (cm *connManager) TrimOpenConns(ctx context.Context) {
	cm.lk.Lock()
	defer cm.lk.Unlock()
	defer log.EventBegin(ctx, "connCleanup").Done()
	if cm.lowWater == 0 || cm.highWater == 0 {
		// disabled
		return
	}
	cm.lastTrim = time.Now()

	if len(cm.peers) < cm.lowWater {
		log.Info("open connection count below limit")
		return
	}

	var infos []*peerInfo

	for _, inf := range cm.peers {
		infos = append(infos, inf)
	}

	sort.Slice(infos, func(i, j int) bool {
		return infos[i].value < infos[j].value
	})

	close_count := len(infos) - cm.lowWater
	toclose := infos[:close_count]

	for _, inf := range toclose {
		if time.Since(inf.firstSeen) < cm.gracePeriod {
			continue
		}

		// TODO: if a peer has more than one connection, maybe only close one?
		for c, _ := range inf.conns {
			log.Info("closing conn: ", c.RemotePeer())
			log.Event(ctx, "closeConn", c.RemotePeer())
			c.Close()
		}
	}

	if len(cm.peers) > cm.highWater {
		log.Error("still over high water mark after trimming connections")
	}
}

func (cm *connManager) GetTagInfo(p peer.ID) *TagInfo {
	cm.lk.Lock()
	defer cm.lk.Unlock()

	pi, ok := cm.peers[p]
	if !ok {
		return nil
	}

	out := &TagInfo{
		FirstSeen: pi.firstSeen,
		Value:     pi.value,
		Tags:      make(map[string]int),
		Conns:     make(map[string]time.Time),
	}

	for t, v := range pi.tags {
		out.Tags[t] = v
	}
	for c, t := range pi.conns {
		out.Conns[c.RemoteMultiaddr().String()] = t
	}

	return out
}

func (cm *connManager) TagPeer(p peer.ID, tag string, val int) {
	cm.lk.Lock()
	defer cm.lk.Unlock()

	pi, ok := cm.peers[p]
	if !ok {
		log.Error("tried to tag conn from untracked peer: ", p)
		return
	}

	pi.value += (val - pi.tags[tag])
	pi.tags[tag] = val
}

func (cm *connManager) UntagPeer(p peer.ID, tag string) {
	cm.lk.Lock()
	defer cm.lk.Unlock()

	pi, ok := cm.peers[p]
	if !ok {
		log.Error("tried to remove tag from untracked peer: ", p)
		return
	}

	pi.value -= pi.tags[tag]
	delete(pi.tags, tag)
}

func (cm *connManager) Notifee() inet.Notifiee {
	return (*cmNotifee)(cm)
}

type cmNotifee connManager

func (nn *cmNotifee) cm() *connManager {
	return (*connManager)(nn)
}

func (nn *cmNotifee) Connected(n inet.Network, c inet.Conn) {
	cm := nn.cm()

	cm.lk.Lock()
	defer cm.lk.Unlock()

	pinfo, ok := cm.peers[c.RemotePeer()]
	if !ok {
		pinfo = &peerInfo{
			firstSeen: time.Now(),
			tags:      make(map[string]int),
			conns:     make(map[inet.Conn]time.Time),
		}
		cm.peers[c.RemotePeer()] = pinfo
	}

	_, ok = pinfo.conns[c]
	if ok {
		log.Error("received connected notification for conn we are already tracking: ", c.RemotePeer())
		return
	}

	pinfo.conns[c] = time.Now()
	cm.connCount++

	if cm.connCount > nn.highWater {
		if cm.lastTrim.IsZero() || time.Since(cm.lastTrim) > time.Second*10 {
			go cm.TrimOpenConns(context.Background())
		}
	}
}

func (nn *cmNotifee) Disconnected(n inet.Network, c inet.Conn) {
	cm := nn.cm()

	cm.lk.Lock()
	defer cm.lk.Unlock()

	cinf, ok := cm.peers[c.RemotePeer()]
	if !ok {
		log.Error("received disconnected notification for peer we are not tracking: ", c.RemotePeer())
		return
	}

	_, ok = cinf.conns[c]
	if !ok {
		log.Error("received disconnected notification for conn we are not tracking: ", c.RemotePeer())
		return
	}

	delete(cinf.conns, c)
	cm.connCount--
	if len(cinf.conns) == 0 {
		delete(cm.peers, c.RemotePeer())
	}
}

func (nn *cmNotifee) Listen(n inet.Network, addr ma.Multiaddr)      {}
func (nn *cmNotifee) ListenClose(n inet.Network, addr ma.Multiaddr) {}
func (nn *cmNotifee) OpenedStream(inet.Network, inet.Stream)        {}
func (nn *cmNotifee) ClosedStream(inet.Network, inet.Stream)        {}

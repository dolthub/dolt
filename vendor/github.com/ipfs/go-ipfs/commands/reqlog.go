package commands

import (
	"strings"
	"sync"
	"time"
)

type ReqLogEntry struct {
	StartTime time.Time
	EndTime   time.Time
	Active    bool
	Command   string
	Options   map[string]interface{}
	Args      []string
	ID        int

	req Request
	log *ReqLog
}

func (r *ReqLogEntry) Finish() {
	log := r.log
	log.lock.Lock()
	defer log.lock.Unlock()

	r.Active = false
	r.EndTime = time.Now()
	r.log.maybeCleanup()

	// remove references to save memory
	r.req = nil
	r.log = nil

}

func (r *ReqLogEntry) Copy() *ReqLogEntry {
	out := *r
	out.log = nil
	return &out
}

type ReqLog struct {
	Requests []*ReqLogEntry
	nextID   int
	lock     sync.Mutex
	keep     time.Duration
}

func (rl *ReqLog) Add(req Request) *ReqLogEntry {
	rl.lock.Lock()
	defer rl.lock.Unlock()

	rle := &ReqLogEntry{
		StartTime: time.Now(),
		Active:    true,
		Command:   strings.Join(req.Path(), "/"),
		Options:   req.Options(),
		Args:      req.StringArguments(),
		ID:        rl.nextID,
		req:       req,
		log:       rl,
	}

	rl.nextID++
	rl.Requests = append(rl.Requests, rle)
	return rle
}

func (rl *ReqLog) ClearInactive() {
	rl.lock.Lock()
	defer rl.lock.Unlock()
	k := rl.keep
	rl.keep = 0
	rl.cleanup()
	rl.keep = k
}

func (rl *ReqLog) maybeCleanup() {
	// only do it every so often or it might
	// become a perf issue
	if len(rl.Requests)%10 == 0 {
		rl.cleanup()
	}
}

func (rl *ReqLog) cleanup() {
	i := 0
	now := time.Now()
	for j := 0; j < len(rl.Requests); j++ {
		rj := rl.Requests[j]
		if rj.Active || rl.Requests[j].EndTime.Add(rl.keep).After(now) {
			rl.Requests[i] = rl.Requests[j]
			i++
		}
	}
	rl.Requests = rl.Requests[:i]
}

func (rl *ReqLog) SetKeepTime(t time.Duration) {
	rl.lock.Lock()
	defer rl.lock.Unlock()
	rl.keep = t
}

// Report generates a copy of all the entries in the requestlog
func (rl *ReqLog) Report() []*ReqLogEntry {
	rl.lock.Lock()
	defer rl.lock.Unlock()
	out := make([]*ReqLogEntry, len(rl.Requests))

	for i, e := range rl.Requests {
		out[i] = e.Copy()
	}

	return out
}

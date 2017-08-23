// Copyright 2013, Chandra Sekar S.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the README.md file.

// Package pubsub implements a simple multi-topic pub-sub
// library.
//
// Topics must be strings and messages of any type can be
// published. A topic can have any number of subcribers and
// all of them receive messages published on the topic.
package pubsub

type operation int

const (
	sub operation = iota
	subOnce
	subOnceEach
	pub
	unsub
	unsubAll
	closeTopic
	shutdown
)

// PubSub is a collection of topics.
type PubSub struct {
	cmdChan  chan cmd
	capacity int
}

type cmd struct {
	op     operation
	topics []string
	ch     chan interface{}
	msg    interface{}
}

// New creates a new PubSub and starts a goroutine for handling operations.
// The capacity of the channels created by Sub and SubOnce will be as specified.
func New(capacity int) *PubSub {
	ps := &PubSub{make(chan cmd), capacity}
	go ps.start()
	return ps
}

// Sub returns a channel on which messages published on any of
// the specified topics can be received.
func (ps *PubSub) Sub(topics ...string) chan interface{} {
	return ps.sub(sub, topics...)
}

// SubOnce is similar to Sub, but only the first message published, after subscription,
// on any of the specified topics can be received.
func (ps *PubSub) SubOnce(topics ...string) chan interface{} {
	return ps.sub(subOnce, topics...)
}

// SubOnceEach returns a channel on which callers receive, at most, one message
// for each topic.
func (ps *PubSub) SubOnceEach(topics ...string) chan interface{} {
	return ps.sub(subOnceEach, topics...)
}

func (ps *PubSub) sub(op operation, topics ...string) chan interface{} {
	ch := make(chan interface{}, ps.capacity)
	ps.cmdChan <- cmd{op: op, topics: topics, ch: ch}
	return ch
}

// AddSub adds subscriptions to an existing channel.
func (ps *PubSub) AddSub(ch chan interface{}, topics ...string) {
	ps.cmdChan <- cmd{op: sub, topics: topics, ch: ch}
}

// AddSubOnceEach adds subscriptions to an existing channel with SubOnceEach
// behavior.
func (ps *PubSub) AddSubOnceEach(ch chan interface{}, topics ...string) {
	ps.cmdChan <- cmd{op: subOnceEach, topics: topics, ch: ch}
}

// Pub publishes the given message to all subscribers of
// the specified topics.
func (ps *PubSub) Pub(msg interface{}, topics ...string) {
	ps.cmdChan <- cmd{op: pub, topics: topics, msg: msg}
}

// Unsub unsubscribes the given channel from the specified
// topics. If no topic is specified, it is unsubscribed
// from all topics.
func (ps *PubSub) Unsub(ch chan interface{}, topics ...string) {
	if len(topics) == 0 {
		ps.cmdChan <- cmd{op: unsubAll, ch: ch}
		return
	}

	ps.cmdChan <- cmd{op: unsub, topics: topics, ch: ch}
}

// Close closes all channels currently subscribed to the specified topics.
// If a channel is subscribed to multiple topics, some of which is
// not specified, it is not closed.
func (ps *PubSub) Close(topics ...string) {
	ps.cmdChan <- cmd{op: closeTopic, topics: topics}
}

// Shutdown closes all subscribed channels and terminates the goroutine.
func (ps *PubSub) Shutdown() {
	ps.cmdChan <- cmd{op: shutdown}
}

func (ps *PubSub) start() {
	reg := registry{
		topics:    make(map[string]map[chan interface{}]subtype),
		revTopics: make(map[chan interface{}]map[string]bool),
	}

loop:
	for cmd := range ps.cmdChan {
		if cmd.topics == nil {
			switch cmd.op {
			case unsubAll:
				reg.removeChannel(cmd.ch)

			case shutdown:
				break loop
			}

			continue loop
		}

		for _, topic := range cmd.topics {
			switch cmd.op {
			case sub:
				reg.add(topic, cmd.ch, stNorm)

			case subOnce:
				reg.add(topic, cmd.ch, stOnceAny)

			case subOnceEach:
				reg.add(topic, cmd.ch, stOnceEach)

			case pub:
				reg.send(topic, cmd.msg)

			case unsub:
				reg.remove(topic, cmd.ch)

			case closeTopic:
				reg.removeTopic(topic)
			}
		}
	}

	for topic, chans := range reg.topics {
		for ch, _ := range chans {
			reg.remove(topic, ch)
		}
	}
}

// registry maintains the current subscription state. It's not
// safe to access a registry from multiple goroutines simultaneously.
type registry struct {
	topics    map[string]map[chan interface{}]subtype
	revTopics map[chan interface{}]map[string]bool
}

type subtype int

const (
	stOnceAny = iota
	stOnceEach
	stNorm
)

func (reg *registry) add(topic string, ch chan interface{}, st subtype) {
	if reg.topics[topic] == nil {
		reg.topics[topic] = make(map[chan interface{}]subtype)
	}
	reg.topics[topic][ch] = st

	if reg.revTopics[ch] == nil {
		reg.revTopics[ch] = make(map[string]bool)
	}
	reg.revTopics[ch][topic] = true
}

func (reg *registry) send(topic string, msg interface{}) {
	for ch, st := range reg.topics[topic] {
		ch <- msg
		switch st {
		case stOnceAny:
			for topic := range reg.revTopics[ch] {
				reg.remove(topic, ch)
			}
		case stOnceEach:
			reg.remove(topic, ch)
		}
	}
}

func (reg *registry) removeTopic(topic string) {
	for ch := range reg.topics[topic] {
		reg.remove(topic, ch)
	}
}

func (reg *registry) removeChannel(ch chan interface{}) {
	for topic := range reg.revTopics[ch] {
		reg.remove(topic, ch)
	}
}

func (reg *registry) remove(topic string, ch chan interface{}) {
	if _, ok := reg.topics[topic]; !ok {
		return
	}

	if _, ok := reg.topics[topic][ch]; !ok {
		return
	}

	delete(reg.topics[topic], ch)
	delete(reg.revTopics[ch], topic)

	if len(reg.topics[topic]) == 0 {
		delete(reg.topics, topic)
	}

	if len(reg.revTopics[ch]) == 0 {
		close(ch)
		delete(reg.revTopics, ch)
	}
}

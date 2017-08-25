// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/ipfs"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/math"
	"github.com/attic-labs/noms/samples/go/ipfs-chat/dbg"
	"github.com/jroimartin/gocui"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	all          = ""
	users        = "users"
	messages     = "messages"
	input        = "input"
	searchPrefix = "/s "
	quitPrefix   = "/q"
)

var (
	viewNames   = []string{users, messages, input}
	firstLayout = true
)

func main() {
	// allow short (-h) help
	kingpin.CommandLine.HelpFlag.Short('h')

	clientCmd := kingpin.Command("client", "runs the ipfs-chat client UI")
	clientTopic := clientCmd.Flag("topic", "IPFS pubsub topic to publish and subscribe to").Default("ipfs-chat").String()
	username := clientCmd.Flag("username", "username to sign in as").String()
	clientDS := clientCmd.Arg("dataset", "the dataset spec to store chat data in").Required().String()

	importCmd := kingpin.Command("import", "imports data into a chat")
	importDir := importCmd.Flag("dir", "directory that contains data to import").Default("./data").ExistingDir()
	importDS := importCmd.Arg("dataset", "the dataset spec to import chat data to").Required().String()

	daemonCmd := kingpin.Command("daemon", "runs a daemon that simulates filecoin, eagerly storing all chunks for a chat")
	daemonTopic := daemonCmd.Flag("topic", "IPFS pubsub topic to publish and subscribe to").Default("ipfs-chat").String()
	daemonInterval := daemonCmd.Flag("interval", "amount of time to wait before publishing state to network").Default("5s").Duration()
	daemonNetworkDS := daemonCmd.Arg("network-dataset", "the dataset spec to use to read and write data to the IPFS network").Required().String()
	daemonLocalDS := daemonCmd.Arg("local-dataset", "the dataset spec to use to read and write data locally").Required().String()

	kingpin.CommandLine.Help = "A demonstration of using Noms to build a scalable multiuser collaborative application."

	switch kingpin.Parse() {
	case "client":
		runClient(*username, *clientTopic, *clientDS)
	case "import":
		runImport(*importDir, *importDS)
	case "daemon":
		runDaemon(*daemonTopic, *daemonInterval, *daemonNetworkDS, *daemonLocalDS)
	}
}

func runClient(username, topic, clientDS string) {
	var displayingSearchResults = false

	dsSpec := clientDS
	sp, err := spec.ForDataset(dsSpec)
	d.CheckErrorNoUsage(err)
	ds := sp.GetDataset()

	ds, err = InitDatabase(ds)
	d.PanicIfError(err)

	g, err := gocui.NewGui(gocui.Output256)
	d.PanicIfError(err)
	defer g.Close()

	sub, err := ipfs.CurrentNode.Floodsub.Subscribe(topic)
	d.PanicIfError(err)
	go Replicate(sub, ds, ds, func(nds datas.Dataset) {
		ds = nds
		if displayingSearchResults || !textScrolledToEnd(g) {
			g.Execute(func(g *gocui.Gui) (err error) {
				updateViewTitle(g, messages, "messages (NEW!)")
				return
			})
		} else {
			updateMessagesAndRefresh(g, ds, nil, nil)
		}
	})

	g.Highlight = true
	g.SelFgColor = gocui.ColorGreen
	g.Cursor = true

	relayout := func(g *gocui.Gui) error {
		return layout(g, ds)
	}
	g.SetManagerFunc(relayout)

	d.PanicIfError(g.SetKeybinding("", gocui.KeyF1, gocui.ModNone, debugInfo))
	d.PanicIfError(g.SetKeybinding(all, gocui.KeyCtrlC, gocui.ModNone, quit))
	d.PanicIfError(g.SetKeybinding(all, gocui.KeyTab, gocui.ModNone, nextView))
	d.PanicIfError(g.SetKeybinding(messages, gocui.KeyArrowUp, gocui.ModNone, arrowUp))
	d.PanicIfError(g.SetKeybinding(messages, gocui.KeyArrowDown, gocui.ModNone, arrowDown))
	d.PanicIfError(g.SetKeybinding(input, gocui.KeyEnter, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) (err error) {
		defer func() {
			v.Clear()
			v.SetCursor(0, 0)
		}()
		buf := v.Buffer()
		buf = strings.TrimSpace(buf)
		msgView, err := g.View(messages)
		d.PanicIfError(err)
		msgView.Title = "messages"
        msgView.Autoscroll = true
		if buf == "" {
			updateMessages(g, msgView, ds, nil, nil)
			return nil
		}
		sIds, sTerms, nds, err := handleEnter(buf, username, time.Now(), ds)
		if err != nil {
			return
		}
		displayingSearchResults = sIds != nil && sIds.Len() != 0
		// Eep this is so ghetto. We need to restructure this code so there is a clear understanding of dataset lifetime.
		ds = nds
		updateMessages(g, msgView, ds, sIds, sTerms)
		if displayingSearchResults {
			return
		}
		Publish(sub, topic, ds.HeadRef().TargetHash().String())
		return
	}))
	d.PanicIfError(g.SetKeybinding("", gocui.KeyF1, gocui.ModNone, debugInfo))
	if err := g.MainLoop(); err != nil && err != gocui.ErrQuit {
		log.Panicln(err)
	}
}

func layout(g *gocui.Gui, ds datas.Dataset) error {
	maxX, maxY := g.Size()
	if v, err := g.SetView(users, 0, 0, 25, maxY-1); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Title = users
		v.Wrap = false
		v.Editable = false
		resetAuthors(g, ds)
	}
	if v, err := g.SetView(messages, 25, 0, maxX-1, maxY-2-1); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Title = messages
		v.Editable = false
		v.Wrap = true
		v.Autoscroll = true
		updateMessages(g, v, ds, nil, nil)
		return nil
	}
	if v, err := g.SetView(input, 25, maxY-2-1, maxX-1, maxY-1); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Wrap = true
		v.Editable = true
		v.Autoscroll = true
	}
	if firstLayout {
		firstLayout = false
		g.SetCurrentView(input)
		dbg.Debug("started up")
	}
	return nil
}

type dataPager struct {
	dataset    datas.Dataset
	msgKeyChan chan types.String
	doneChan   chan struct{}
	msgMap     types.Map
	terms      []string
}

var dp dataPager

func (dp *dataPager) Close() {
	dp.doneChan <- struct{}{}
}

func (dp *dataPager) IsEmpty() bool {
	return dp.msgKeyChan == nil && dp.doneChan == nil
}

func (dp *dataPager) Next() (string, bool) {
	msgKey := <-dp.msgKeyChan
	if msgKey == "" {
		return "", false
	}
	nm := dp.msgMap.Get(msgKey)

	var m Message
	err := marshal.Unmarshal(nm, &m)
	if err != nil {
		return fmt.Sprintf("ERROR: %s", err.Error()), true
	}

	s1 := fmt.Sprintf("%s: %s", m.Author, m.Body)
	s2 := highlightTerms(s1, dp.terms)
	return s2, true
}

func updateMessages(g *gocui.Gui, v *gocui.View, ds datas.Dataset, filterIds *types.Map, terms []string) error {
	resetAuthors(g, ds)
	v.Clear()
	v.SetOrigin(0, 0)
	_, winHeight := v.Size()

	if !dp.IsEmpty() {
		dp.Close()
	}

	doneChan := make(chan struct{})
	msgMap, msgKeyChan, err := ListMessages(ds, filterIds, doneChan)
	d.PanicIfError(err)
	dp = dataPager{
		dataset:    ds,
		msgKeyChan: msgKeyChan,
		doneChan:   doneChan,
		msgMap:     msgMap,
		terms:      terms,
	}

	items := []string{}
	for len(items) < winHeight {
		m, ok := dp.Next()
		if !ok {
			break
		}
		items = append([]string{m}, items...)
	}

	for _, s := range items {
		fmt.Fprintf(v, "%s\n", s)
	}
	return nil
}

func resetAuthors(g *gocui.Gui, ds datas.Dataset) {
	v, err := g.View(users)
	d.PanicIfError(err)
	v.Clear()
	for _, u := range GetAuthors(ds) {
		fmt.Fprintln(v, u)
	}
}

func updateMessagesAndRefresh(g *gocui.Gui, ds datas.Dataset, sids *types.Map, terms []string) {
	g.Execute(func(_ *gocui.Gui) error {
		v, err := g.View(messages)
		d.PanicIfError(err)
		err = updateMessages(g, v, ds, sids, terms)
		d.PanicIfError(err)
		return nil
	})
}

func prependMessages(v *gocui.View) int {
	buf := viewBuffer(v)
	if len(buf) == 0 {
		return 0
	}

	msg, ok := dp.Next()
	if !ok {
		return 0
	}

	v.Clear()
	fmt.Fprintf(v, "%s\n", msg)
	fmt.Fprintf(v, "%s", highlightTerms(buf, dp.terms))
	return countLines(msg) + countLines(buf)
}

func scrollView(v *gocui.View, dy, lineCnt int) {
	// Get the size and position of the view.
	_, height := v.Size()
	ox1, oy1 := v.Origin()
	cx1, cy1 := v.Cursor()

	// maxCy will either be the height of the screen - 1, or in the case that
	// the there aren't enough lines to fill the screen, it will be the
	// lineCnt - origin
	newCy := cy1 + dy
	maxCy := math.MinInt(lineCnt-oy1, height-1)

	// If the newCy doesn't require scrolling, then just move the cursor.
	if newCy >= 0 && newCy < maxCy {
		v.MoveCursor(cx1, dy, false)
		return
	}

	// If the cursor is already at the bottom of the screen and there are no
	// lines left to scroll up, then we're at the bottom.
	if newCy >= maxCy && oy1 >= lineCnt-height {
		// Set autoscroll to normal again.
		v.Autoscroll = true
	} else {
		// The cursor is already at the bottom or top of the screen so scroll
		// the text
		v.Autoscroll = false
		v.SetOrigin(ox1, oy1+dy)
	}
}

func handleEnter(body string, author string, clientTime time.Time, ds datas.Dataset) (*types.Map, []string, datas.Dataset, error) {
	if strings.HasPrefix(body, searchPrefix) {
		st := TermsFromString(body[len(searchPrefix):])
		ids := SearchIndex(ds, st)
		return &ids, st, ds, nil
	}

	if strings.HasPrefix(body, quitPrefix) {
		return nil, nil, ds, gocui.ErrQuit
	}

	ds, err := AddMessage(body, author, clientTime, ds)
	return nil, nil, ds, err
}

func quit(_ *gocui.Gui, _ *gocui.View) error {
	return gocui.ErrQuit
}

func arrowUp(_ *gocui.Gui, v *gocui.View) error {
	lineCnt := countLines(viewBuffer(v))
	if _, oy := v.Origin(); oy == 0 {
		lineCnt = prependMessages(v)
	}
	scrollView(v, -1, lineCnt)
	return nil
}

func arrowDown(_ *gocui.Gui, v *gocui.View) error {
	lineCnt := countLines(viewBuffer(v))
	scrollView(v, 1, lineCnt)
	return nil
}

func debugInfo(g *gocui.Gui, _ *gocui.View) error {
	msgView, _ := g.View(messages)
	w, h := msgView.Size()
	dbg.Debug("info, window size:(%d, %d), lineCnt: %d", w, h, countLines(viewBuffer(msgView)))
	cx, cy := msgView.Cursor()
	ox, oy := msgView.Origin()
	dbg.Debug("info, origin: (%d,%d), cursor: (%d,%d)", ox, oy, cx, cy)
	// dbg.Debug("info, view buffer:\n%s", highlightTerms(viewBuffer(msgView), dp.terms))
    return nil
}

func viewBuffer(v *gocui.View) string {
	buf := strings.TrimSpace(v.ViewBuffer())
	if len(buf) > 0 && buf[len(buf)-1] != byte('\n') {
		buf = buf + "\n"
	}
	return buf
}

func countLines(s string) int {
	return strings.Count(s, "\n")
}

func nextView(g *gocui.Gui, v *gocui.View) (err error) {
	nextName := nextViewName(v.Name())
	if _, err = g.SetCurrentView(nextName); err != nil {
		return
	}
	_, err = g.SetViewOnTop(nextName)
	return
}

func nextViewName(currentView string) string {
	for i, viewname := range viewNames {
		if currentView == viewname {
			return viewNames[(i+1)%len(viewNames)]
		}
	}
	return viewNames[0]
}

func textScrolledToEnd(g *gocui.Gui) bool {
	v, err := g.View(messages)
	if err != nil {
		// doubt this will ever happen, if it does just assume we're at bottom
		return true
	}
	_, oy := v.Origin()
	_, h := v.Size()
	lc := countLines(viewBuffer(v))
	dbg.Debug("textScrolledToEnd, oy: %d, h: %d, lc: %d, lc-oy: %d, res: %t", oy, h, lc, lc-oy, lc-oy <= h)
	return lc-oy <= h
}

func updateViewTitle(g *gocui.Gui, viewname, title string) (err error) {
	v, err := g.View(viewname)
	if err != nil {
		return
	}
	v.Title = title
	return
}

var bgColors, fgColors = GenColors()

func GenColors() ([]string, []string) {
	bg, fg := []string{}, []string{}
	for i := 1; i <= 9; i++ {
		// skip dark blue & white
		if i != 4 && i != 7 {
			bg = append(bg, fmt.Sprintf("\x1b[48;5;%dm\x1b[30m%%s\x1b[0m", i))
			fg = append(fg, fmt.Sprintf("\x1b[38;5;%dm%%s\x1b[0m", i))
		}
	}
	return bg, fg
}

func colorTerm(color int, s string, background bool) string {
	c := fgColors[color]
	if background {
		c = bgColors[color]
	}
	return fmt.Sprintf(c, s)
}

func highlightTerms(s string, terms []string) string {
	for i, t := range terms {
		color := i % len(fgColors)
		re := regexp.MustCompile(fmt.Sprintf("(?i)%s", regexp.QuoteMeta(t)))
		s = re.ReplaceAllStringFunc(s, func(s string) string {
			return colorTerm(color, s, false)
		})
	}
	return s
}

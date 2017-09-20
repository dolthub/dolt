// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package lib

import (
	"fmt"
	"regexp"
	"runtime"
	"strings"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/math"
	"github.com/attic-labs/noms/samples/go/ipfs-chat/dbg"
	"github.com/jroimartin/gocui"
)

const (
	allViews    = ""
	usersView   = "users"
	messageView = "messages"
	inputView   = "input"

	searchPrefix = "/s"
	quitPrefix   = "/q"
)

type TermUI struct {
	Gui      *gocui.Gui
	InSearch bool
}

var (
	viewNames   = []string{usersView, messageView, inputView}
	firstLayout = true
)

func CreateTermUI(events chan ChatEvent) TermUI {
	g, err := gocui.NewGui(gocui.Output256)
	d.PanicIfError(err)

	g.Highlight = true
	g.SelFgColor = gocui.ColorGreen
	g.Cursor = true

	relayout := func(g *gocui.Gui) error {
		return layout(g)
	}
	g.SetManagerFunc(relayout)

	d.PanicIfError(g.SetKeybinding(allViews, gocui.KeyF1, gocui.ModNone, debugInfo))
	d.PanicIfError(g.SetKeybinding(allViews, gocui.KeyCtrlC, gocui.ModNone, quit))
	d.PanicIfError(g.SetKeybinding(allViews, gocui.KeyCtrlC, gocui.ModAlt, quitWithStack))
	d.PanicIfError(g.SetKeybinding(allViews, gocui.KeyTab, gocui.ModNone, nextView))
	d.PanicIfError(g.SetKeybinding(messageView, gocui.KeyArrowUp, gocui.ModNone, arrowUp))
	d.PanicIfError(g.SetKeybinding(messageView, gocui.KeyArrowDown, gocui.ModNone, arrowDown))
	d.PanicIfError(g.SetKeybinding(inputView, gocui.KeyEnter, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) (err error) {
		defer func() {
			v.Clear()
			v.SetCursor(0, 0)
			msgView, err := g.View(messageView)
			d.PanicIfError(err)
			msgView.Title = "messages"
			msgView.Autoscroll = true
		}()
		buf := strings.TrimSpace(v.Buffer())
		if strings.HasPrefix(buf, searchPrefix) {
			events <- ChatEvent{EventType: SearchEvent, Event: strings.TrimSpace(buf[len(searchPrefix):])}
			return
		}
		if strings.HasPrefix(buf, quitPrefix) {
			err = gocui.ErrQuit
			return
		}
		events <- ChatEvent{EventType: InputEvent, Event: buf}
		return
	}))

	return TermUI{Gui: g, InSearch: false}
}

func (t TermUI) Close() {
	dbg.Debug("Closing gui")
	t.Gui.Close()
}

func (t TermUI) UpdateMessagesFromSync(ds datas.Dataset) {
	if t.InSearch || !textScrolledToEnd(t.Gui) {
		t.Gui.Execute(func(g *gocui.Gui) (err error) {
			updateViewTitle(g, messageView, "messages (NEW!)")
			return
		})
	} else {
		t.UpdateMessagesAsync(ds, nil, nil)
	}
}

func (t TermUI) Layout() error {
	return layout(t.Gui)
}

func layout(g *gocui.Gui) error {
	maxX, maxY := g.Size()
	if v, err := g.SetView(usersView, 0, 0, 25, maxY-1); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Title = usersView
		v.Wrap = false
		v.Editable = false
	}
	if v, err := g.SetView(messageView, 25, 0, maxX-1, maxY-2-1); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Title = messageView
		v.Editable = false
		v.Wrap = true
		v.Autoscroll = true
		return nil
	}
	if v, err := g.SetView(inputView, 25, maxY-2-1, maxX-1, maxY-1); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Wrap = true
		v.Editable = true
		v.Autoscroll = true
	}
	if firstLayout {
		firstLayout = false
		g.SetCurrentView(inputView)
		dbg.Debug("started up")
	}
	return nil
}

func (t TermUI) UpdateMessages(ds datas.Dataset, filterIds *types.Map, terms []string) error {
	defer dbg.BoxF("updateMessages")()

	t.ResetAuthors(ds)
	v, err := t.Gui.View(messageView)
	d.PanicIfError(err)
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

func (t TermUI) ResetAuthors(ds datas.Dataset) {
	v, err := t.Gui.View(usersView)
	d.PanicIfError(err)
	v.Clear()
	for _, u := range GetAuthors(ds) {
		fmt.Fprintln(v, u)
	}
}

func (t TermUI) UpdateMessagesAsync(ds datas.Dataset, sids *types.Map, terms []string) {
	t.Gui.Execute(func(_ *gocui.Gui) error {
		err := t.UpdateMessages(ds, sids, terms)
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

func quit(_ *gocui.Gui, _ *gocui.View) error {
	dbg.Debug("QUITTING #####")
	return gocui.ErrQuit
}

func quitWithStack(_ *gocui.Gui, _ *gocui.View) error {
	dbg.Debug("QUITTING WITH STACK")
	stacktrace := make([]byte, 1024*1024)
	length := runtime.Stack(stacktrace, true)
	dbg.Debug(string(stacktrace[:length]))
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
	msgView, _ := g.View(messageView)
	w, h := msgView.Size()
	dbg.Debug("info, window size:(%d, %d), lineCnt: %d", w, h, countLines(viewBuffer(msgView)))
	cx, cy := msgView.Cursor()
	ox, oy := msgView.Origin()
	dbg.Debug("info, origin: (%d,%d), cursor: (%d,%d)", ox, oy, cx, cy)
	dbg.Debug("info, view buffer:\n%s", highlightTerms(viewBuffer(msgView), dp.terms))
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
	v, err := g.View(messageView)
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

var bgColors, fgColors = genColors()

func genColors() ([]string, []string) {
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

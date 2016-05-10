package main

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/mattn/go-colorable"
	"github.com/mgutz/ansi"
)

func main() {
	printColors()
	print256Colors()
	printConstants()
}

func pad(s string, length int) string {
	for len(s) < length {
		s += " "
	}
	return s
}

func padColor(s string, styles []string) string {
	buffer := ""
	for _, style := range styles {
		buffer += ansi.Color(pad(s+style, 20), s+style)
	}
	return buffer
}

func printPlain() {
	ansi.DisableColors(true)
	bgColors := []string{
		"",
		":black",
		":red",
		":green",
		":yellow",
		":blue",
		":magenta",
		":cyan",
		":white",
	}
	for fg := range ansi.Colors {
		for _, bg := range bgColors {
			println(padColor(fg, []string{"" + bg, "+b" + bg, "+bh" + bg, "+u" + bg}))
			println(padColor(fg, []string{"+uh" + bg, "+B" + bg, "+Bb" + bg /* backgrounds */, "" + bg + "+h"}))
			println(padColor(fg, []string{"+b" + bg + "+h", "+bh" + bg + "+h", "+u" + bg + "+h", "+uh" + bg + "+h"}))
		}
	}
}

func printColors() {
	ansi.DisableColors(false)
	stdout := colorable.NewColorableStdout()

	bgColors := []string{
		"",
		":black",
		":red",
		":green",
		":yellow",
		":blue",
		":magenta",
		":cyan",
		":white",
	}

	keys := []string{}
	for fg := range ansi.Colors {
		_, err := strconv.Atoi(fg)
		if err != nil {
			keys = append(keys, fg)
		}
	}
	sort.Strings(keys)

	for _, fg := range keys {
		for _, bg := range bgColors {
			fmt.Fprintln(stdout, padColor(fg, []string{"" + bg, "+b" + bg, "+bh" + bg, "+u" + bg}))
			fmt.Fprintln(stdout, padColor(fg, []string{"+uh" + bg, "+B" + bg, "+Bb" + bg /* backgrounds */, "" + bg + "+h"}))
			fmt.Fprintln(stdout, padColor(fg, []string{"+b" + bg + "+h", "+bh" + bg + "+h", "+u" + bg + "+h", "+uh" + bg + "+h"}))
		}
	}
}

func print256Colors() {
	ansi.DisableColors(false)
	stdout := colorable.NewColorableStdout()

	bgColors := []string{""}
	for i := 0; i < 256; i++ {
		key := fmt.Sprintf(":%d", i)
		bgColors = append(bgColors, key)
	}

	keys := []string{}
	for fg := range ansi.Colors {
		n, err := strconv.Atoi(fg)
		if err == nil {
			keys = append(keys, fmt.Sprintf("%3d", n))
		}
	}
	sort.Strings(keys)

	for _, fg := range keys {
		for _, bg := range bgColors {
			fmt.Fprintln(stdout, padColor(fg, []string{"" + bg, "+b" + bg, "+u" + bg}))
			fmt.Fprintln(stdout, padColor(fg, []string{"+B" + bg, "+Bb" + bg}))
		}
	}
}

func printConstants() {
	stdout := colorable.NewColorableStdout()
	fmt.Fprintln(stdout, ansi.DefaultFG, "ansi.DefaultFG", ansi.Reset)
	fmt.Fprintln(stdout, ansi.Black, "ansi.Black", ansi.Reset)
	fmt.Fprintln(stdout, ansi.Red, "ansi.Red", ansi.Reset)
	fmt.Fprintln(stdout, ansi.Green, "ansi.Green", ansi.Reset)
	fmt.Fprintln(stdout, ansi.Yellow, "ansi.Yellow", ansi.Reset)
	fmt.Fprintln(stdout, ansi.Blue, "ansi.Blue", ansi.Reset)
	fmt.Fprintln(stdout, ansi.Magenta, "ansi.Magenta", ansi.Reset)
	fmt.Fprintln(stdout, ansi.Cyan, "ansi.Cyan", ansi.Reset)
	fmt.Fprintln(stdout, ansi.White, "ansi.White", ansi.Reset)
	fmt.Fprintln(stdout, ansi.LightBlack, "ansi.LightBlack", ansi.Reset)
	fmt.Fprintln(stdout, ansi.LightRed, "ansi.LightRed", ansi.Reset)
	fmt.Fprintln(stdout, ansi.LightGreen, "ansi.LightGreen", ansi.Reset)
	fmt.Fprintln(stdout, ansi.LightYellow, "ansi.LightYellow", ansi.Reset)
	fmt.Fprintln(stdout, ansi.LightBlue, "ansi.LightBlue", ansi.Reset)
	fmt.Fprintln(stdout, ansi.LightMagenta, "ansi.LightMagenta", ansi.Reset)
	fmt.Fprintln(stdout, ansi.LightCyan, "ansi.LightCyan", ansi.Reset)
	fmt.Fprintln(stdout, ansi.LightWhite, "ansi.LightWhite", ansi.Reset)
}

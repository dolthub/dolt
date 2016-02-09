package xlsx

import (
	"fmt"
	"strconv"
)

type theme struct {
	colors []string
}

func newTheme(themeXml xlsxTheme) *theme {
	clrMap := map[string]string{}
	clrSchemes := themeXml.ThemeElements.ClrScheme.Children
	for _, scheme := range clrSchemes {
		var rgbColor string
		if scheme.SysClr != nil {
			rgbColor = scheme.SysClr.LastClr
		} else {
			rgbColor = scheme.SrgbClr.Val
		}
		clrMap[scheme.XMLName.Local] = rgbColor
	}
	colors := []string{clrMap["lt1"], clrMap["dk1"], clrMap["lt2"], clrMap["dk2"], clrMap["accent1"],
		clrMap["accent2"], clrMap["accent3"], clrMap["accent4"], clrMap["accent5"],
		clrMap["accent6"], clrMap["hlink"], clrMap["folHlink"]}
	return &theme{colors}
}

func (t *theme) themeColor(index int64, tint float64) string {
	baseColor := t.colors[index]
	if tint == 0 {
		return "FF" + baseColor
	} else {
		r, _ := strconv.ParseInt(baseColor[0:2], 16, 64)
		g, _ := strconv.ParseInt(baseColor[2:4], 16, 64)
		b, _ := strconv.ParseInt(baseColor[4:6], 16, 64)
		h, s, l := RGBToHSL(uint8(r), uint8(g), uint8(b))
		if tint < 0 {
			l *= (1 + tint)
		} else {
			l = l*(1-tint) + (1 - (1 - tint))
		}
		br, bg, bb := HSLToRGB(h, s, l)
		return fmt.Sprintf("FF%02X%02X%02X", br, bg, bb)
	}
}

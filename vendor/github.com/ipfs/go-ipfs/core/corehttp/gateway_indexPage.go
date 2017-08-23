package corehttp

import (
	"html/template"
	"net/url"
	"path"
	"strings"

	"github.com/ipfs/go-ipfs/assets"
)

// structs for directory listing
type listingTemplateData struct {
	Listing  []directoryItem
	Path     string
	BackLink string
}

type directoryItem struct {
	Size string
	Name string
	Path string
}

var listingTemplate *template.Template

func init() {
	knownIconsBytes, err := assets.Asset("dir-index-html/knownIcons.txt")
	if err != nil {
		panic(err)
	}
	knownIcons := make(map[string]struct{})
	for _, ext := range strings.Split(strings.TrimSuffix(string(knownIconsBytes), "\n"), "\n") {
		knownIcons[ext] = struct{}{}
	}

	// helper to guess the type/icon for it by the extension name
	iconFromExt := func(name string) string {
		ext := path.Ext(name)
		_, ok := knownIcons[ext]
		if !ok {
			// default blank icon
			return "ipfs-_blank"
		}
		return "ipfs-" + ext[1:] // slice of the first dot
	}

	// custom template-escaping function to escape a full path, including '#' and '?'
	urlEscape := func(rawUrl string) string {
		pathUrl := url.URL{Path: rawUrl}
		return pathUrl.String()
	}

	// Directory listing template
	dirIndexBytes, err := assets.Asset("dir-index-html/dir-index.html")
	if err != nil {
		panic(err)
	}

	listingTemplate = template.Must(template.New("dir").Funcs(template.FuncMap{
		"iconFromExt": iconFromExt,
		"urlEscape":   urlEscape,
	}).Parse(string(dirIndexBytes)))
}

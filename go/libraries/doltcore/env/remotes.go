package env

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/config"
	"net/url"
	"strings"
)

const (
	remotePrefix   = "remote."
	RemoteUrlParam = "url"
)

type Remote struct {
	Name string
	Url  string
}

func NewRemote(name string, params map[string]string) (*Remote, error) {
	urlStr := params[RemoteUrlParam]
	return &Remote{name, urlStr}, nil
}

func (r *Remote) GetRemoteDB() *doltdb.DoltDB {
	remoteLocStr := DoltNomsProtocolID + ":" + r.Url
	return doltdb.LoadDoltDB(doltdb.Location(remoteLocStr))
}

func parseRemotesFromConfig(cfg config.ReadableConfig) (map[string]*Remote, error) {
	lenRemotePrefix := len(remotePrefix)

	nameToRemoteParams := make(map[string]map[string]string)
	cfg.Iter(func(key string, value string) (stop bool) {
		if strings.HasPrefix(key, remotePrefix) {
			noPrefix := key[lenRemotePrefix:]
			idx := strings.Index(noPrefix, ".")

			if idx > 0 {
				remoteName := noPrefix[:idx]
				remoteKey := noPrefix[idx+1:]

				if _, ok := nameToRemoteParams[remoteName]; !ok {
					nameToRemoteParams[remoteName] = make(map[string]string)
				}

				nameToRemoteParams[remoteName][remoteKey] = value
			}
		}

		return false
	})

	var err error
	remotes := make(map[string]*Remote)
	for k, v := range nameToRemoteParams {
		remotes[k], err = NewRemote(k, v)

		if err != nil {
			return nil, err
		}
	}

	return remotes, nil
}

func RemoteConfigParam(remoteName, paramName string) string {
	return remotePrefix + remoteName + "." + paramName
}

func ParseRemoteUrl(rawUrl string) (*url.URL, error) {
	isNaked := false
	if !hasSchemaRegEx.MatchString(rawUrl) {
		isNaked = true
		rawUrl = "hack://" + rawUrl
	}

	remoteUrl, err := url.Parse(rawUrl)

	if err != nil {
		return nil, err
	}

	if isNaked {
		remoteUrl.Scheme = ""
	}

	return remoteUrl, nil
}

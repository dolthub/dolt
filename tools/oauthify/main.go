// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"log"
	"os"

	flag "github.com/juju/gnuflag"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
)

func main() {
	clientID := flag.String("client-id", "", "the client id of the calling application")
	clientSecret := flag.String("client-secret", "", "the client secret of the calling application")
	authURL := flag.String("auth-url", "", "the authorization website")
	tokenURL := flag.String("token-url", "", "the token exchange endpoint")
	isBroken := flag.Bool("pass-client-in-url", false,
		"some oauth providers need client credentials passed in querystring - see "+
			"https://godoc.org/golang.org/x/oauth2#RegisterBrokenAuthHeaderProvider")

	flag.Usage = func() {
		fmt.Printf(
			`oauthify is a helper program to authenticate with oauth services.

Run oauthify once to get an access token, then use that token in other
services.

Currently, oauthify is only known to work with Dropbox and is missing
many features, such as:

- support for refresh tokens
- support for oauth1
- support for the redirect oauth2 flow
- support for scopes

`)
		flag.PrintDefaults()
	}

	flag.Parse(false)
	if *clientID == "" || *clientSecret == "" || *authURL == "" || *tokenURL == "" {
		fmt.Fprintln(os.Stderr, "Required parameter not specified\n")
		flag.PrintDefaults()
		return
	}

	if *isBroken {
		oauth2.RegisterBrokenAuthHeaderProvider(*tokenURL)
	}

	ctx := context.Background()
	conf := &oauth2.Config{
		ClientID:     *clientID,
		ClientSecret: *clientSecret,
		Scopes:       nil,
		Endpoint: oauth2.Endpoint{
			AuthURL:  *authURL,
			TokenURL: *tokenURL,
		},
	}

	// TODO: If necessary for other providers, can add "offline" flag like so:
	//url := conf.AuthCodeURL("state", oauth2.AccessTypeOffline)

	url := conf.AuthCodeURL("state")
	fmt.Println("Visit the following URL, then paste the resulting code below:\n")
	fmt.Printf("%s\n\n", url)

	var code string
	if _, err := fmt.Scan(&code); err != nil {
		log.Fatal(err)
	}
	tok, err := conf.Exchange(ctx, code)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\nAccess token: %s\n", tok.AccessToken)
	if tok.RefreshToken != "" {
		fmt.Printf("Refresh token: %s\n", tok.RefreshToken)
	}
}

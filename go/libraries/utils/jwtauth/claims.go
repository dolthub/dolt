package jwtauth

import (
	"gopkg.in/square/go-jose.v2/jwt"
)

type Claims struct {
	jwt.Claims
	OnBehalfOf string `json:"on_behalf_of"`
}

package config

type Reprovider struct {
	Interval string // Time period to reprovide locally stored objects to the network
	Strategy string // Which keys to announce
}

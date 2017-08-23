package config

// Mounts stores the (string) mount points
type Mounts struct {
	IPFS           string
	IPNS           string
	FuseAllowOther bool
}

package config

// Addresses stores the (string) multiaddr addresses for the node.
type Addresses struct {
	Swarm   []string // addresses for the swarm network
	API     string   // address for the local API (RPC)
	Gateway string   // address to listen on for IPFS HTTP object gateway
}

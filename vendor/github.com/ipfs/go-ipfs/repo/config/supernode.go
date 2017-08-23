package config

import "github.com/ipfs/go-ipfs/thirdparty/ipfsaddr"

// TODO replace with final servers before merge

// TODO rename
type SupernodeClientConfig struct {
	Servers []string
}

var DefaultSNRServers = []string{
	"/ip4/104.236.176.52/tcp/4002/ipfs/QmXdb7tWTxdFEQEFgWBqkuYSrZd3mXrC7HxkD4krGNYx2U",
	"/ip4/104.236.179.241/tcp/4002/ipfs/QmVRqViDByUxjUMoPnjurjKvZhaEMFDtK35FJXHAM4Lkj6",
	"/ip4/104.236.151.122/tcp/4002/ipfs/QmSZwGx8Tn8tmcM4PtDJaMeUQNRhNFdBLVGPzRiNaRJtFH",
	"/ip4/162.243.248.213/tcp/4002/ipfs/QmbHVEEepCi7rn7VL7Exxpd2Ci9NNB6ifvqwhsrbRMgQFP",
	"/ip4/128.199.219.111/tcp/4002/ipfs/Qmb3brdCYmKG1ycwqCbo6LUwWxTuo3FisnJV2yir7oN92R",
	"/ip4/104.236.76.40/tcp/4002/ipfs/QmdRBCV8Cz2dGhoKLkD3YjPwVFECmqADQkx5ZteF2c6Fy4",
	"/ip4/178.62.158.247/tcp/4002/ipfs/QmUdiMPci7YoEUBkyFZAh2pAbjqcPr7LezyiPD2artLw3v",
	"/ip4/178.62.61.185/tcp/4002/ipfs/QmVw6fGNqBixZE4bewRLT2VXX7fAHUHs8JyidDiJ1P7RUN",
}

func (gcr *SupernodeClientConfig) ServerIPFSAddrs() ([]ipfsaddr.IPFSAddr, error) {
	var addrs []ipfsaddr.IPFSAddr
	for _, server := range gcr.Servers {
		addr, err := ipfsaddr.ParseString(server)
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

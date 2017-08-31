package iptbutil

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	serial "github.com/ipfs/go-ipfs/repo/fsrepo/serialize"
	"gx/ipfs/QmZLUtHGe9HDQrreAYkXCzzK6mHVByV4MRd8heXAtV5wyS/stump"
)

// GetNumNodes returns the number of testbed nodes configured in the testbed directory
func GetNumNodes() int {
	for i := 0; i < 2000; i++ {
		dir, err := IpfsDirN(i)
		if err != nil {
			return i
		}
		_, err = os.Stat(dir)
		if os.IsNotExist(err) {
			return i
		}
	}
	panic("i dont know whats going on")
}

func TestBedDir() (string, error) {
	tbd := os.Getenv("IPTB_ROOT")
	if len(tbd) != 0 {
		return tbd, nil
	}

	home := os.Getenv("HOME")
	if len(home) == 0 {
		return "", fmt.Errorf("environment variable HOME is not set")
	}

	return path.Join(home, "testbed"), nil
}

func IpfsDirN(n int) (string, error) {
	tbd, err := TestBedDir()
	if err != nil {
		return "", err
	}
	return path.Join(tbd, fmt.Sprint(n)), nil
}

type InitCfg struct {
	Count     int
	Force     bool
	Bootstrap string
	PortStart int
	Mdns      bool
	Utp       bool
	Websocket bool
	Override  string
	NodeType  string
}

func (c *InitCfg) swarmAddrForPeer(i int) string {
	str := "/ip4/0.0.0.0/tcp/%d"
	if c.Utp {
		str = "/ip4/0.0.0.0/udp/%d/utp"
	}
	if c.Websocket {
		str = "/ip4/0.0.0.0/tcp/%d/ws"
	}

	if c.PortStart == 0 {
		return fmt.Sprintf(str, 0)
	}
	return fmt.Sprintf(str, c.PortStart+i)
}

func (c *InitCfg) apiAddrForPeer(i int) string {
	ip := "127.0.0.1"
	if c.NodeType == "docker" {
		ip = "0.0.0.0"
	}

	var port int
	if c.PortStart != 0 {
		port = c.PortStart + 1000 + i
	}

	return fmt.Sprintf("/ip4/%s/tcp/%d", ip, port)
}

func YesNoPrompt(prompt string) bool {
	var s string
	for {
		fmt.Println(prompt)
		fmt.Scanf("%s", &s)
		switch s {
		case "y", "Y":
			return true
		case "n", "N":
			return false
		}
		fmt.Println("Please press either 'y' or 'n'")
	}
}

func LoadNodeN(n int) (IpfsNode, error) {
	specs, err := ReadNodeSpecs()
	if err != nil {
		return nil, err
	}

	return specs[n].Load()
}

func LoadNodes() ([]IpfsNode, error) {
	specs, err := ReadNodeSpecs()
	if err != nil {
		return nil, err
	}

	return NodesFromSpecs(specs)
}

func NodesFromSpecs(specs []*NodeSpec) ([]IpfsNode, error) {
	var out []IpfsNode
	for _, s := range specs {
		nd, err := s.Load()
		if err != nil {
			return nil, err
		}
		out = append(out, nd)
	}
	return out, nil
}

type NodeSpec struct {
	Type  string
	Dir   string
	Extra map[string]interface{}
}

func ReadNodeSpecs() ([]*NodeSpec, error) {
	tbd, err := TestBedDir()
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadFile(filepath.Join(tbd, "nodespec"))
	if err != nil {
		return nil, err
	}

	var specs []*NodeSpec
	err = json.Unmarshal(data, &specs)
	if err != nil {
		return nil, err
	}

	return specs, nil
}

func WriteNodeSpecs(specs []*NodeSpec) error {
	tbd, err := TestBedDir()
	if err != nil {
		return err
	}

	err = os.MkdirAll(tbd, 0775)
	if err != nil {
		return err
	}

	fi, err := os.Create(filepath.Join(tbd, "nodespec"))
	if err != nil {
		return err
	}

	defer fi.Close()
	err = json.NewEncoder(fi).Encode(specs)
	if err != nil {
		return err
	}

	return nil
}

func (ns *NodeSpec) Load() (IpfsNode, error) {
	switch ns.Type {
	case "local":
		ln := &LocalNode{
			Dir: ns.Dir,
		}

		if _, err := os.Stat(filepath.Join(ln.Dir, "config")); err == nil {
			pid, err := GetPeerID(ln.Dir)
			if err != nil {
				return nil, err
			}

			ln.PeerID = pid
		}

		return ln, nil
	case "docker":
		imgi, ok := ns.Extra["image"]
		if !ok {
			return nil, fmt.Errorf("no 'image' field on docker node spec")
		}

		img := imgi.(string)

		dn := &DockerNode{
			ImageName: img,
			LocalNode: LocalNode{
				Dir: ns.Dir,
			},
		}

		if _, err := os.Stat(filepath.Join(dn.Dir, "config")); err == nil {
			pid, err := GetPeerID(dn.Dir)
			if err != nil {
				return nil, err
			}

			dn.PeerID = pid
		}

		didfi := filepath.Join(ns.Dir, "dockerID")
		if _, err := os.Stat(didfi); err == nil {
			data, err := ioutil.ReadFile(didfi)
			if err != nil {
				return nil, err
			}

			dn.ID = string(data)
		}

		return dn, nil
	default:
		return nil, fmt.Errorf("unrecognized iptb node type")
	}
}

func initSpecs(cfg *InitCfg) ([]*NodeSpec, error) {
	var specs []*NodeSpec
	// generate node spec

	for i := 0; i < cfg.Count; i++ {
		dir, err := IpfsDirN(i)
		if err != nil {
			return nil, err
		}
		var ns *NodeSpec

		switch cfg.NodeType {
		case "docker":
			img := "go-ipfs"
			if altimg := os.Getenv("IPFS_DOCKER_IMAGE"); altimg != "" {
				img = altimg
			}
			ns = &NodeSpec{
				Type: "docker",
				Dir:  dir,
				Extra: map[string]interface{}{
					"image": img,
				},
			}
		default:
			ns = &NodeSpec{
				Type: "local",
				Dir:  dir,
			}
		}
		specs = append(specs, ns)
	}

	return specs, nil
}

func IpfsInit(cfg *InitCfg) error {
	tbd, err := TestBedDir()
	if err != nil {
		return err
	}

	if _, err := os.Stat(filepath.Join(tbd, "nodespec")); !os.IsNotExist(err) {
		if !cfg.Force && !YesNoPrompt("testbed nodes already exist, overwrite? [y/n]") {
			return nil
		}
		tbd, err := TestBedDir()
		err = os.RemoveAll(tbd)
		if err != nil {
			return err
		}
	}

	specs, err := initSpecs(cfg)
	if err != nil {
		return err
	}

	nodes, err := NodesFromSpecs(specs)
	if err != nil {
		return err
	}

	err = WriteNodeSpecs(specs)
	if err != nil {
		return err
	}

	wait := sync.WaitGroup{}
	for _, n := range nodes {
		wait.Add(1)
		go func(nd IpfsNode) {
			defer wait.Done()
			err := nd.Init()
			if err != nil {
				stump.Error(err)
				return
			}
		}(n)
	}
	wait.Wait()

	// Now setup bootstrapping
	switch cfg.Bootstrap {
	case "star":
		err := starBootstrap(nodes, cfg)
		if err != nil {
			return err
		}
	case "none":
		err := clearBootstrapping(nodes, cfg)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized bootstrapping option: %s", cfg.Bootstrap)
	}

	/*
		if cfg.Override != "" {
			err := ApplyConfigOverride(cfg)
			if err != nil {
				return err
			}
		}
	*/

	return nil
}

func ApplyConfigOverride(cfg *InitCfg) error {
	fir, err := os.Open(cfg.Override)
	if err != nil {
		return err
	}
	defer fir.Close()

	var configs map[string]interface{}
	err = json.NewDecoder(fir).Decode(&configs)
	if err != nil {
		return err
	}

	for i := 0; i < cfg.Count; i++ {
		err := applyOverrideToNode(configs, i)
		if err != nil {
			return err
		}
	}

	return nil
}

func applyOverrideToNode(ovr map[string]interface{}, node int) error {
	for k, v := range ovr {
		_ = k
		switch v.(type) {
		case map[string]interface{}:
		default:
		}

	}

	panic("not implemented")
}

func starBootstrap(nodes []IpfsNode, icfg *InitCfg) error {
	// '0' node is the bootstrap node
	king := nodes[0]

	bcfg, err := king.GetConfig()
	if err != nil {
		return err
	}

	bcfg.Bootstrap = nil
	bcfg.Addresses.Swarm = []string{icfg.swarmAddrForPeer(0)}
	bcfg.Addresses.API = icfg.apiAddrForPeer(0)
	bcfg.Addresses.Gateway = ""
	bcfg.Discovery.MDNS.Enabled = icfg.Mdns

	err = king.WriteConfig(bcfg)
	if err != nil {
		return err
	}

	for i, nd := range nodes[1:] {
		cfg, err := nd.GetConfig()
		if err != nil {
			return err
		}

		ba := fmt.Sprintf("%s/ipfs/%s", bcfg.Addresses.Swarm[0], bcfg.Identity.PeerID)
		ba = strings.Replace(ba, "0.0.0.0", "127.0.0.1", -1)
		cfg.Bootstrap = []string{ba}
		cfg.Addresses.Gateway = ""
		cfg.Discovery.MDNS.Enabled = icfg.Mdns
		cfg.Addresses.Swarm = []string{
			icfg.swarmAddrForPeer(i + 1),
		}
		cfg.Addresses.API = icfg.apiAddrForPeer(i + 1)

		err = nd.WriteConfig(cfg)
		if err != nil {
			return err
		}
	}
	return nil
}

func clearBootstrapping(nodes []IpfsNode, icfg *InitCfg) error {
	for i, nd := range nodes {
		cfg, err := nd.GetConfig()
		if err != nil {
			return err
		}

		cfg.Bootstrap = nil
		cfg.Addresses.Gateway = ""
		cfg.Addresses.Swarm = []string{icfg.swarmAddrForPeer(i)}
		cfg.Addresses.API = icfg.apiAddrForPeer(i)
		cfg.Discovery.MDNS.Enabled = icfg.Mdns
		err = nd.WriteConfig(cfg)
		if err != nil {
			return err
		}
	}
	return nil
}

func IpfsKillAll(nds []IpfsNode) error {
	var errs []error
	for _, n := range nds {
		err := n.Kill()
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		var errstr string
		for _, e := range errs {
			errstr += "\n" + e.Error()
		}
		return fmt.Errorf(strings.TrimSpace(errstr))
	}
	return nil
}

func IpfsStart(nodes []IpfsNode, waitall bool, args []string) error {
	for _, n := range nodes {
		if err := n.Start(args); err != nil {
			return err
		}
	}
	if waitall {
		for _, n := range nodes {
			err := waitOnSwarmPeers(n)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

func waitOnAPI(n IpfsNode) error {
	for i := 0; i < 50; i++ {
		err := tryAPICheck(n)
		if err == nil {
			return nil
		}
		stump.VLog("temp error waiting on API: ", err)
		time.Sleep(time.Millisecond * 200)
	}
	return fmt.Errorf("node %s failed to come online in given time period", n.GetPeerID())
}

func tryAPICheck(n IpfsNode) error {
	addr, err := n.APIAddr()
	if err != nil {
		return err
	}

	stump.VLog("checking api addresss at: ", addr)
	resp, err := http.Get("http://" + addr + "/api/v0/id")
	if err != nil {
		return err
	}

	out := make(map[string]interface{})
	err = json.NewDecoder(resp.Body).Decode(&out)
	if err != nil {
		return fmt.Errorf("liveness check failed: %s", err)
	}

	id, ok := out["ID"]
	if !ok {
		return fmt.Errorf("liveness check failed: ID field not present in output")
	}

	idstr := id.(string)
	if idstr != n.GetPeerID() {
		return fmt.Errorf("liveness check failed: unexpected peer at endpoint")
	}

	return nil
}

func waitOnSwarmPeers(n IpfsNode) error {
	addr, err := n.APIAddr()
	if err != nil {
		return err
	}

	for i := 0; i < 50; i++ {
		resp, err := http.Get("http://" + addr + "/api/v0/swarm/peers")
		if err == nil {
			out := make(map[string]interface{})
			err := json.NewDecoder(resp.Body).Decode(&out)
			if err != nil {
				return fmt.Errorf("liveness check failed: %s", err)
			}

			pstrings, ok := out["Strings"]
			if ok {
				if len(pstrings.([]interface{})) == 0 {
					continue
				}
				return nil
			}

			peers, ok := out["Peers"]
			if !ok {
				return fmt.Errorf("object from swarm peers doesnt look right (api mismatch?)")
			}

			if peers == nil {
				time.Sleep(time.Millisecond * 200)
				continue
			}

			if plist, ok := peers.([]interface{}); ok && len(plist) == 0 {
				continue
			}

			return nil
		}
		time.Sleep(time.Millisecond * 200)
	}
	return fmt.Errorf("node at %s failed to bootstrap in given time period", addr)
}

// GetPeerID reads the config of node 'n' and returns its peer ID
func GetPeerID(ipfsdir string) (string, error) {
	cfg, err := serial.Load(path.Join(ipfsdir, "config"))
	if err != nil {
		return "", err
	}
	return cfg.Identity.PeerID, nil
}

func ConnectNodes(from, to IpfsNode) error {
	if from == to {
		// skip connecting to self..
		return nil
	}

	out, err := to.RunCmd("ipfs", "id", "-f", "<addrs>")
	if err != nil {
		return fmt.Errorf("error checking node address: %s", err)
	}

	stump.Log("connecting %s -> %s\n", from, to)

	addrs := strings.Fields(string(out))
	fmt.Println("Addresses: ", addrs)
	orderishAddresses(addrs)
	for i := 0; i < len(addrs); i++ {
		addr := addrs[i]
		stump.Log("trying ipfs swarm connect %s", addr)
		_, err = from.RunCmd("ipfs", "swarm", "connect", addr)
		if err == nil {
			stump.Log("connection success!")
			break
		}
		stump.Log("dial attempt to %s failed: %s", addr, err)
		time.Sleep(time.Second)
	}

	return nil
}

func orderishAddresses(addrs []string) {
	for i, a := range addrs {
		if strings.Contains(a, "127.0.0.1") {
			addrs[i], addrs[0] = addrs[0], addrs[i]
			return
		}
	}
}

type BW struct {
	TotalIn  int
	TotalOut int
}

func GetBW(n IpfsNode) (*BW, error) {
	addr, err := n.APIAddr()
	if err != nil {
		return nil, err
	}

	resp, err := http.Get("http://" + addr + "/api/v0/stats/bw")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var bw BW
	err = json.NewDecoder(resp.Body).Decode(&bw)
	if err != nil {
		return nil, err
	}

	return &bw, nil
}

const (
	attrId    = "id"
	attrPath  = "path"
	attrBwIn  = "bw_in"
	attrBwOut = "bw_out"
)

func GetListOfAttr() []string {
	return []string{attrId, attrPath, attrBwIn, attrBwOut}
}

func GetAttrDescr(attr string) (string, error) {
	switch attr {
	case attrId:
		return "node ID", nil
	case attrPath:
		return "node IPFS_PATH", nil
	case attrBwIn:
		return "node input bandwidth", nil
	case attrBwOut:
		return "node output bandwidth", nil
	default:
		return "", errors.New("unrecognized attribute")
	}
}

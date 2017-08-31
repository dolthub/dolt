package iptbutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	cnet "gx/ipfs/QmRebo5SY2EajaMx2Fnom21KHPWSivJRJHYwe58nZ5N8yC/go-ctrlnet"
	"gx/ipfs/QmZLUtHGe9HDQrreAYkXCzzK6mHVByV4MRd8heXAtV5wyS/stump"
)

type DockerNode struct {
	ImageName string
	ID        string

	apiAddr string

	LocalNode
}

var _ IpfsNode = &DockerNode{}

func (dn *DockerNode) Start(args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("cannot yet pass daemon args to docker nodes")
	}

	cmd := exec.Command("docker", "run", "-d", "-v", dn.Dir+":/data/ipfs", dn.ImageName)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %s", err, string(out))
	}

	id := bytes.TrimSpace(out)
	idfile := filepath.Join(dn.Dir, "dockerID")
	err = ioutil.WriteFile(idfile, id, 0664)
	if err != nil {
		return err
	}

	dn.ID = string(id)

	err = waitOnAPI(dn)
	if err != nil {
		return err
	}

	return nil
}

func (dn *DockerNode) setAPIAddr() error {
	internal, err := dn.LocalNode.APIAddr()
	if err != nil {
		return err
	}

	port := strings.Split(internal, ":")[1]

	dip, err := dn.getDockerIP()
	if err != nil {
		return err
	}

	dn.apiAddr = dip + ":" + port

	maddr := []byte("/ip4/" + dip + "/tcp/" + port)
	return ioutil.WriteFile(filepath.Join(dn.Dir, "api"), maddr, 0644)
}

func (dn *DockerNode) APIAddr() (string, error) {
	if dn.apiAddr == "" {
		if err := dn.setAPIAddr(); err != nil {
			return "", err
		}
	}

	return dn.apiAddr, nil
}

func (dn *DockerNode) getDockerIP() (string, error) {
	cmd := exec.Command("docker", "inspect", dn.ID)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s: %s", err, string(out))
	}

	var info []interface{}
	if err := json.Unmarshal(out, &info); err != nil {
		return "", err
	}

	if len(info) == 0 {
		return "", fmt.Errorf("got no inspect data")
	}

	cinfo := info[0].(map[string]interface{})
	netinfo := cinfo["NetworkSettings"].(map[string]interface{})
	return netinfo["IPAddress"].(string), nil
}

func (dn *DockerNode) Kill() error {
	out, err := exec.Command("docker", "kill", "--signal=INT", dn.ID).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %s", err, string(out))
	}

	return os.Remove(filepath.Join(dn.Dir, "dockerID"))
}

func (dn *DockerNode) String() string {
	return "docker:" + dn.PeerID
}

func (dn *DockerNode) RunCmd(args ...string) (string, error) {
	if dn.ID == "" {
		return "", fmt.Errorf("no docker id set on node")
	}

	args = append([]string{"exec", "-ti", dn.ID}, args...)
	cmd := exec.Command("docker", args...)
	cmd.Stdin = os.Stdin

	stump.VLog("running: ", cmd.Args)

	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("%s: %s", err, string(out))
	}

	return string(out), nil
}

func (dn *DockerNode) Shell() error {
	nodes, err := LoadNodes()
	if err != nil {
		return err
	}

	nenvs := os.Environ()
	for i, n := range nodes {
		peerid := n.GetPeerID()
		if peerid == "" {
			return fmt.Errorf("failed to check peerID")
		}

		nenvs = append(nenvs, fmt.Sprintf("NODE%d=%s", i, peerid))
	}

	cmd := exec.Command("docker", "exec", "-ti", dn.ID, "/bin/bash")
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	cmd.Stdin = os.Stdin

	return cmd.Run()
}

func (dn *DockerNode) GetAttr(name string) (string, error) {
	switch name {
	case "ifname":
		return dn.getInterfaceName()
	default:
		return dn.LocalNode.GetAttr(name)
	}
}

func (dn *DockerNode) SetAttr(name, val string) error {
	switch name {
	case "latency":
		return dn.setLatency(val)
	default:
		return fmt.Errorf("no attribute named: %s", name)
	}
}

func (dn *DockerNode) setLatency(val string) error {
	dur, err := time.ParseDuration(val)
	if err != nil {
		return err
	}

	ifn, err := dn.getInterfaceName()
	if err != nil {
		return err
	}

	settings := &cnet.LinkSettings{
		Latency: int(dur.Nanoseconds() / 1000000),
	}

	return cnet.SetLink(ifn, settings)
}

func (dn *DockerNode) getInterfaceName() (string, error) {
	out, err := dn.RunCmd("ip", "link")
	if err != nil {
		return "", err
	}

	var cside string
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, "@if") {
			ifnum := strings.Split(strings.Split(l, " ")[1], "@")[1]
			cside = ifnum[2 : len(ifnum)-1]
			break
		}
	}

	if cside == "" {
		return "", fmt.Errorf("container-side interface not found")
	}

	localout, err := exec.Command("ip", "link").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s: %s", err, localout)
	}

	for _, l := range strings.Split(string(localout), "\n") {
		if strings.HasPrefix(l, cside+": ") {
			return strings.Split(strings.Fields(l)[1], "@")[0], nil
		}
	}

	return "", fmt.Errorf("could not determine interface")
}

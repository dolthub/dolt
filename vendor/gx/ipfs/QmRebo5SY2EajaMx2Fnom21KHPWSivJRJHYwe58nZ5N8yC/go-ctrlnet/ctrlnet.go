package ctrlnet

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"strings"
)

type LinkSettings struct {
	Latency    int
	Jitter     int
	Bandwidth  int
	PacketLoss int
}

func (ls *LinkSettings) cmd(iface string, init bool) []string {
	var cmd = "change"
	if init {
		cmd = "add"
	}

	base := []string{"tc", "qdisc", cmd, "dev", iface, "root", "netem"}

	// even if latency is zero, put it on so the command never fails
	base = append(base, "delay", fmt.Sprintf("%dms", ls.Latency))
	if ls.Jitter > 0 {
		base = append(base, fmt.Sprintf("%dms", ls.Jitter), "distribution", "normal")
	}

	if ls.Bandwidth > 0 {
		base = append(base, "rate", fmt.Sprint(ls.Bandwidth))
	}

	if ls.PacketLoss > 0 {
		base = append(base, "loss", fmt.Sprintf("%d%%", ls.PacketLoss))
	}

	return base
}

func initLink(name string) (bool, error) {
	out, err := exec.Command("tc", "qdisc", "show").CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("dev listing failed: %s - %s", string(out), err)
	}

	lines := strings.Split(string(out), "\n")
	for _, l := range lines {
		if strings.Contains(l, name) && strings.Contains(l, "netem") {
			return false, nil
		}
	}

	return true, nil
}

func SetLink(name string, settings *LinkSettings) error {
	doinit, err := initLink(name)
	if err != nil {
		return err
	}
	args := settings.cmd(name, doinit)
	c := exec.Command(args[0], args[1:]...)
	out, err := c.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error setting link: %s - %s", string(out), err)
	}

	return nil
}

func GetInterfaces(filter string) ([]string, error) {
	ifs, err := ioutil.ReadDir("/sys/devices/virtual/net")
	if err != nil {
		return nil, err
	}
	var out []string
	for _, i := range ifs {
		if strings.Contains(i.Name(), filter) {
			out = append(out, i.Name())
		}
	}
	return out, nil
}

// +build linux

package docker

import "testing"

func TestGetDockerIDList(t *testing.T) {
	// If there is not docker environment, this test always fail.
	// not tested here
	/*
		_, err := GetDockerIDList()
		if err != nil {
			t.Errorf("error %v", err)
		}
	*/
}

func TestGetDockerStat(t *testing.T) {
	// If there is not docker environment, this test always fail.
	// not tested here

	/*
		ret, err := GetDockerStat()
		if err != nil {
			t.Errorf("error %v", err)
		}
		if len(ret) == 0 {
			t.Errorf("ret is empty")
		}
		empty := CgroupDockerStat{}
		for _, v := range ret {
			if empty == v {
				t.Errorf("empty CgroupDockerStat")
			}
			if v.ContainerID == "" {
				t.Errorf("Could not get container id")
			}
		}
	*/
}

func TestCgroupCPU(t *testing.T) {
	v, _ := GetDockerIDList()
	for _, id := range v {
		v, err := CgroupCPUDocker(id)
		if err != nil {
			t.Errorf("error %v", err)
		}
		if v.CPU == "" {
			t.Errorf("could not get CgroupCPU %v", v)
		}

	}
}

func TestCgroupCPUInvalidId(t *testing.T) {
	_, err := CgroupCPUDocker("bad id")
	if err == nil {
		t.Error("Expected path does not exist error")
	}
}

func TestCgroupMem(t *testing.T) {
	v, _ := GetDockerIDList()
	for _, id := range v {
		v, err := CgroupMemDocker(id)
		if err != nil {
			t.Errorf("error %v", err)
		}
		empty := &CgroupMemStat{}
		if v == empty {
			t.Errorf("Could not CgroupMemStat %v", v)
		}
	}
}

func TestCgroupMemInvalidId(t *testing.T) {
	_, err := CgroupMemDocker("bad id")
	if err == nil {
		t.Error("Expected path does not exist error")
	}
}

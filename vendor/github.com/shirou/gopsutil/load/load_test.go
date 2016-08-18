package load

import (
	"fmt"
	"testing"
)

func TestLoad(t *testing.T) {
	v, err := Avg()
	if err != nil {
		t.Errorf("error %v", err)
	}

	empty := &AvgStat{}
	if v == empty {
		t.Errorf("error load: %v", v)
	}
}

func TestLoadAvgStat_String(t *testing.T) {
	v := AvgStat{
		Load1:  10.1,
		Load5:  20.1,
		Load15: 30.1,
	}
	e := `{"load1":10.1,"load5":20.1,"load15":30.1}`
	if e != fmt.Sprintf("%v", v) {
		t.Errorf("LoadAvgStat string is invalid: %v", v)
	}
}

func TestMisc(t *testing.T) {
	v, err := Misc()
	if err != nil {
		t.Errorf("error %v", err)
	}

	empty := &MiscStat{}
	if v == empty {
		t.Errorf("error load: %v", v)
	}
}

func TestMiscStatString(t *testing.T) {
	v := MiscStat{
		ProcsRunning: 1,
		ProcsBlocked: 2,
		Ctxt:         3,
	}
	e := `{"procsRunning":1,"procsBlocked":2,"ctxt":3}`
	if e != fmt.Sprintf("%v", v) {
		t.Errorf("TestMiscString string is invalid: %v", v)
	}
}

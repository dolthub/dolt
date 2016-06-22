package buzhash

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

var loremipsum1 = `Lorem ipsum dolor sit amet, consectetuer adipiscing elit.
Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et
magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis,
ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis
enim. Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu. In
enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis
eu pede mollis pretium. Integer tincidunt. Cras dapibus. Vivamus elementum
semper nisi. Aenean vulputate eleifend tellus. Aenean leo ligula, porttitor eu,
consequat vitae, eleifend ac, enim. Aliquam lorem ante, dapibus in, viverra
quis, feugiat a, tellus. Phasellus viverra nulla ut metus varius laoreet.
Quisque rutrum. Aenean imperdiet. Etiam ultricies nisi vel augue. Curabitur
ullamcorper ultricies nisi. Nam eget dui.

Etiam rhoncus. Maecenas tempus, tellus eget condimentum rhoncus, sem quam semper
libero, sit amet adipiscing sem neque sed ipsum. Nam quam nunc, blandit vel,
luctus pulvinar, hendrerit id, lorem. Maecenas nec odio et ante tincidunt
tempus. Donec vitae sapien ut libero venenatis faucibus. Nullam quis ante. Etiam
sit amet orci eget eros faucibus tincidunt. Duis leo. Sed fringilla mauris sit
amet nibh. Donec sodales sagittis magna. Sed consequat, leo eget bibendum
sodales, augue velit cursus nunc, quis gravida magna mi a libero. Fusce
vulputate eleifend sapien. Vestibulum purus quam, scelerisque ut, mollis sed,
nonummy id, metus. Nullam accumsan lorem in dui. Cras ultricies mi eu turpis
hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et
ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia.`

var loremipsum2 = `Nam pretium turpis et arcu. Duis arcu tortor, suscipit eget,
imperdiet nec, imperdiet iaculis, ipsum. Sed aliquam ultrices mauris. Integer
ante arcu, accumsan a, consectetuer eget, posuere ut, mauris. Praesent
adipiscing. Phasellus ullamcorper ipsum rutrum nunc. Nunc nonummy metus.
Vestibulum volutpat pretium libero. Cras id dui. Aenean ut eros et nisl sagittis
vestibulum. Nullam nulla eros, ultricies sit amet, nonummy id, imperdiet
feugiat, pede. Sed lectus. Donec mollis hendrerit risus. Phasellus nec sem in
justo pellentesque facilisis. Etiam imperdiet imperdiet orci. Nunc nec neque.
Phasellus leo dolor, tempus non, auctor et, hendrerit quis, nisi.

Curabitur ligula sapien, tincidunt non, euismod vitae, posuere imperdiet, leo.
Maecenas malesuada. Praesent congue erat at massa. Sed cursus turpis vitae
tortor. Donec posuere vulputate arcu. Phasellus accumsan cursus velit.
Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia
Curae; Sed aliquam, nisi quis porttitor congue, elit erat euismod orci, ac
placerat dolor lectus quis orci. Phasellus consectetuer vestibulum elit. Aenean
tellus metus, bibendum sed, posuere ac, mattis non, nunc. Vestibulum fringilla
pede sit amet augue. In turpis. Pellentesque posuere. Praesent turpis.`

// Test the rolling hash property of the buzhash
func TestRollingHash(t *testing.T) {
	phrase1 := "Aenean massa. Cum sociis natoque"
	phrase2 := "Phasellus leo dolor, tempus non, auctor et, hendrerit quis, nisi"

	hasher1 := NewBuzHash(32)
	fmt.Fprint(hasher1, phrase1)
	p1sum := hasher1.Sum32()

	hasher1.Reset()
	found := false
	for idx, b := range []byte(loremipsum1) {
		ssum := hasher1.HashByte(b)
		if (ssum == p1sum) && (idx-32 == 91) {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Could not find '%s' by its checksum %08x.", phrase1, p1sum)
	}

	hasher2 := NewBuzHash(64)
	fmt.Fprint(hasher2, phrase2)
	p2sum := hasher2.Sum32()

	hasher2.Reset()
	found = false
	for idx, b := range []byte(loremipsum2) {
		ssum := hasher2.HashByte(b)
		if (ssum == p2sum) && (idx-64 == 592) {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Could not find '%s' by its checksum %08x.", phrase2, p2sum)
	}
}

func TestSum(t *testing.T) {
	h := NewBuzHash(16)
	fmt.Fprint(h, loremipsum1)

	sum32 := h.Sum32()
	sumBytes := h.Sum(nil)

	if l := len(sumBytes); l != 4 {
		t.Fatalf("h.Sum() returned slice of len %d, expected 4", l)
	}

	var sumBytesAsNum uint32
	if err := binary.Read(bytes.NewBuffer(sumBytes), binary.LittleEndian, &sumBytesAsNum); err != nil {
		t.Fatalf("Could not read binary number? %s", err)
	}

	if sum32 != sumBytesAsNum {
		t.Errorf("Sum32 (%08x) and Sum (%08x) returned different sums!", sum32, sumBytesAsNum)
	}
}

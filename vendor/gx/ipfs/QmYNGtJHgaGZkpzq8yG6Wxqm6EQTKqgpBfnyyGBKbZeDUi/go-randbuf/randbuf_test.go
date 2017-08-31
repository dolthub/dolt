package randbuf

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestMsg(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rb := RandBuf(r, 100)

	if len(rb) != 100 {
		t.Error("length incorrect")
	}

	fmt.Println("size:", len(rb)) // 1000
	fmt.Println("buf:", rb)
}

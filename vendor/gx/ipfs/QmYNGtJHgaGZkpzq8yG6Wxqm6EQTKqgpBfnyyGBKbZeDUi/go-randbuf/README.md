# go-randbuf - Generate a random `[]byte` of size n

```go
import (
  "fmt"
  randbuf "github.com/jbenet/go-randbuf"
  "math/rand"
  "time"
)

r := rand.New(rand.NewSource(time.Now().UnixNano()))
rb := randbuf.RandBuf(r, 100)

fmt.Println("size:", len(rb)) // 1000
fmt.Println("buf:", rb)
```

package retry

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/attic-labs/noms/d"
)

var (
	sleepFn = time.Sleep
)

func Request(url string, requestFn func() (*http.Response, error)) *http.Response {
	retries := []time.Duration{100 * time.Millisecond, 2 * time.Second, 5 * time.Second}

	body := func(resp *http.Response) string {
		b := &bytes.Buffer{}
		_, err := io.Copy(b, resp.Body)
		d.Chk.NoError(err)
		return b.String()
	}

	for i, dur := range retries {
		resp, err := requestFn()
		d.Chk.NoError(err)
		if class := resp.StatusCode / 100; class != 4 && class != 5 {
			return resp
		}
		fmt.Printf("Failed to fetch %s on attempt #%d, code %d, body %s. Trying again in %s.\n",
			url, i, resp.StatusCode, body(resp), dur.String())
		sleepFn(dur)
	}

	finalMsg := func(resp *http.Response) string {
		return fmt.Sprintf("Failed to fetch %s on final attempt #%d, code %d, body %s. Goodbye.\n",
			url, len(retries), resp.StatusCode, body(resp))
	}

	resp, err := requestFn()
	d.Chk.NoError(err)
	switch resp.StatusCode / 100 {
	case 4:
		d.Exp.Fail(finalMsg(resp))
	case 5:
		d.Chk.Fail(finalMsg(resp))
	}

	return resp
}

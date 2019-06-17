package retry

import "net/http"

func ProcessHttpResp(resp *http.Response, err error) RetriableCallState {
	if err == nil {
		switch resp.StatusCode / 100 {
		case 2:
			return Success
		case 3:
			return PermanentFailure
		case 4:
			return PermanentFailure
		case 5:
			return RetriableFailure
		}
	}

	return RetriableFailure
}

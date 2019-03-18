package main

import (
	"fmt"
	"github.com/attic-labs/noms/go/hash"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func ServeHTTP(respWr http.ResponseWriter, req *http.Request) {
	logger := getReqLogger("HTTP_"+req.Method, req.RequestURI)
	defer func() { logger("finished") }()

	path := strings.TrimLeft(req.URL.Path, "/")
	tokens := strings.Split(path, "/")

	if len(tokens) != 3 {
		logger(fmt.Sprintf("response to:", req.RequestURI, "method:", req.Method, "http response code: ", http.StatusNotFound))
		respWr.WriteHeader(http.StatusNotFound)
	}

	org := tokens[0]
	repo := tokens[1]
	hashStr := tokens[2]

	statusCode := http.StatusMethodNotAllowed
	switch req.Method {
	case http.MethodGet:
		query := req.URL.Query()
		statusCode = readChunk(logger, org, repo, hashStr, query, respWr)

	case http.MethodPost:
		statusCode = writeChunk(logger, org, repo, hashStr, req)
	}

	if statusCode != -1 {
		respWr.WriteHeader(statusCode)
	}
}

func writeChunk(logger func(string), org, repo, fileId string, request *http.Request) int {
	_, ok := hash.MaybeParse(fileId)

	if !ok {
		logger(fileId + " is not a valid hash")
		return http.StatusBadRequest
	}

	logger(fileId + " is valid")
	data, err := ioutil.ReadAll(request.Body)

	if err != nil {
		logger("failed to read body " + err.Error())
		return http.StatusInternalServerError
	}

	err = writeLocal(logger, org, repo, fileId, data)

	if err != nil {
		return http.StatusInternalServerError
	}

	return http.StatusOK
}

func writeLocal(logger func(string), org, repo, fileId string, data []byte) error {
	path := filepath.Join(org, repo, fileId)

	err := ioutil.WriteFile(path, data, os.ModePerm)

	if err != nil {
		logger(fmt.Sprintf("failed to write file %s", path))
		return err
	}

	logger("Successfully wrote object to storage")

	return nil
}

/*func writeS3(logger func(string), org, repo, fileId string, data []byte) error {
	putObjResp, err := s3Api.PutObject(&s3.PutObjectInput{
		Bucket:        bucket,
		Key:           &fileId,
		Body:          bytes.NewReader(data),
		ACL:           aws.String("private"),
		ContentLength: aws.Int64(int64(len(data))),
	})

	if err != nil {
		logger(fmt.Sprintln("failed to write to s3.", err))
		return err
	}

	logger(fmt.Sprintln("successfully written to s3.", putObjResp))

	return nil
}*/

func readChunk(logger func(string), org, repo, fileId string, query url.Values, writer io.Writer) int {
	offStr := query.Get("off")
	lenStr := query.Get("len")

	offset, err := strconv.ParseUint(offStr, 10, 64)

	if err != nil {
		logger(fmt.Sprintln(offStr, "is not a valid offset"))
		return http.StatusBadRequest
	}

	length, err := strconv.ParseUint(lenStr, 10, 32)

	if err != nil {
		logger(fmt.Sprintln(lenStr + "is not a valid length"))
		return http.StatusBadRequest
	}

	//data, retVal := readS3Range(logger, org, repo, fileId, int64(offset), int64(length))
	data, retVal := readLocalRange(logger, org, repo, fileId, int64(offset), int64(length))

	if retVal != -1 {
		return retVal
	}

	logger(fmt.Sprintf("writing %d bytes", len(data)))
	err = iohelp.WriteAll(writer, data)

	if err != nil {
		logger("failed to write data to response " + err.Error())
		return -1
	}

	logger("Successfully wrote data")
	return -1
}

func readLocalRange(logger func(string), org, repo, fileId string, offset, length int64) ([]byte, int) {
	path := filepath.Join(org, repo, fileId)

	logger(fmt.Sprintf("Attempting to read bytes %d to %d from %s", offset, offset+length, path))
	info, err := os.Stat(path)

	if err != nil {
		logger(fmt.Sprintf("file %s not found", path))
		return nil, http.StatusNotFound
	}

	logger(fmt.Sprintf("Verified file %s exists", path))

	if info.Size() < int64(offset+length) {
		logger(fmt.Sprintf("Attempted to read bytes %d to %d, but the file is only %d bytes in size", offset, offset+length, info.Size()))
		return nil, http.StatusBadRequest
	}

	logger(fmt.Sprintf("Verified the file is large enough to contain the range"))
	f, err := os.Open(path)

	if err != nil {
		logger(fmt.Sprintf("Failed to open %s", path))
		return nil, http.StatusInternalServerError
	}

	logger(fmt.Sprintf("Successfully opened file"))
	pos, err := f.Seek(int64(offset), 0)

	if err != nil {
		logger(fmt.Sprintf("Failed to seek to %d", offset))
		return nil, http.StatusInternalServerError
	}

	logger(fmt.Sprintf("Seek succeeded.  Current position is %d", pos))
	diff := int64(offset) - pos
	data, err := iohelp.ReadNBytes(f, int(diff+int64(length)))

	if err != nil {
		logger(fmt.Sprintf("Failed to read %d bytes", diff+int64(length)))
		return nil, http.StatusInternalServerError
	}

	logger(fmt.Sprintf("Successfully read %d bytes", len(data)))
	return data[diff:], -1
}

/*func readS3Range(logger func(string), org, repo, fileId string, offset, length int64) ([]byte, int) {
	rangeStr := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	getResp, err := s3Api.GetObject(&s3.GetObjectInput{
		Bucket: bucket,
		Key:    &fileId,
		Range:  aws.String(rangeStr),
	})

	if err != nil {
		logger(err.Error())

		if err.Error() == s3.ErrCodeNoSuchKey {
			return nil, http.StatusNotFound
		}

		return nil, http.StatusInternalServerError
	}

	data, err := ioutil.ReadAll(getResp.Body)

	if err != nil {
		logger(err.Error())

		return nil, http.StatusInternalServerError
	}

	return data, -1
}*/

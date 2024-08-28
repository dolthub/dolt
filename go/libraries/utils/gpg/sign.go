package gpg

import (
	"bytes"
	"context"
	"encoding/pem"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"os/exec"
	"strings"
)

func Sign(ctx context.Context, keyId string, message []byte) ([]byte, error) {
	args := []string{"--clear-sign", "-u", keyId}
	cmdStr := fmt.Sprintf("gpg %s", strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, "gpg", args...)

	stdOut, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to get stdout for command '%s': %w", cmdStr, err)
	}

	stdErr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to get stderr for command '%s': %w", cmdStr, err)
	}

	stdIn, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to get stdin for command '%s': %w", cmdStr, err)
	}

	err = cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to start command '%s': %w", cmdStr, err)
	}

	eg, egCtx := errgroup.WithContext(ctx)
	outBuf := listenToOut(egCtx, eg, stdOut)
	errBuf := listenToOut(egCtx, eg, stdErr)

	n, err := io.Copy(stdIn, strings.NewReader(string(message)))
	if err != nil {
		return nil, fmt.Errorf("failed to write stdin for command '%s': %w", cmdStr, err)
	} else if n < int64(len(message)) {
		return nil, fmt.Errorf("failed to write stdin for command '%s': EOF before all bytes written", cmd)
	}

	err = stdIn.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close stdin for command '%s': %w", cmdStr, err)
	}

	for {
		state, err := cmd.Process.Wait()
		if err != nil {
			return nil, fmt.Errorf("failed to wait for command '%s': %w", cmdStr, err)
		}

		if state.Exited() {
			if state.ExitCode() != 0 {
				return nil, fmt.Errorf("command '%s' exited with code %d. stdout: '%s', stderr: '%s'", cmdStr, state.ExitCode(), outBuf.String(), errBuf.String())
			}

			break
		}
	}

	err = eg.Wait()
	if err != nil {
		return nil, fmt.Errorf("failed to read output for command '%s': %w", cmdStr, err)
	}

	return outBuf.Bytes(), nil
}

func listenToOut(ctx context.Context, eg *errgroup.Group, r io.Reader) *bytes.Buffer {
	buf := bytes.NewBuffer(nil)
	eg.Go(func() error {
		_, err := io.Copy(buf, r)
		return err
	})
	return buf
}

// Throws away all intersperesed text and returns all decoded PEM blocks, in the order they are read.
func DecodeAllPEMBlocks(bs []byte) ([]*pem.Block, error) {
	const beginHeaderPrefix = "BEGIN "
	const pemSeperator = "-----"

	sections := strings.Split(string(bs), pemSeperator)
	filtered := make([]string, 0, len(sections))

	for i, section := range sections {
		section := strings.TrimSpace(section)

		if i == 0 || i == len(sections)-1 {
			if section == "" {
				continue
			}
		}

		filtered = append(filtered, section)
	}

	pemBlocks := make([]*pem.Block, 0, len(filtered))
	for i := 0; i < len(filtered); {
		headerName := filtered[i]
		i++

		if strings.HasPrefix(headerName, beginHeaderPrefix) {
			headerName = headerName[len(beginHeaderPrefix):]

			body := filtered[i]
			i++

			headers := make(map[string]string)
			lines := strings.Split(body, "\n")
			for j, line := range lines {
				trimmed := strings.TrimSpace(line)
				tokens := strings.Split(trimmed, ":")
				if len(tokens) == 2 {
					headers[strings.TrimSpace(tokens[0])] = strings.TrimSpace(tokens[1])
				} else {
					if j > 0 {
						if lines[j] == "" {
							j++
						}

						lines = lines[j:]
					}

					break
				}
			}

			body = strings.Join(lines, "\n")
			pemBlocks = append(pemBlocks, &pem.Block{
				Type:    headerName,
				Headers: headers,
				Bytes:   []byte(body),
			})
		}
	}

	return pemBlocks, nil
}

func GetBlocksOfType(blocks []*pem.Block, blTypeStr string) []*pem.Block {
	var ret []*pem.Block
	for _, block := range blocks {
		if block.Type == blTypeStr {
			ret = append(ret, block)
		}
	}
	return ret
}

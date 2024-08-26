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

	var signature []byte
	pemBlocks := decodeAllPEMBlocks(outBuf.Bytes())
	for _, block := range pemBlocks {
		fmt.Println("type:", block.Type)
		fmt.Println("headers:", block.Headers)
		fmt.Println("bytes:", block.Bytes)

		if block.Type == "SIGNATURE" {
			signature = block.Bytes
		}
	}

	return signature, nil
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
func decodeAllPEMBlocks(bs []byte) []*pem.Block {
	fmt.Println("decoding PEM blocks in:")
	fmt.Println(string(bs))

	var b *pem.Block
	var ret []*pem.Block
	for {
		b, bs = pem.Decode(bs)
		if b != nil {
			fmt.Println("decoded block")
			ret = append(ret, b)
		} else {
			fmt.Printf("decoded '%d' blocks\n", len(ret))
			return ret
		}
	}
}

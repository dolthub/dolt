// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package git

import (
	"context"
	"fmt"
	"strings"
)

// WriteAPIImpl implements WriteAPI using the git CLI plumbing commands, via Runner.
// It performs updates without requiring a working tree checkout.
type WriteAPIImpl struct {
	r *Runner
}

var _ WriteAPI = (*WriteAPIImpl)(nil)

func NewWriteAPIImpl(r *Runner) *WriteAPIImpl {
	return &WriteAPIImpl{r: r}
}

func (p *WriteAPIImpl) ReadTree(ctx context.Context, commit OID, indexFile string) error {
	_, err := p.r.Run(ctx, RunOptions{IndexFile: indexFile}, "read-tree", commit.String()+"^{tree}")
	return err
}

func (p *WriteAPIImpl) ReadTreeEmpty(ctx context.Context, indexFile string) error {
	_, err := p.r.Run(ctx, RunOptions{IndexFile: indexFile}, "read-tree", "--empty")
	return err
}

func (p *WriteAPIImpl) UpdateIndexCacheInfo(ctx context.Context, indexFile string, mode string, oid OID, path string) error {
	_, err := p.r.Run(ctx, RunOptions{IndexFile: indexFile}, "update-index", "--add", "--cacheinfo", mode, oid.String(), path)
	return err
}

func (p *WriteAPIImpl) WriteTree(ctx context.Context, indexFile string) (OID, error) {
	out, err := p.r.Run(ctx, RunOptions{IndexFile: indexFile}, "write-tree")
	if err != nil {
		return "", err
	}
	oid := strings.TrimSpace(string(out))
	if oid == "" {
		return "", fmt.Errorf("git write-tree returned empty oid")
	}
	return OID(oid), nil
}

func (p *WriteAPIImpl) CommitTree(ctx context.Context, tree OID, parent *OID, message string, author *Identity) (OID, error) {
	args := []string{"commit-tree", tree.String(), "-m", message}
	if parent != nil && parent.String() != "" {
		args = append(args, "-p", parent.String())
	}

	var env []string
	if author != nil {
		if author.Name != "" {
			env = append(env,
				"GIT_AUTHOR_NAME="+author.Name,
				"GIT_COMMITTER_NAME="+author.Name,
			)
		}
		if author.Email != "" {
			env = append(env,
				"GIT_AUTHOR_EMAIL="+author.Email,
				"GIT_COMMITTER_EMAIL="+author.Email,
			)
		}
	}

	out, err := p.r.Run(ctx, RunOptions{Env: env}, args...)
	if err != nil {
		return "", err
	}
	oid := strings.TrimSpace(string(out))
	if oid == "" {
		return "", fmt.Errorf("git commit-tree returned empty oid")
	}
	return OID(oid), nil
}

func (p *WriteAPIImpl) UpdateRefCAS(ctx context.Context, ref string, newOID OID, oldOID OID, msg string) error {
	args := []string{"update-ref"}
	if msg != "" {
		args = append(args, "-m", msg)
	}
	args = append(args, ref, newOID.String(), oldOID.String())
	_, err := p.r.Run(ctx, RunOptions{}, args...)
	return err
}

func (p *WriteAPIImpl) UpdateRef(ctx context.Context, ref string, newOID OID, msg string) error {
	args := []string{"update-ref"}
	if msg != "" {
		args = append(args, "-m", msg)
	}
	args = append(args, ref, newOID.String())
	_, err := p.r.Run(ctx, RunOptions{}, args...)
	return err
}

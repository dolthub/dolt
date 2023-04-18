// Copyright 2022 Dolthub, Inc.
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

package import_benchmarker

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/pkg/errors"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// getSizeOnDisk returns the size of the .dolt repo. This is useful for understanding how a repo grows in size in
// proportion to the number of rows.
func getSizeOnDisk(fs filesys.Filesys, workingDir string) (float64, error) {
	doltDir := filepath.Join(workingDir, dbfactory.DoltDir)
	exists, _ := fs.Exists(doltDir)

	if !exists {
		return 0, errors.New("dir does not exist")
	}

	size, err := dirSizeMB(doltDir)
	if err != nil {
		return 0, err
	}

	roundedStr := fmt.Sprintf("%.2f", size)
	rounded, _ := strconv.ParseFloat(roundedStr, 2)

	return rounded, nil
}

// cc: https://stackoverflow.com/questions/32482673/how-to-get-directory-total-size
func dirSizeMB(path string) (float64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})

	sizeMB := float64(size) / 1024.0 / 1024.0

	return sizeMB, err
}

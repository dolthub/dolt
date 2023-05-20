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

package sql_server_driver

import (
	"golang.org/x/sys/windows"
)

func (s *SqlServer) GracefulStop() error {
	err := windows.GenerateConsoleCtrlEvent(windows.CTRL_BREAK_EVENT, uint32(s.Cmd.Process.Pid))
	if err != nil {
		return err
	}
	
	<-s.Done

	_, err = s.Cmd.Process.Wait()
	if err != nil {
		return err
	}

	return nil
}

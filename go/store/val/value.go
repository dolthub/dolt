// Copyright 2021 Dolthub, Inc.
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

package val

type Value struct {
	Enc Encoding
	Val []byte
}

func (v Value) Null() bool {
	return v.Val == nil
}

func (v Value) size() byteSize {
	return byteSize(len(v.Val))
}

func (v Value) fixedSize() bool {
	return FixedWidth(v.Enc)
}

// Copyright 2019-2022 Dolthub, Inc.
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

package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var ExpectedHeader = regexp.MustCompile(`// Copyright (2019|2020|2021|2022|2023|2024|2025|2019-2020|2019-2021|2019-2022|2019-2023|2019-2024|2019-2025|2020-2021|2020-2022|2020-2023|2020-2024|2020-2025|2021-2022|2021-2023|2021-2024|2021-2025|2022-2023|2022-2024|2022-2025|2023-2024|2023-2025|2024-2025) Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 \(the "License"\);
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

`)

var ExpectedHeaderForFileFromNoms = regexp.MustCompile(`// Copyright (2019|2020|2021|2022|2023|2019-2020|2019-2021|2019-2022|2019-2023|2020-2021|2020-2022|2020-2023|2021-2022|2021-2023|2022-2023) Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 \(the "License"\);
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright`)

type CopiedNomsFile struct {
	Path               string
	NomsPath           string
	HadCopyrightNotice bool
}

// Noms file paths referenced from a1f990c94dcc03f9f1845d25a55e84108f1be673 in github.com/attic-labs/noms.
var CopiedNomsFiles []CopiedNomsFile = []CopiedNomsFile{
	// These started as slightly modified copies of existing noms value implementations.
	{Path: "store/types/float.go", NomsPath: "go/types/number.go", HadCopyrightNotice: true},
	{Path: "store/types/float_util.go", NomsPath: "go/types/number_util.go", HadCopyrightNotice: true},
	{Path: "store/types/int.go", NomsPath: "go/types/number.go", HadCopyrightNotice: true},
	{Path: "store/types/null_value.go", NomsPath: "go/types/number.go", HadCopyrightNotice: true},
	{Path: "store/types/tuple.go", NomsPath: "go/types/struct.go", HadCopyrightNotice: true},
	{Path: "store/types/uint.go", NomsPath: "go/types/number.go", HadCopyrightNotice: true},
	{Path: "store/prolly/tree/chunker.go", NomsPath: "go/types/sequence_chunker.go", HadCopyrightNotice: true},
	{Path: "store/prolly/tree/node_cursor.go", NomsPath: "go/types/sequence_cursor.go", HadCopyrightNotice: true},
	{Path: "store/prolly/tree/node_splitter.go", NomsPath: "go/types/rolling_value_hasher.go", HadCopyrightNotice: true},

	// These included source files from noms did not have copyright notices.
	{Path: "store/types/common_supertype.go", NomsPath: "go/types/common_supertype.go", HadCopyrightNotice: false},
	{Path: "store/types/common_supertype_test.go", NomsPath: "go/types/common_supertype_test.go", HadCopyrightNotice: false},
	{Path: "store/types/simplify.go", NomsPath: "go/types/simplify.go", HadCopyrightNotice: false},
	{Path: "store/types/simplify_test.go", NomsPath: "go/types/simplify_test.go", HadCopyrightNotice: false},
	{Path: "store/util/random/id.go", NomsPath: "go/util/random/id.go", HadCopyrightNotice: false},
	{Path: "store/util/random/id_test.go", NomsPath: "go/util/random/id_test.go", HadCopyrightNotice: false},

	// These files started as almost direct copies, potentially with some moving.
	{Path: "store/cmd/noms/util/check_error.go", NomsPath: "go/d/check_error.go", HadCopyrightNotice: true},
	{Path: "store/cmd/noms/util/command.go", NomsPath: "cmd/util/command.go", HadCopyrightNotice: true},
	{Path: "store/cmd/noms/util/help.go", NomsPath: "cmd/util/help.go", HadCopyrightNotice: true},
	{Path: "store/cmd/noms/util/kingpin_command.go", NomsPath: "cmd/util/kingpin_command.go", HadCopyrightNotice: true},
	{Path: "store/chunks/chunk.go", NomsPath: "go/chunks/chunk.go", HadCopyrightNotice: true},
	{Path: "store/chunks/chunk_store.go", NomsPath: "go/chunks/chunk_store.go", HadCopyrightNotice: true},
	{Path: "store/chunks/chunk_store_common_test.go", NomsPath: "go/chunks/chunk_store_common_test.go", HadCopyrightNotice: true},
	{Path: "store/chunks/chunk_test.go", NomsPath: "go/chunks/chunk_test.go", HadCopyrightNotice: true},
	{Path: "store/chunks/memory_store.go", NomsPath: "go/chunks/memory_store.go", HadCopyrightNotice: true},
	{Path: "store/chunks/memory_store_test.go", NomsPath: "go/chunks/memory_store_test.go", HadCopyrightNotice: true},
	{Path: "store/chunks/test_utils.go", NomsPath: "go/chunks/test_utils.go", HadCopyrightNotice: true},
	{Path: "store/cmd/noms/commit_iterator.go", NomsPath: "cmd/noms/commit_iterator.go", HadCopyrightNotice: true},
	{Path: "store/cmd/noms/noms.go", NomsPath: "cmd/noms/noms.go", HadCopyrightNotice: true},
	{Path: "store/cmd/noms/noms_blob.go", NomsPath: "cmd/noms/noms_blob.go", HadCopyrightNotice: true},
	{Path: "store/cmd/noms/noms_blob_get.go", NomsPath: "cmd/noms/noms_blob_get.go", HadCopyrightNotice: true},
	{Path: "store/cmd/noms/noms_blob_get_test.go", NomsPath: "cmd/noms/noms_blob_get_test.go", HadCopyrightNotice: true},
	{Path: "store/cmd/noms/noms_blob_put.go", NomsPath: "cmd/noms/noms_blob_put.go", HadCopyrightNotice: true},
	{Path: "store/cmd/noms/noms_config.go", NomsPath: "cmd/noms/noms_config.go", HadCopyrightNotice: true},
	{Path: "store/cmd/noms/noms_ds.go", NomsPath: "cmd/noms/noms_ds.go", HadCopyrightNotice: true},
	{Path: "store/cmd/noms/noms_ds_test.go", NomsPath: "cmd/noms/noms_ds_test.go", HadCopyrightNotice: true},
	{Path: "store/cmd/noms/noms_root.go", NomsPath: "cmd/noms/noms_root.go", HadCopyrightNotice: true},
	{Path: "store/cmd/noms/noms_root_test.go", NomsPath: "cmd/noms/noms_root_test.go", HadCopyrightNotice: true},
	{Path: "store/cmd/noms/noms_show.go", NomsPath: "cmd/noms/noms_show.go", HadCopyrightNotice: true},
	{Path: "store/cmd/noms/noms_show_test.go", NomsPath: "cmd/noms/noms_show_test.go", HadCopyrightNotice: true},
	{Path: "store/cmd/noms/noms_stats.go", NomsPath: "cmd/noms/noms_stats.go", HadCopyrightNotice: true},
	{Path: "store/config/config.go", NomsPath: "go/config/config.go", HadCopyrightNotice: true},
	{Path: "store/config/config_test.go", NomsPath: "go/config/config_test.go", HadCopyrightNotice: true},
	{Path: "store/config/resolver.go", NomsPath: "go/config/resolver.go", HadCopyrightNotice: true},
	{Path: "store/config/resolver_test.go", NomsPath: "go/config/resolver_test.go", HadCopyrightNotice: true},
	{Path: "store/constants/version.go", NomsPath: "go/constants/version.go", HadCopyrightNotice: true},
	{Path: "store/d/try.go", NomsPath: "go/d/try.go", HadCopyrightNotice: true},
	{Path: "store/d/try_test.go", NomsPath: "go/d/try_test.go", HadCopyrightNotice: true},
	{Path: "store/datas/commit.go", NomsPath: "go/datas/commit.go", HadCopyrightNotice: true},
	{Path: "store/datas/commit_options.go", NomsPath: "go/datas/commit_options.go", HadCopyrightNotice: true},
	{Path: "store/datas/commit_test.go", NomsPath: "go/datas/commit_test.go", HadCopyrightNotice: true},
	{Path: "store/datas/database.go", NomsPath: "go/datas/database.go", HadCopyrightNotice: true},
	{Path: "store/datas/database_common.go", NomsPath: "go/datas/database_common.go", HadCopyrightNotice: true},
	{Path: "store/datas/database_test.go", NomsPath: "go/datas/database_test.go", HadCopyrightNotice: true},
	{Path: "store/datas/dataset.go", NomsPath: "go/datas/dataset.go", HadCopyrightNotice: true},
	{Path: "store/datas/dataset_test.go", NomsPath: "go/datas/dataset_test.go", HadCopyrightNotice: true},
	{Path: "store/diff/apply_patch.go", NomsPath: "go/diff/apply_patch.go", HadCopyrightNotice: true},
	{Path: "store/diff/apply_patch_test.go", NomsPath: "go/diff/apply_patch_test.go", HadCopyrightNotice: true},
	{Path: "store/diff/diff.go", NomsPath: "go/diff/diff.go", HadCopyrightNotice: true},
	{Path: "store/diff/diff_test.go", NomsPath: "go/diff/diff_test.go", HadCopyrightNotice: true},
	{Path: "store/diff/patch.go", NomsPath: "go/diff/patch.go", HadCopyrightNotice: true},
	{Path: "store/diff/patch_test.go", NomsPath: "go/diff/patch_test.go", HadCopyrightNotice: true},
	{Path: "store/diff/print_diff.go", NomsPath: "go/diff/print_diff.go", HadCopyrightNotice: true},
	{Path: "store/hash/base32.go", NomsPath: "go/hash/base32.go", HadCopyrightNotice: true},
	{Path: "store/hash/base32_test.go", NomsPath: "go/hash/base32_test.go", HadCopyrightNotice: true},
	{Path: "store/hash/hash.go", NomsPath: "go/hash/hash.go", HadCopyrightNotice: true},
	{Path: "store/hash/hash_slice.go", NomsPath: "go/hash/hash_slice.go", HadCopyrightNotice: true},
	{Path: "store/hash/hash_slice_test.go", NomsPath: "go/hash/hash_slice_test.go", HadCopyrightNotice: true},
	{Path: "store/hash/hash_test.go", NomsPath: "go/hash/hash_test.go", HadCopyrightNotice: true},
	{Path: "store/marshal/decode.go", NomsPath: "go/marshal/decode.go", HadCopyrightNotice: true},
	{Path: "store/marshal/decode_test.go", NomsPath: "go/marshal/decode_test.go", HadCopyrightNotice: true},
	{Path: "store/marshal/encode.go", NomsPath: "go/marshal/encode.go", HadCopyrightNotice: true},
	{Path: "store/marshal/encode_test.go", NomsPath: "go/marshal/encode_test.go", HadCopyrightNotice: true},
	{Path: "store/marshal/encode_type.go", NomsPath: "go/marshal/encode_type.go", HadCopyrightNotice: true},
	{Path: "store/marshal/encode_type_test.go", NomsPath: "go/marshal/encode_type_test.go", HadCopyrightNotice: true},
	{Path: "store/merge/candidate.go", NomsPath: "go/merge/candidate.go", HadCopyrightNotice: true},
	{Path: "store/merge/three_way.go", NomsPath: "go/merge/three_way.go", HadCopyrightNotice: true},
	{Path: "store/merge/three_way_keyval_test.go", NomsPath: "go/merge/three_way_keyval_test.go", HadCopyrightNotice: true},
	{Path: "store/merge/three_way_list.go", NomsPath: "go/merge/three_way_list.go", HadCopyrightNotice: true},
	{Path: "store/merge/three_way_list_test.go", NomsPath: "go/merge/three_way_list_test.go", HadCopyrightNotice: true},
	{Path: "store/merge/three_way_ordered_sequence.go", NomsPath: "go/merge/three_way_ordered_sequence.go", HadCopyrightNotice: true},
	{Path: "store/merge/three_way_set_test.go", NomsPath: "go/merge/three_way_set_test.go", HadCopyrightNotice: true},
	{Path: "store/merge/three_way_test.go", NomsPath: "go/merge/three_way_test.go", HadCopyrightNotice: true},
	{Path: "store/metrics/histogram.go", NomsPath: "go/metrics/histogram.go", HadCopyrightNotice: true},
	{Path: "store/metrics/histogram_test.go", NomsPath: "go/metrics/histogram_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/aws_table_chunk_source.go", NomsPath: "go/nbs/aws_chunk_source.go", HadCopyrightNotice: true},
	{Path: "store/nbs/aws_table_chunk_source_test.go", NomsPath: "go/nbs/aws_chunk_source_test.go", HadCopyrightNotice: true},

	{Path: "store/nbs/aws_table_persister.go", NomsPath: "go/nbs/aws_table_persister.go", HadCopyrightNotice: true},
	{Path: "store/nbs/aws_table_persister_test.go", NomsPath: "go/nbs/aws_table_persister_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/benchmarks/block_store_benchmarks.go", NomsPath: "go/nbs/benchmarks/block_store_benchmarks.go", HadCopyrightNotice: true},
	{Path: "store/nbs/benchmarks/cachedrop/drop_cache.go", NomsPath: "go/nbs/benchmarks/cachedrop/drop_cache.go", HadCopyrightNotice: true},
	{Path: "store/nbs/benchmarks/chunker/main.go", NomsPath: "go/nbs/benchmarks/chunker/main.go", HadCopyrightNotice: true},
	{Path: "store/nbs/benchmarks/data_source.go", NomsPath: "go/nbs/benchmarks/data_source.go", HadCopyrightNotice: true},
	{Path: "store/nbs/benchmarks/drop_cache_linux.go", NomsPath: "go/nbs/benchmarks/drop_cache_linux.go", HadCopyrightNotice: true},
	{Path: "store/nbs/benchmarks/drop_cache_other.go", NomsPath: "go/nbs/benchmarks/drop_cache_other.go", HadCopyrightNotice: true},
	{Path: "store/nbs/benchmarks/file_block_store.go", NomsPath: "go/nbs/benchmarks/file_block_store.go", HadCopyrightNotice: true},
	{Path: "store/nbs/benchmarks/gen/gen.go", NomsPath: "go/nbs/benchmarks/gen/gen.go", HadCopyrightNotice: true},
	{Path: "store/nbs/benchmarks/gen/rolling_value_hasher.go", NomsPath: "go/nbs/benchmarks/gen/rolling_value_hasher.go", HadCopyrightNotice: true},
	{Path: "store/nbs/benchmarks/main.go", NomsPath: "go/nbs/benchmarks/main.go", HadCopyrightNotice: true},
	{Path: "store/nbs/benchmarks/null_block_store.go", NomsPath: "go/nbs/benchmarks/null_block_store.go", HadCopyrightNotice: true},
	{Path: "store/nbs/block_store_test.go", NomsPath: "go/nbs/block_store_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/conjoiner.go", NomsPath: "go/nbs/conjoiner.go", HadCopyrightNotice: true},
	{Path: "store/nbs/conjoiner_test.go", NomsPath: "go/nbs/conjoiner_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/dynamo_fake_test.go", NomsPath: "go/nbs/dynamo_fake_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/dynamo_manifest.go", NomsPath: "go/nbs/dynamo_manifest.go", HadCopyrightNotice: true},
	{Path: "store/nbs/dynamo_manifest_test.go", NomsPath: "go/nbs/dynamo_manifest_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/file_manifest.go", NomsPath: "go/nbs/file_manifest.go", HadCopyrightNotice: true},
	{Path: "store/nbs/file_manifest_test.go", NomsPath: "go/nbs/file_manifest_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/file_table_persister.go", NomsPath: "go/nbs/file_table_persister.go", HadCopyrightNotice: true},
	{Path: "store/nbs/file_table_persister_test.go", NomsPath: "go/nbs/file_table_persister_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/frag/main.go", NomsPath: "go/nbs/frag/main.go", HadCopyrightNotice: true},
	{Path: "store/nbs/manifest.go", NomsPath: "go/nbs/manifest.go", HadCopyrightNotice: true},
	{Path: "store/nbs/manifest_cache.go", NomsPath: "go/nbs/manifest_cache.go", HadCopyrightNotice: true},
	{Path: "store/nbs/manifest_cache_test.go", NomsPath: "go/nbs/manifest_cache_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/mem_table.go", NomsPath: "go/nbs/mem_table.go", HadCopyrightNotice: true},
	{Path: "store/nbs/mem_table_test.go", NomsPath: "go/nbs/mem_table_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/file_table_reader.go", NomsPath: "go/nbs/mmap_table_reader.go", HadCopyrightNotice: true},
	{Path: "store/nbs/file_table_reader_test.go", NomsPath: "go/nbs/mmap_table_reader_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/empty_chunk_source.go", NomsPath: "go/nbs/empty_chunk_source.go", HadCopyrightNotice: true},
	{Path: "store/nbs/root_tracker_test.go", NomsPath: "go/nbs/root_tracker_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/s3_fake_test.go", NomsPath: "go/nbs/s3_fake_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/s3_table_reader.go", NomsPath: "go/nbs/s3_table_reader.go", HadCopyrightNotice: true},
	{Path: "store/nbs/s3_table_reader_test.go", NomsPath: "go/nbs/s3_table_reader_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/stats.go", NomsPath: "go/nbs/stats.go", HadCopyrightNotice: true},
	{Path: "store/nbs/stats_test.go", NomsPath: "go/nbs/stats_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/store.go", NomsPath: "go/nbs/store.go", HadCopyrightNotice: true},
	{Path: "store/nbs/table.go", NomsPath: "go/nbs/table.go", HadCopyrightNotice: true},
	{Path: "store/nbs/table_persister.go", NomsPath: "go/nbs/table_persister.go", HadCopyrightNotice: true},
	{Path: "store/nbs/table_persister_test.go", NomsPath: "go/nbs/table_persister_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/table_reader.go", NomsPath: "go/nbs/table_reader.go", HadCopyrightNotice: true},
	{Path: "store/nbs/table_set.go", NomsPath: "go/nbs/table_set.go", HadCopyrightNotice: true},
	{Path: "store/nbs/table_set_test.go", NomsPath: "go/nbs/table_set_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/table_test.go", NomsPath: "go/nbs/table_test.go", HadCopyrightNotice: true},
	{Path: "store/nbs/table_writer.go", NomsPath: "go/nbs/table_writer.go", HadCopyrightNotice: true},
	{Path: "store/nbs/test/manifest_clobber.go", NomsPath: "go/nbs/test/manifest_clobber.go", HadCopyrightNotice: true},
	{Path: "store/nomdl/lexer.go", NomsPath: "go/nomdl/lexer.go", HadCopyrightNotice: true},
	{Path: "store/nomdl/parser.go", NomsPath: "go/nomdl/parser.go", HadCopyrightNotice: true},
	{Path: "store/nomdl/parser_test.go", NomsPath: "go/nomdl/parser_test.go", HadCopyrightNotice: true},
	{Path: "store/perf/codec-perf-rig/main.go", NomsPath: "go/perf/codec-perf-rig/main.go", HadCopyrightNotice: true},
	{Path: "store/perf/suite/suite.go", NomsPath: "go/perf/suite/suite.go", HadCopyrightNotice: true},
	{Path: "store/perf/suite/suite_test.go", NomsPath: "go/perf/suite/suite_test.go", HadCopyrightNotice: true},
	{Path: "store/sloppy/sloppy.go", NomsPath: "go/sloppy/sloppy.go", HadCopyrightNotice: true},
	{Path: "store/sloppy/sloppy_test.go", NomsPath: "go/sloppy/sloppy_test.go", HadCopyrightNotice: true},
	{Path: "store/spec/absolute_path.go", NomsPath: "go/spec/absolute_path.go", HadCopyrightNotice: true},
	{Path: "store/spec/absolute_path_test.go", NomsPath: "go/spec/absolute_path_test.go", HadCopyrightNotice: true},
	{Path: "store/spec/spec.go", NomsPath: "go/spec/spec.go", HadCopyrightNotice: true},
	{Path: "store/spec/spec_test.go", NomsPath: "go/spec/spec_test.go", HadCopyrightNotice: true},
	{Path: "store/spec/util.go", NomsPath: "go/spec/util.go", HadCopyrightNotice: true},
	{Path: "store/types/blob.go", NomsPath: "go/types/blob.go", HadCopyrightNotice: true},
	{Path: "store/types/blob_leaf_sequence.go", NomsPath: "go/types/blob_leaf_sequence.go", HadCopyrightNotice: true},
	{Path: "store/types/blob_test.go", NomsPath: "go/types/blob_test.go", HadCopyrightNotice: true},
	{Path: "store/types/bool.go", NomsPath: "go/types/bool.go", HadCopyrightNotice: true},
	{Path: "store/types/codec.go", NomsPath: "go/types/codec.go", HadCopyrightNotice: true},
	{Path: "store/types/codec_test.go", NomsPath: "go/types/codec_test.go", HadCopyrightNotice: true},
	{Path: "store/types/collection.go", NomsPath: "go/types/collection.go", HadCopyrightNotice: true},
	{Path: "store/types/collection_test.go", NomsPath: "go/types/collection_test.go", HadCopyrightNotice: true},
	{Path: "store/types/compare_test.go", NomsPath: "go/types/compare_test.go", HadCopyrightNotice: true},
	{Path: "store/types/edit_distance.go", NomsPath: "go/types/edit_distance.go", HadCopyrightNotice: true},
	{Path: "store/types/edit_distance_test.go", NomsPath: "go/types/edit_distance_test.go", HadCopyrightNotice: true},
	{Path: "store/types/encode_human_readable.go", NomsPath: "go/types/encode_human_readable.go", HadCopyrightNotice: true},
	{Path: "store/types/encode_human_readable_test.go", NomsPath: "go/types/encode_human_readable_test.go", HadCopyrightNotice: true},
	{Path: "store/types/encoding_test.go", NomsPath: "go/types/encoding_test.go", HadCopyrightNotice: true},
	{Path: "store/types/equals_test.go", NomsPath: "go/types/equals_test.go", HadCopyrightNotice: true},
	{Path: "store/types/get_hash.go", NomsPath: "go/types/get_hash.go", HadCopyrightNotice: true},
	{Path: "store/types/incremental_test.go", NomsPath: "go/types/incremental_test.go", HadCopyrightNotice: true},
	{Path: "store/types/indexed_sequence_diff.go", NomsPath: "go/types/indexed_sequence_diff.go", HadCopyrightNotice: true},
	{Path: "store/types/indexed_sequences.go", NomsPath: "go/types/indexed_sequences.go", HadCopyrightNotice: true},
	{Path: "store/types/leaf_sequence.go", NomsPath: "go/types/leaf_sequence.go", HadCopyrightNotice: true},
	{Path: "store/types/less.go", NomsPath: "go/types/less.go", HadCopyrightNotice: true},
	{Path: "store/types/list.go", NomsPath: "go/types/list.go", HadCopyrightNotice: true},
	{Path: "store/types/list_editor.go", NomsPath: "go/types/list_editor.go", HadCopyrightNotice: true},
	{Path: "store/types/list_editor_test.go", NomsPath: "go/types/list_editor_test.go", HadCopyrightNotice: true},
	{Path: "store/types/list_iterator.go", NomsPath: "go/types/list_iterator.go", HadCopyrightNotice: true},
	{Path: "store/types/list_iterator_test.go", NomsPath: "go/types/list_iterator_test.go", HadCopyrightNotice: true},
	{Path: "store/types/list_leaf_sequence.go", NomsPath: "go/types/list_leaf_sequence.go", HadCopyrightNotice: true},
	{Path: "store/types/list_test.go", NomsPath: "go/types/list_test.go", HadCopyrightNotice: true},
	{Path: "store/types/make_type.go", NomsPath: "go/types/make_type.go", HadCopyrightNotice: true},
	{Path: "store/types/map.go", NomsPath: "go/types/map.go", HadCopyrightNotice: true},
	{Path: "store/types/map_editor.go", NomsPath: "go/types/map_editor.go", HadCopyrightNotice: true},
	{Path: "store/types/map_iterator.go", NomsPath: "go/types/map_iterator.go", HadCopyrightNotice: true},
	{Path: "store/types/map_iterator_test.go", NomsPath: "go/types/map_iterator_test.go", HadCopyrightNotice: true},
	{Path: "store/types/map_leaf_sequence.go", NomsPath: "go/types/map_leaf_sequence.go", HadCopyrightNotice: true},
	{Path: "store/types/map_test.go", NomsPath: "go/types/map_test.go", HadCopyrightNotice: true},
	{Path: "store/types/meta_sequence.go", NomsPath: "go/types/meta_sequence.go", HadCopyrightNotice: true},
	{Path: "store/types/noms_kind.go", NomsPath: "go/types/noms_kind.go", HadCopyrightNotice: true},
	{Path: "store/types/noms_kind_test.go", NomsPath: "go/types/noms_kind_test.go", HadCopyrightNotice: true},
	{Path: "store/types/ordered_sequences.go", NomsPath: "go/types/ordered_sequences.go", HadCopyrightNotice: true},
	{Path: "store/types/ordered_sequences_diff.go", NomsPath: "go/types/ordered_sequences_diff.go", HadCopyrightNotice: true},
	{Path: "store/types/ordered_sequences_diff_test.go", NomsPath: "go/types/ordered_sequences_diff_test.go", HadCopyrightNotice: true},
	{Path: "store/types/path.go", NomsPath: "go/types/path.go", HadCopyrightNotice: true},
	{Path: "store/types/path_test.go", NomsPath: "go/types/path_test.go", HadCopyrightNotice: true},
	{Path: "store/types/perf/dummy.go", NomsPath: "go/types/perf/dummy.go", HadCopyrightNotice: true},
	{Path: "store/types/perf/perf_test.go", NomsPath: "go/types/perf/perf_test.go", HadCopyrightNotice: true},
	{Path: "store/types/primitives_test.go", NomsPath: "go/types/primitives_test.go", HadCopyrightNotice: true},
	{Path: "store/types/ref.go", NomsPath: "go/types/ref.go", HadCopyrightNotice: true},
	{Path: "store/types/ref_test.go", NomsPath: "go/types/ref_test.go", HadCopyrightNotice: true},
	{Path: "store/types/rolling_value_hasher.go", NomsPath: "go/types/rolling_value_hasher.go", HadCopyrightNotice: true},
	{Path: "store/types/sequence.go", NomsPath: "go/types/sequence.go", HadCopyrightNotice: true},
	{Path: "store/types/sequence_chunker.go", NomsPath: "go/types/sequence_chunker.go", HadCopyrightNotice: true},
	{Path: "store/types/sequence_concat.go", NomsPath: "go/types/sequence_concat.go", HadCopyrightNotice: true},
	{Path: "store/types/sequence_cursor.go", NomsPath: "go/types/sequence_cursor.go", HadCopyrightNotice: true},
	{Path: "store/types/sequence_cursor_test.go", NomsPath: "go/types/sequence_cursor_test.go", HadCopyrightNotice: true},
	{Path: "store/types/set.go", NomsPath: "go/types/set.go", HadCopyrightNotice: true},
	{Path: "store/types/set_editor.go", NomsPath: "go/types/set_editor.go", HadCopyrightNotice: true},
	{Path: "store/types/set_iterator.go", NomsPath: "go/types/set_iterator.go", HadCopyrightNotice: true},
	{Path: "store/types/set_iterator_test.go", NomsPath: "go/types/set_iterator_test.go", HadCopyrightNotice: true},
	{Path: "store/types/set_leaf_sequence.go", NomsPath: "go/types/set_leaf_sequence.go", HadCopyrightNotice: true},
	{Path: "store/types/set_test.go", NomsPath: "go/types/set_test.go", HadCopyrightNotice: true},
	{Path: "store/types/string.go", NomsPath: "go/types/string.go", HadCopyrightNotice: true},
	{Path: "store/types/string_test.go", NomsPath: "go/types/string_test.go", HadCopyrightNotice: true},
	{Path: "store/types/struct.go", NomsPath: "go/types/struct.go", HadCopyrightNotice: true},
	{Path: "store/types/struct_test.go", NomsPath: "go/types/struct_test.go", HadCopyrightNotice: true},
	{Path: "store/types/subtype.go", NomsPath: "go/types/subtype.go", HadCopyrightNotice: true},
	{Path: "store/types/subtype_test.go", NomsPath: "go/types/subtype_test.go", HadCopyrightNotice: true},
	{Path: "store/types/type.go", NomsPath: "go/types/type.go", HadCopyrightNotice: true},
	{Path: "store/types/type_desc.go", NomsPath: "go/types/type_desc.go", HadCopyrightNotice: true},
	{Path: "store/types/type_test.go", NomsPath: "go/types/type_test.go", HadCopyrightNotice: true},
	{Path: "store/types/util_test.go", NomsPath: "go/types/util_test.go", HadCopyrightNotice: true},
	{Path: "store/types/validate_type.go", NomsPath: "go/types/validate_type.go", HadCopyrightNotice: true},
	{Path: "store/types/validating_decoder.go", NomsPath: "go/types/validating_decoder.go", HadCopyrightNotice: true},
	{Path: "store/types/validating_decoder_test.go", NomsPath: "go/types/validating_decoder_test.go", HadCopyrightNotice: true},
	{Path: "store/types/value.go", NomsPath: "go/types/value.go", HadCopyrightNotice: true},
	{Path: "store/types/value_decoder.go", NomsPath: "go/types/value_decoder.go", HadCopyrightNotice: true},
	{Path: "store/types/value_stats.go", NomsPath: "go/types/value_stats.go", HadCopyrightNotice: true},
	{Path: "store/types/value_store.go", NomsPath: "go/types/value_store.go", HadCopyrightNotice: true},
	{Path: "store/types/value_store_test.go", NomsPath: "go/types/value_store_test.go", HadCopyrightNotice: true},
	{Path: "store/types/walk_refs.go", NomsPath: "go/types/walk_refs.go", HadCopyrightNotice: true},
	{Path: "store/types/walk_refs_test.go", NomsPath: "go/types/walk_refs_test.go", HadCopyrightNotice: true},
	{Path: "store/util/clienttest/client_test_suite.go", NomsPath: "go/util/clienttest/client_test_suite.go", HadCopyrightNotice: true},
	{Path: "store/util/datetime/date_time.go", NomsPath: "go/util/datetime/date_time.go", HadCopyrightNotice: true},
	{Path: "store/util/datetime/date_time_test.go", NomsPath: "go/util/datetime/date_time_test.go", HadCopyrightNotice: true},
	{Path: "store/util/exit/exit.go", NomsPath: "go/util/exit/exit.go", HadCopyrightNotice: true},
	{Path: "store/util/functions/all.go", NomsPath: "go/util/functions/all.go", HadCopyrightNotice: true},
	{Path: "store/util/functions/all_test.go", NomsPath: "go/util/functions/all_test.go", HadCopyrightNotice: true},
	{Path: "store/util/outputpager/page_output.go", NomsPath: "go/util/outputpager/page_output.go", HadCopyrightNotice: true},
	{Path: "store/util/profile/profile.go", NomsPath: "go/util/profile/profile.go", HadCopyrightNotice: true},
	{Path: "store/util/progressreader/reader.go", NomsPath: "go/util/progressreader/reader.go", HadCopyrightNotice: true},
	{Path: "store/util/sizecache/size_cache.go", NomsPath: "go/util/sizecache/size_cache.go", HadCopyrightNotice: true},
	{Path: "store/util/sizecache/size_cache_test.go", NomsPath: "go/util/sizecache/size_cache_test.go", HadCopyrightNotice: true},
	{Path: "store/util/status/status.go", NomsPath: "go/util/status/status.go", HadCopyrightNotice: true},
	{Path: "store/util/test/equals_ignore_hashes.go", NomsPath: "go/util/test/equals_ignore_hashes.go", HadCopyrightNotice: true},
	{Path: "store/util/verbose/verbose.go", NomsPath: "go/util/verbose/verbose.go", HadCopyrightNotice: true},
	{Path: "store/util/writers/max_line_writer.go", NomsPath: "go/util/writers/max_line_writer.go", HadCopyrightNotice: true},
	{Path: "store/util/writers/prefix_writer.go", NomsPath: "go/util/writers/prefix_writer.go", HadCopyrightNotice: true},
	{Path: "store/util/writers/writers_test.go", NomsPath: "go/util/writers/writers_test.go", HadCopyrightNotice: true},
}

// Attempt to enforce some simple copyright header standards on some
// directories in the repository.
// The rules:
// - Every .go file in //go that is not a .pb.go file and that did not
//   come from noms must have ExpectedHeader.
// - Every .go file in //go that came from noms and whose file in noms
//   had a copyright notice must have ExpectedHeaderForFileFromNoms.
// - Every .proto file in //proto must have ExpectedHeader.

func main() {
	failed := CheckGo()
	failed = CheckProto() || failed
	if failed {
		os.Exit(1)
	}
}

func CheckGo() bool {
	nomsLookup := make(map[string]*CopiedNomsFile)
	for i := range CopiedNomsFiles {
		nomsLookup[CopiedNomsFiles[i].Path] = &CopiedNomsFiles[i]
	}
	var failed bool
	filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, ".pb.go") {
			info := nomsLookup[path]
			hasNomsHeader := info != nil && info.HadCopyrightNotice
			if info != nil {
				delete(nomsLookup, path)
			}
			f, err := os.Open(path)
			if err != nil {
				panic(err)
			}
			defer f.Close()
			bs, err := io.ReadAll(f)
			if err != nil {
				panic(err)
			}
			var passes bool
			if hasNomsHeader {
				passes = ExpectedHeaderForFileFromNoms.Match(bs)
			} else {
				passes = ExpectedHeader.Match(bs)
			}
			if !passes {
				fmt.Printf("ERROR: Wrong copyright header: %v\n", path)
				failed = true
			}
		}
		return nil
	})
	for path := range nomsLookup {
		fmt.Printf("ERROR: Missing noms file from CopiedNomsFiles: %v\n", path)
		fmt.Printf("  Please update with new location or remove the reference in ./utils/copyrightshdrs/")
		failed = true
	}
	return failed
}

func CheckProto() bool {
	var failed bool
	filepath.Walk("../proto", func(path string, info os.FileInfo, err error) error {
		if strings.HasSuffix(path, ".proto") {
			f, err := os.Open(path)
			if err != nil {
				panic(err)
			}
			defer f.Close()
			bs, err := io.ReadAll(f)
			if err != nil {
				panic(err)
			}
			passes := ExpectedHeader.Match(bs)
			if !passes {
				fmt.Printf("ERROR: Wrong copyright header: %v\n", path)
				failed = true
			}
		} else if strings.HasPrefix(path, "../proto/third_party") {
			return filepath.SkipDir
		}
		return nil
	})
	return failed
}

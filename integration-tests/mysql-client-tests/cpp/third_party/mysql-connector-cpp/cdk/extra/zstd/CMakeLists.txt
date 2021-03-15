# ################################################################
# Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under both the BSD-style license (found in the
# LICENSE file in the root directory of this source tree) and the GPLv2 (found
# in the COPYING file in the root directory of this source tree).
# ################################################################

PROJECT(zstd)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8.9)

SET(ZSTD_LIB_DIR
  "${PROJECT_SOURCE_DIR}/lib")
INCLUDE_DIRECTORIES(
  ${ZSTD_LIB_DIR}
  ${ZSTD_LIB_DIR}/common
)

SET(ZSTD_SRCS
  lib/common/debug.c
  lib/common/entropy_common.c
  lib/common/error_private.c
  lib/common/fse_decompress.c
  lib/common/pool.c
  lib/common/threading.c
  lib/common/xxhash.c
  lib/common/zstd_common.c
  lib/compress/fse_compress.c
  lib/compress/hist.c
  lib/compress/huf_compress.c
  lib/compress/zstd_compress.c
  lib/compress/zstd_compress_literals.c
  lib/compress/zstd_compress_sequences.c
  lib/compress/zstd_double_fast.c
  lib/compress/zstd_fast.c
  lib/compress/zstd_lazy.c
  lib/compress/zstd_ldm.c
  lib/compress/zstdmt_compress.c
  lib/compress/zstd_opt.c
  lib/decompress/huf_decompress.c
  lib/decompress/zstd_ddict.c
  lib/decompress/zstd_decompress_block.c
  lib/decompress/zstd_decompress.c
  lib/deprecated/zbuff_common.c
  lib/deprecated/zbuff_compress.c
  lib/deprecated/zbuff_decompress.c
  lib/dictBuilder/cover.c
  lib/dictBuilder/divsufsort.c
  lib/dictBuilder/fastcover.c
  lib/dictBuilder/zdict.c
)

ADD_LIBRARY(zstd STATIC ${ZSTD_SRCS})
set_target_properties(zstd PROPERTIES FOLDER "Misc")

#-----------------------------------------------------------------------------
# Add extra compilation flags
#-----------------------------------------------------------------------------
#INCLUDE(AddZstdCompilationFlags)
#ADD_ZSTD_COMPILATION_FLAGS()

# Always hide XXHash symbols
ADD_DEFINITIONS(-DXXH_NAMESPACE=ZSTD_)

#-----------------------------------------------------------------------------
# Options
#-----------------------------------------------------------------------------
OPTION(ZSTD_LEGACY_SUPPORT "LEGACY SUPPORT" OFF)
OPTION(ZSTD_MULTITHREAD_SUPPORT "MULTITHREADING SUPPORT" OFF)
OPTION(ZSTD_BUILD_PROGRAMS "BUILD PROGRAMS" ON)
OPTION(ZSTD_BUILD_CONTRIB "BUILD CONTRIB" OFF)
OPTION(ZSTD_BUILD_TESTS "BUILD TESTS" OFF)
if (MSVC)
    OPTION(ZSTD_USE_STATIC_RUNTIME "LINK TO STATIC RUN-TIME LIBRARIES" OFF)
endif ()

IF (ZSTD_LEGACY_SUPPORT)
    MESSAGE(STATUS "ZSTD_LEGACY_SUPPORT defined!")
    ADD_DEFINITIONS(-DZSTD_LEGACY_SUPPORT=4)
ELSE (ZSTD_LEGACY_SUPPORT)
    MESSAGE(STATUS "ZSTD_LEGACY_SUPPORT not defined!")
    ADD_DEFINITIONS(-DZSTD_LEGACY_SUPPORT=0)
ENDIF (ZSTD_LEGACY_SUPPORT)

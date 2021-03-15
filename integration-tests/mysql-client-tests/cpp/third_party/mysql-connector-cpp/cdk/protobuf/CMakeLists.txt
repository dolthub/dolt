# Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have included with MySQL.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

# The files in protobuf-3.6.1/ are almost unmodified versions of google
# source files taken from protobuf-cpp-3.6.1.tar.gz archive.
# Changes:
# - remove directories and files
#    protobuf-3.6.1/benchmarks/
#    protobuf-3.6.1/conformance/
#    protobuf-3.6.1/editors/
#    protobuf-3.6.1/examples/
#    protobuf-3.6.1/objectivec/
#    protobuf-3.6.1/third_party/
#    protobuf-3.6.1/python/
#    protobuf-3.6.1/util/
#    protobuf-3.6.1/ar-lib
#    protobuf-3.6.1/test-driver
# - disable configuration check of cmake's mimimum version
# - disable configuration zlib check
# - disable installation script
# - fix minor compilation issues on SunPro 5.14.0
# - fix minor issues to satisfy UBSAN


cmake_minimum_required(VERSION 2.8)
PROJECT(Protobuf)

include(../cmake/setup.cmake)
include(platform)

#
# Create empty config.h as Protobuf code includes it.
# TODO: Put some meaningful configuration there.
#

file(WRITE ${PROJECT_BINARY_DIR}/config.h "\n")

#
# Produce position independent code.
# Note: This is crucial on Solaris
#

enable_pic()
enable_cxx11()


if(SUNPRO)
  set_arch_m64()
endif()

#
# Do not export any symbols
#
# Note: The LIBPROTOBUF_EXPORTS macro should *not* be defined
#

set_visibility(hidden)


#
# Solaris compile and link settings
#

if(SUNPRO)

  add_definitions(-DSOLARIS_64BIT_ENABLED)

  if(SPARC)
    add_definitions(-DGOOGLE_PROTOBUF_ARCH_SPARC)
  endif()

endif()


#
# Configure static runtime library on Windows if requested
#

OPTION(STATIC_MSVCRT "Use static MSVC runtime library" OFF)

IF(WIN32 AND STATIC_MSVCRT)

message("Using static runtime library")
set_msvcrt(static)

ENDIF()


#
# Dependency on threads library.
#
# TODO: Use DepFind* instead?
#

INCLUDE(FindThreads)
IF(CMAKE_USE_PTHREADS_INIT)
  message("Using pthreads for protobuf code")
  ADD_DEFINITIONS(-DHAVE_PTHREAD)
ENDIF()


#
# Skip compression support for now.
#

set(HAVE_ZLIB 0)
ADD_DEFINITIONS(-DHAVE_ZLIB=${HAVE_ZLIB})


#
# Turn off some warning flags when compiling protobuf.
#


if(GCC OR CLANG)

  add_compile_options(
    -Wno-sign-compare
    -Wno-ignored-qualifiers
    -Wno-return-type
    -Wno-unused-function
    -Wno-unused-local-typedefs
    -Wno-unused-local-typedefs
  )

endif()

if(GCC)

  add_compile_options(
    -Wno-maybe-uninitialized
    -Wno-unused-but-set-parameter
  )

endif()

if(APPLE)

  # Our version of protobuf uses many constructs (mainly atomic ops)
  # which were depracated in OSX 10.12.
  # TODO: Upgrade bundled protobuf

  add_compile_options(-Wno-deprecated-declarations)

endif()

if(WIN32)

  add_compile_options(
    /wd4018
    /wd4996
    /wd4244
    /wd4715
    /wd4065
    /wd4800
    /wd4355
    /wd4267
    /wd4506
    /wd4146
  )

endif()

if(SUNPRO)

  # Option -erroff disables only warnings, actually. This option requires
  # a warning tag, which is printed if -errtags option is enabled.

  add_compile_options(
    -errtags=yes
    -erroff=anonnotype
  )

endif()


#
# Define Protobuf targets.
#


SET(PROTO_SRC_DIR
  "${PROJECT_SOURCE_DIR}/protobuf-3.6.1/src")


file(WRITE "${PROJECT_BINARY_DIR}/exports.cmake"
  "set(PROTOBUF_INCLUDE_DIR \"${PROTO_SRC_DIR}\")\n\n"
)

macro(pb_export)
  export(TARGETS ${ARGV} NAMESPACE pb_
         APPEND FILE "${PROJECT_BINARY_DIR}/exports.cmake")
endmacro()

INCLUDE_DIRECTORIES(
  ${PROJECT_BINARY_DIR}
  ${PROTO_SRC_DIR}
  ${ZLIB_INCLUDE_DIR}
)


SET(LIBPROTOBUF_LITE_SOURCES
  ${PROTO_SRC_DIR}/google/protobuf/stubs/common.cc
  ${PROTO_SRC_DIR}/google/protobuf/stubs/hash.h
  ${PROTO_SRC_DIR}/google/protobuf/stubs/int128.cc
  ${PROTO_SRC_DIR}/google/protobuf/stubs/io_win32.cc
  ${PROTO_SRC_DIR}/google/protobuf/stubs/map_util.h
  ${PROTO_SRC_DIR}/google/protobuf/stubs/status.cc
  ${PROTO_SRC_DIR}/google/protobuf/stubs/stringpiece.cc
  ${PROTO_SRC_DIR}/google/protobuf/stubs/stringprintf.cc
  ${PROTO_SRC_DIR}/google/protobuf/stubs/stringprintf.h
  ${PROTO_SRC_DIR}/google/protobuf/stubs/structurally_valid.cc
  ${PROTO_SRC_DIR}/google/protobuf/arena.cc
  ${PROTO_SRC_DIR}/google/protobuf/extension_set.cc
  ${PROTO_SRC_DIR}/google/protobuf/generated_message_util.cc
  ${PROTO_SRC_DIR}/google/protobuf/message_lite.cc
  ${PROTO_SRC_DIR}/google/protobuf/repeated_field.cc
  ${PROTO_SRC_DIR}/google/protobuf/wire_format_lite.cc
  ${PROTO_SRC_DIR}/google/protobuf/io/coded_stream.cc
  ${PROTO_SRC_DIR}/google/protobuf/io/coded_stream_inl.h
  ${PROTO_SRC_DIR}/google/protobuf/io/zero_copy_stream.cc
  ${PROTO_SRC_DIR}/google/protobuf/io/zero_copy_stream_impl_lite.cc
  ${PROTO_SRC_DIR}/google/protobuf/implicit_weak_message.cc
)

ADD_LIBRARY(protobuf-lite STATIC ${LIBPROTOBUF_LITE_SOURCES})
SET_PROPERTY(TARGET protobuf-lite PROPERTY EXCLUDE_FROM_ALL TRUE)
pb_export(protobuf-lite)

SET(LIBPROTOBUF_SOURCES
  ${PROTO_SRC_DIR}/google/protobuf/any.cc

  ${PROTO_SRC_DIR}/google/protobuf/stubs/strutil.cc
  ${PROTO_SRC_DIR}/google/protobuf/stubs/strutil.h
  ${PROTO_SRC_DIR}/google/protobuf/stubs/substitute.cc
  ${PROTO_SRC_DIR}/google/protobuf/stubs/substitute.h

  ${PROTO_SRC_DIR}/google/protobuf/stubs/structurally_valid.cc
  ${PROTO_SRC_DIR}/google/protobuf/descriptor.cc
  ${PROTO_SRC_DIR}/google/protobuf/descriptor.pb.cc
  ${PROTO_SRC_DIR}/google/protobuf/descriptor_database.cc
  ${PROTO_SRC_DIR}/google/protobuf/dynamic_message.cc
  ${PROTO_SRC_DIR}/google/protobuf/extension_set_heavy.cc
  ${PROTO_SRC_DIR}/google/protobuf/generated_message_reflection.cc
  ${PROTO_SRC_DIR}/google/protobuf/map_field.cc
  ${PROTO_SRC_DIR}/google/protobuf/message.cc
  ${PROTO_SRC_DIR}/google/protobuf/reflection_ops.cc
  ${PROTO_SRC_DIR}/google/protobuf/service.cc
  ${PROTO_SRC_DIR}/google/protobuf/text_format.cc
  ${PROTO_SRC_DIR}/google/protobuf/unknown_field_set.cc
  ${PROTO_SRC_DIR}/google/protobuf/wire_format.cc
  ${PROTO_SRC_DIR}/google/protobuf/io/printer.cc
  ${PROTO_SRC_DIR}/google/protobuf/io/strtod.cc
  ${PROTO_SRC_DIR}/google/protobuf/io/tokenizer.cc
  ${PROTO_SRC_DIR}/google/protobuf/io/zero_copy_stream_impl.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_map_field.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_padding_optimizer.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/importer.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/parser.cc
)

if(HAVE_ZLIB)
  LIST(APPEND LIBPROTOBUF_SOURCES
       ${PROTO_SRC_DIR}/google/protobuf/io/gzip_stream.cc)
endif()

ADD_LIBRARY(protobuf STATIC ${LIBPROTOBUF_SOURCES})
TARGET_LINK_LIBRARIES(protobuf protobuf-lite
  ${CMAKE_THREAD_LIBS_INIT} ${ZLIB_LIBRARY} ${LIBRT})

pb_export(protobuf)

SET(LIBPROTOC_SOURCES
  ${PROTO_SRC_DIR}/google/protobuf/compiler/code_generator.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/command_line_interface.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/plugin.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/plugin.pb.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/subprocess.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/subprocess.h
  ${PROTO_SRC_DIR}/google/protobuf/compiler/zip_writer.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/zip_writer.h
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_enum.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_enum.h
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_enum_field.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_enum_field.h
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_extension.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_extension.h
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_field.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_field.h
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_file.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_file.h
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_generator.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_helpers.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_helpers.h
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_message.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_message.h
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_message_field.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_message_field.h
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_options.h
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_primitive_field.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_primitive_field.h
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_service.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_service.h
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_string_field.cc
  ${PROTO_SRC_DIR}/google/protobuf/compiler/cpp/cpp_string_field.h
  ${PROTO_SRC_DIR}/google/protobuf/stubs/io_win32.cc
)

ADD_LIBRARY(protoclib STATIC ${LIBPROTOC_SOURCES})
TARGET_LINK_LIBRARIES(protoclib protobuf)

ADD_EXECUTABLE(protoc ${PROTO_SRC_DIR}/google/protobuf/compiler/main.cc)
TARGET_COMPILE_DEFINITIONS(protoc PRIVATE OPENSOURCE_PROTOBUF_CPP_BOOTSTRAP)
TARGET_LINK_LIBRARIES(protoc protoclib)

pb_export(protoc)

# Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0, as
# published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an
# additional permission to link the program and your derivative works
# with the separately licensed software that they have included with
# MySQL.
#
# Without limiting anything contained in the foregoing, this file,
# which is part of MySQL Connector/C++, is also subject to the
# Universal FOSS Exception, version 1.0, a copy of which can be found at
# http://oss.oracle.com/licenses/universal-foss-exception.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA


include(CheckIncludeFile)

check_include_file(sys/endian.h HAVE_ENDIAN_H)
add_config(HAVE_ENDIAN_H)

check_include_file(sys/byteorder.h HAVE_BYTEORDER_H)  # on Solaris
add_config(HAVE_BYTEORDER_H)

add_config_option(DEBUG_PROTOBUF BOOL ADVANCED DEFAULT OFF "Debug Protobuf messages")

set(use_full_protobuf ${WITH_TESTS})

if (DEBUG_PROTOBUF)
  message("Protobuf debugging enabled")
  add_definitions(-DDEBUG_PROTOBUF)
  set(use_full_protobuf 1)
endif(DEBUG_PROTOBUF)


SET(PROTOCOL pb)

SET(proto_mysqlx_defs
  ${PROTOCOL}/mysqlx_connection.proto
  ${PROTOCOL}/mysqlx_crud.proto
  ${PROTOCOL}/mysqlx_cursor.proto
  ${PROTOCOL}/mysqlx_datatypes.proto
  ${PROTOCOL}/mysqlx_expect.proto
  ${PROTOCOL}/mysqlx_expr.proto
  ${PROTOCOL}/mysqlx_notice.proto
  ${PROTOCOL}/mysqlx_prepare.proto
  ${PROTOCOL}/mysqlx_resultset.proto
  ${PROTOCOL}/mysqlx_session.proto
  ${PROTOCOL}/mysqlx_sql.proto
  ${PROTOCOL}/mysqlx.proto
)

if(NOT use_full_protobuf)

  #
  # The custom command below invokes make_lite_pb.cmake script which modifies
  # our pb definitions for use with protobuf-lite. Modified definitions are
  # stored in ${LITE} location.
  #

  set(LITE "${CMAKE_CURRENT_BINARY_DIR}/lite")
  file(MAKE_DIRECTORY "${LITE}")

  add_custom_command(OUTPUT prepare_pb_lite
    COMMAND ${CMAKE_COMMAND} -Dsrc="${PROTOCOL}" -Dtgt="${LITE}" -P make_lite_pb.cmake
    COMMAND ${CMAKE_COMMAND} -E touch ${CMAKE_CURRENT_BINARY_DIR}/prepare_pb_lite
    DEPENDS ${proto_mysqlx_defs}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Preparing protobuf files for protobuf-lite"
  )

  #
  # Collect paths to modified pb definition files in ${proto_mysqlx_defs_lite}.
  #

  UNSET(proto_mysqlx_defs_lite)

  foreach(proto ${proto_mysqlx_defs})

    get_filename_component(name ${proto} NAME)
    list(APPEND proto_mysqlx_defs_lite "${LITE}/${name}")

    # This custom command causes lite definitions to be re-generated if they
    # do not exist or become outdated.

    add_custom_command(
      OUTPUT "${LITE}/${name}"
	  COMMAND ${CMAKE_COMMAND} -E touch "${LITE}/${name}"
      DEPENDS prepare_pb_lite
      COMMENT ""
    )

  endforeach()

  set(proto_mysqlx_defs ${proto_mysqlx_defs_lite})

endif(NOT use_full_protobuf)


mysqlx_protobuf_generate_cpp(PB_SRCS PB_HDRS ${proto_mysqlx_defs})

file(GLOB HEADERS *.h)

ADD_LIBRARY(cdk_proto_mysqlx STATIC
            protocol.cc protocol_compression.cc session.cc rset.cc
            stmt.cc crud.cc
            ${PB_SRCS}
            ${proto_mysqlx_defs}
            ${HEADERS}
)

# For the generated PB headers
target_include_directories(cdk_proto_mysqlx PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}
)

if(use_full_protobuf)
  target_link_libraries(cdk_proto_mysqlx PRIVATE Protobuf::pb-full)
else()
  target_link_libraries(cdk_proto_mysqlx PRIVATE Protobuf::pb-lite)
endif()

target_link_libraries(cdk_proto_mysqlx PRIVATE cdk_foundation zlib lz4 zstd)

ADD_COVERAGE(cdk_proto_mysqlx)

source_group("Protobuf Definitions" FILES ${proto_mysqlx_defs})
source_group("Protobuf Generated" FILES ${PB_SRCS})



#ADD_SUBDIRECTORY(tests)

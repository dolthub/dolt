# Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

ADD_DEFINITIONS(-DPORT=9876)

IF (WITH_TESTS)


if(0)

ADD_EXECUTABLE(foundation_tests_server test_server.cc)
TARGET_LINK_LIBRARIES(foundation_tests_server cdk)
SET_TARGET_PROPERTIES(foundation_tests_server
  PROPERTIES OUTPUT_NAME test_server
)

add_custom_command(TARGET foundation_tests_server
                   POST_BUILD
                   COMMAND cmake -E copy $<TARGET_FILE:foundation_tests_server> "${CMAKE_CURRENT_BINARY_DIR}/test_server${CMAKE_EXECUTABLE_SUFFIX}")


ADD_TEST_ENVIRONMENT("FOUNDATION_TEST_SERVER=${CMAKE_CURRENT_BINARY_DIR}/test_server${CMAKE_EXECUTABLE_SUFFIX}")

endif()


ADD_NG_TEST(foundation-t
  error_t.cc time_t.cc
  opaque_t.cc opaque_t_impl.cc
  stream_t.cc
  # connection_tcpip_t.cc  # this uses test server
  diagnostics_t.cc codec_t.cc
)


ENDIF()

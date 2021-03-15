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


cmake_minimum_required(VERSION 2.8)
# TODO: Why it is not enough to set it in the top-level CMakeLists.txt?
#cmake_policy(SET CMP0023 OLD)

#ADD_SUBDIRECTORY(tests)


SET(cdk_sources
  session.cc
  codec.cc
)

file(GLOB HEADERS *.h)

add_library(cdk STATIC ${cdk_sources} ${HEADERS})

target_link_libraries(cdk
  PUBLIC  cdk_mysqlx cdk_parser
  PRIVATE Protobuf::pb-lite  # required by codecc.cc
)

add_coverage(cdk)

EXPORT(TARGETS cdk
  APPEND FILE ${PROJECT_BINARY_DIR}/exports.cmake
)



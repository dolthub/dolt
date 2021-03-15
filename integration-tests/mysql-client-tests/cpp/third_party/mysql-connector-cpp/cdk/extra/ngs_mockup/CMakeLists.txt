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


CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(NGSMockupServer)

IF(NOT DEFINED WITH_NGS_MOCKUP)
  OPTION(WITH_NGS_MOCKUP "Build NGS mockup server" OFF)
ENDIF()

IF(WITH_NGS_MOCKUP)
MESSAGE("Building NGS mockup server")

# We do not apply strict warning requirements to this code.

if (MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W3")
endif()

ADD_DEFINITIONS(-DDEFAULT_PORT=33060)

ADD_EXECUTABLE(ngs_mockup ngs_mockup.cc)
TARGET_LINK_LIBRARIES(ngs_mockup ${target_proto_mysqlx})

#
# Copy executable to location that is independent from
# build configuration.
#

add_custom_command(TARGET ngs_mockup
                   POST_BUILD
                   COMMAND cmake -E copy $<TARGET_FILE:ngs_mockup> "${CMAKE_CURRENT_BINARY_DIR}/ngs_mockup${CMAKE_EXECUTABLE_SUFFIX}")

ENDIF()

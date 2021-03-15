# Copyright (c) 2020, Oracle and/or its affiliates. All rights reserved.
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

cmake_minimum_required(VERSION 3.0)

#
# This is test cmake project that uses Connector/C++ as a subproject,
# to verify that this usage scenario works.
#

project(Sub_project_test C CXX)

#
# Find Connector/C++ sources.
#

if (NOT DEFINED CONCPP_SRC)
  get_filename_component(CONCPP_SRC ../.. REALPATH)
endif()

message("- using Con/C++ sources at: ${CONCPP_SRC}")

if (NOT EXISTS ${CONCPP_SRC}/CMakeLists.txt)
  message(FATAL_ERROR
    "Could not find CMakeLists.txt at Con/C++ source location."
  )
endif()

#
# If CONCPP_CACHE is defined, it should point at Connector/C++
# build location where a cmake cache is stored. Various settings
# required to build connector source are read from that cache.
#

if (DEFINED CONCPP_CACHE)

  message("- loading settings from Con/C++ cmake cache at: ${CONCPP_CACHE}")
  load_cache(${CONCPP_CACHE} READ_WITH_PREFIX ""
    WITH_SSL
    WITH_JDBC
    WITH_BOOST
    WITH_MYSQL MYSQL_INCLUDE_DIR MYSQL_LIB_DIR MYSQL_CONFIG_EXECUTABLE
  )

  message("- WITH_SSL: ${WITH_SSL}")
  message("- WITH_BOOST: ${WITH_BOOST}")
  message("- WITH_MYSQL: ${WITH_MYSQL} (${MYSQL_INCLUDE_DIR} ${MYSQL_LIB_DIR})")

endif()

#
# Include Connector/C++ as a sub-project
#

add_subdirectory(${CONCPP_SRC} connector-cpp)

#
# Define test applications to verify that they build correctly.
#

set(CMAKE_CXX_STANDARD 11)

add_executable(devapi_test ${CONCPP_SRC}/testapp/devapi_test.cc)
target_link_libraries(devapi_test connector)

add_executable(xapi_test ${CONCPP_SRC}/testapp/xapi_test.c)
target_link_libraries(xapi_test connector)

if (WITH_JDBC)

  add_executable(jdbc_test ${CONCPP_SRC}/testapp/jdbc_test.cc)
  target_link_libraries(jdbc_test connector-jdbc)

endif()

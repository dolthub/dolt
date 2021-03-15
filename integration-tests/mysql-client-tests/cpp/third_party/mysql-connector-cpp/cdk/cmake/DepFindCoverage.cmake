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


if(COMMAND add_coverage)
  return()
endif()

function(add_coverage)
endfunction()

#
# Currently works only with gcc.
#

if(NOT GCC)
  return()
endif()


add_config_option(WITH_COVERAGE BOOL ADVANCED DEFAULT OFF
  "Build with coverage support (debug, gcc only)")

if(WITH_COVERAGE)
  message(STATUS "Setting up coverage (gcov).")
endif()

function(add_coverage target)

  if(NOT WITH_COVERAGE)
    return()
  endif()

  message(STATUS "Enabling gcc coverage support for target: ${target}")
  target_link_libraries(${target} PRIVATE Coverage::enable)

endfunction()


add_library(coverage-enable-if INTERFACE)
add_library(Coverage::enable ALIAS coverage-enable-if)

target_compile_definitions(coverage-enable-if INTERFACE WITH_COVERAGE)
target_compile_options(coverage-enable-if INTERFACE -O0 -fprofile-arcs -ftest-coverage)

# TODO: See if gcov is installed on the system

target_link_libraries(coverage-enable-if INTERFACE gcov)


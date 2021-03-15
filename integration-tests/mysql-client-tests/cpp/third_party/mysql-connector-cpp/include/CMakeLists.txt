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


#
# MySQL Connector/C++ Public Headers
#
# see headers.cmake for explanations how project's public
# headers are declared and processed.
#

include(headers)

#
# Headers require C++11 compiler. Set appropriate option
# for gcc/clang compilers.
#

if(NOT WIN32)
  set(HEADER_CHECK_OPTIONS "-std=c++11")
endif()

setup_headers(${CMAKE_CURRENT_SOURCE_DIR})

add_headers_dir(mysqlx)

if(WITH_JDBC)
  # TODO: enable when JDBC public headers are ready for it
  # add_headers_dir(mysql)
  add_subdirectory(mysql)
endif()

#
# Target to test public headers, if WITH_HEADERS_CHECKS build option is set.
#

add_headers_target()


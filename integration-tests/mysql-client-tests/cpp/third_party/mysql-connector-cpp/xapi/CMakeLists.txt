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


#
# TODO: Fix XAPI code instead of disabling these warnings
#

if(MSVC)

  add_compile_options(
    /wd4244
    /wd4267
    /wd4127
    /wd4996
    /wd4702
  )

endif()


set(_libmysqlx_cc_src
         crud.cc
         result.cc
         mysqlx.cc
         session.cc
)

file(GLOB HEADERS *.h)

add_library(xapi STATIC ${_libmysqlx_cc_src} ${HEADERS})
target_link_libraries(xapi PUBLIC common)
add_coverage(xapi)

add_subdirectory(tests)


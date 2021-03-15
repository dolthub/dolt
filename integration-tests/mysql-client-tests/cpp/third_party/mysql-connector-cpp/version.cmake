# Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
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
# Connector/C++ version
#

set(CONCPP_VERSION_MAJOR  8 CACHE INTERNAL "version info")
set(CONCPP_VERSION_MINOR  0 CACHE INTERNAL "version info")
set(CONCPP_VERSION_MICRO 21 CACHE INTERNAL "version info")
# Level is "-alpha", "-beta", empty if GA
set(CONCPP_VERSION_LEVEL  "" CACHE INTERNAL "version info")

set(CONCPP_VERSION
  "${CONCPP_VERSION_MAJOR}.${CONCPP_VERSION_MINOR}.${CONCPP_VERSION_MICRO}"
  CACHE INTERNAL "version info"
)

#
#  ABI versions
#
# Note: When updating ABI version, a corresponding MYSQLX_ABI_X_Y macro
# needs to be added in include/mysqlx/common/api.h (see comments there).
#

set(ABI_VERSION_MAJOR 2 CACHE INTERNAL "version info")
set(ABI_VERSION_MINOR 0 CACHE INTERNAL "version info")
set(
  ABI_VERSION "${ABI_VERSION_MAJOR}.${ABI_VERSION_MINOR}"
  CACHE INTERNAL "version info"
)

set(JDBC_ABI_VERSION_MAJOR 7 CACHE INTERNAL "version info")
set(JDBC_ABI_VERSION_MINOR 0 CACHE INTERNAL "version info")
set(
  JDBC_ABI_VERSION "${JDBC_ABI_VERSION_MAJOR}.${JDBC_ABI_VERSION_MINOR}"
  CACHE INTERNAL "version info"
)


message(
  "Building version "
  "${CONCPP_VERSION_MAJOR}.${CONCPP_VERSION_MINOR}.${CONCPP_VERSION_MICRO}"
  "${CONCPP_VERSION_LEVEL}"
)

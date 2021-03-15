# Copyright (c) 2018, 2019, Oracle and/or its affiliates. All rights reserved.
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
# Note: Seems not needed any more, but leaving it just in case
#

IF(CMAKE_SYSTEM_NAME MATCHES "Windows")
  STRING(REGEX MATCH "[0-9]+\\.[0-9]+\\.[0-9]+" SYSTEM_VERSION "${CMAKE_SYSTEM_VERSION}")
  SET(SYSTEM_PLATFORM "${CMAKE_SYSTEM_NAME}-${SYSTEM_VERSION}")
ELSEIF(CMAKE_SYSTEM_NAME MATCHES "SunOS")
  # SunOS 5.10=> solaris10
  STRING(REPLACE "5." "" SYSTEM_VERSION "${CMAKE_SYSTEM_VERSION}")
  SET(SYSTEM_PLATFORM "solaris-${SYSTEM_VERSION}")
ELSEIF(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  STRING(REGEX MATCH "[0-9]+\\.[0-9]+"  SYSTEM_VERSION "${CMAKE_SYSTEM_VERSION}")
  SET(SYSTEM_PLATFORM "${CMAKE_SYSTEM_NAME}-${SYSTEM_VERSION}")
ELSEIF(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  IF(CMAKE_OSX_DEPLOYMENT_TARGET)
    SET(SYSTEM_PLATFORM "osx-${CMAKE_OSX_DEPLOYMENT_TARGET}")
  ELSE()
    STRING(REGEX REPLACE "([0-9]+)\\.[0-9]+\\.[0-9]+" "\\1" SYSTEM_VERSION "${CMAKE_SYSTEM_VERSION}")
    # Subtract 4 from Darwin version to get correct osx10.X
    MATH(EXPR SYSTEM_VERSION  "${SYSTEM_VERSION} -4")
    SET(SYSTEM_PLATFORM "macOS-10.${SYSTEM_VERSION}")
  ENDIF()
ELSE()
  STRING(REGEX MATCH "[0-9]+\\.[0-9]+\\.[0-9]+" SYSTEM_VERSION "${CMAKE_SYSTEM_VERSION}")
  SET(SYSTEM_PLATFORM "${CMAKE_SYSTEM_NAME}-${SYSTEM_VERSION}")
ENDIF()

if(EXTRA_NAME_SUFFIX STREQUAL "-commercial")
  SET(CONCPP_LICENSE "COMMERCIAL")
else()
  SET(CONCPP_LICENSE "GPL-2.0")
endif()

#CONFIGURE_FILE(config.h.in   ${CMAKE_BINARY_DIR}/include/config.h)
#INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR}/include)


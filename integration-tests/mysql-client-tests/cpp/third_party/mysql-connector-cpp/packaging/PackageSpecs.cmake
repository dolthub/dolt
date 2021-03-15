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
# Specifications for Connector/C++ binary and source packages
#
# Note: CPACK_XXX variables must be set before include(CPack)
#

set(CONCPP_PACKAGE_BASE_VERSION
    "${CONCPP_VERSION_MAJOR}.${CONCPP_VERSION_MINOR}")
set(CONCPP_PACKAGE_NUMERIC_VERSION
    "${CONCPP_PACKAGE_BASE_VERSION}.${CONCPP_VERSION_MICRO}")
set(CONCPP_PACKAGE_VERSION
    "${CONCPP_PACKAGE_NUMERIC_VERSION}${CONCPP_VERSION_LEVEL}")

# ======================================================================
# Set some initial CPack variables
# ======================================================================

set(CPACK_PACKAGE_NAME    "mysql-connector-c++")
set(CPACK_PACKAGE_VERSION "${CONCPP_PACKAGE_VERSION}")
set(CPACK_PACKAGE_VENDOR  "Oracle Corporation")
set(CPACK_PACKAGE_CONTACT "MySQL Release Engineering <mysql-build@oss.oracle.com>")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY
    "MySQL Connector/C++, a C++ connector library")

# ======================================================================
# Set the default CPack generator
# ======================================================================

if(WIN32)
  set(CPACK_GENERATOR ZIP)
  set(CPACK_SOURCE_GENERATOR ZIP)
else()
  set(CPACK_GENERATOR TGZ)
  set(CPACK_SOURCE_GENERATOR TGZ)
endif()

# ======================================================================
# Set the platform name, if not set from -DPLATFORM_NAME=...
# ======================================================================
#
# TODO: Cover Windows and other architectures we suport
#
# TODO: Decide on proper tagging for Windows: VS version, static/dynamic
# runtime, debug/non-debug etc. Note: some of these differences can/should
# be covered by having several variants of the library added to the package.

if(PLATFORM_NAME)

  # Override with our own name
  set(CPACK_SYSTEM_NAME "${PLATFORM_NAME}")

elseif(WIN32)

  if(IS64BIT)
    set(CPACK_SYSTEM_NAME "winx64")
  else()
    set(CPACK_SYSTEM_NAME "win32")
  endif()

elseif(APPLE)

  if(NOT DEFINED ENV{MACOSX_DEPLOYMENT_TARGET})
    message(FATAL_ERROR "To create packages on OSX, set deployment target"
            " using MACOSX_DEPLOYMENT_TARGET environment variable")
  endif()

  set(osx_version $ENV{MACOSX_DEPLOYMENT_TARGET})
  set(CPACK_SYSTEM_NAME "osx${osx_version}-${CMAKE_SYSTEM_PROCESSOR}")

elseif(NOT CPACK_SYSTEM_NAME)

  # If for some reason not set by CMake
  if(CMAKE_SYSTEM_NAME MATCHES "Linux")
    set(CPACK_SYSTEM_NAME "linux-${CMAKE_SYSTEM_PROCESSOR}")
  else()
    message(FATAL_ERROR "Can't deternine how to set the platform name")
  endif()

endif()

IF(EXTRA_NAME_SUFFIX)
  SET(CPACK_PACKAGE_NAME "mysql-connector-c++${EXTRA_NAME_SUFFIX}")
ELSE(EXTRA_NAME_SUFFIX)
  SET(CPACK_PACKAGE_NAME "mysql-connector-c++")
ENDIF(EXTRA_NAME_SUFFIX)

set(CPACK_PACKAGE_INSTALL_DIRECTORY "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_SYSTEM_NAME}")
set(CPACK_TOPLEVEL_TAG              "${CPACK_PACKAGE_INSTALL_DIRECTORY}")
set(CPACK_PACKAGE_FILE_NAME         "${CPACK_PACKAGE_INSTALL_DIRECTORY}")
if(CMAKE_BUILD_TYPE STREQUAL Debug)
  set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_INSTALL_DIRECTORY}-debug")
endif()

message("Binary package name: ${CPACK_PACKAGE_FILE_NAME}")


# ======================================================================
# Specs for source package
# ======================================================================

set(CPACK_SOURCE_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-src")

message("Source package name: ${CPACK_SOURCE_PACKAGE_FILE_NAME}")

# note: Using regex patterns for CPACK_SOURCE_IGNORE_FILES is fragile because
# they are matched against the full path which can vary depending on where the
# build takes place. Unfortunatelly, I (Rafal) could not find any other mechanism
# for specifying what source files should be excluded from the source package.
#
# note: Double escaping required to get correct pattern string (with single
# escapes) in CPackSourceConfig.cmake

list(APPEND CPACK_SOURCE_IGNORE_FILES "\\\\.git.*")
list(APPEND CPACK_SOURCE_IGNORE_FILES "/jenkins/")
list(APPEND CPACK_SOURCE_IGNORE_FILES "CTestConfig.cmake")



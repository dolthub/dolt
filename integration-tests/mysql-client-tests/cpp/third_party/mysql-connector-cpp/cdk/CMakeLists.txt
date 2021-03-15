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



CMAKE_MINIMUM_REQUIRED(VERSION 3.0)

cmake_policy(VERSION 3.0)

if(POLICY CMP0022)
  cmake_policy(SET CMP0022 NEW)  # consistently use INTERFACE_LINK_LIBRARIES property
endif()

if(POLICY CMP0028)
  cmake_policy(SET CMP0028 NEW)  #23 OLD)
endif()


PROJECT(MySQLCDK)

#
# Load cmake modules.
#

include(cmake/setup.cmake)

include(platform)
include(config_header)
include(config_options)
include(dependency)


#
# Detect if we are configured as stand-alone project, or sub-project.
#

if(PROJECT_NAME STREQUAL CMAKE_PROJECT_NAME)

  MESSAGE("Stand-alone configuration")
  MESSAGE("Building on system: ${CMAKE_SYSTEM}")
  SET(cdk_stand_alone 1)

else()

  MESSAGE("Configuring CDK as part of ${CMAKE_PROJECT_NAME} project")
  SET(cdk_stand_alone 0)
  set(WITH_TESTS OFF)
  set(WITH_DOC OFF)
  set(WITH_HEADER_CHECKS OFF)

endif()


#
# Basic compiler settings.
#

enable_pic()
enable_cxx11()
# ???      -mt

set(HAVE_MOVE_SEMANTICS 1)
add_config(HAVE_MOVE_SEMANTICS)

add_config(CDK_BIG_ENDIAN ${BIG_ENDIAN})

#
#  Dependencies
#

find_dependency(SSL)
find_dependency(Protobuf)
find_dependency(RapidJSON)
find_dependency(Coverage)


# TODO: These macros should not be used in public headers because they are
# specific to the particular build of the connector library and might not be
# correct when used in a different environment.

add_config(WITH_SSL)

#
# Make sure WIN32 is defined on Windows
#

if(WIN32)
  add_definitions(-DWIN32)
  add_definitions(-D_CRT_SECURE_NO_WARNINGS -D_SCL_SECURE_NO_WARNINGS)
endif()

#
# Configure static runtime library on Windows if requested
#

if(cdk_stand_alone)

  add_config_option(STATIC_MSVCRT BOOL ADVANCED DEFAULT OFF "Use static MSVC runtime library")

  if(WIN32 AND STATIC_MSVCRT)
    message("Using static runtime library")
    set_msvcrt(STATIC)
  endif()

endif()

#
# Handling of THROW() statements.
#

add_config_option(THROW_AS_ASSERT BOOL ADVANCED DEFAULT ON
 "Turn THROW statements in the code into assertions for easier debugging"
)

if(THROW_AS_ASSERT)
  add_definitions(-DTHROW_AS_ASSERT)
endif()


#
# Set higher warning level
#

IF(MSVC)

  # TODO: move to /Wall when code base is ready for this

  add_compile_options(/W4)

  # Note: We disable warnings related to C++11 language because we want this
  # to be pure C++ code.
  #
  # 5026 = move ctor was implicitly deleted
  # 5027 = move assignment was implicitly deleted

  if (MSVC_VERSION GREATER 1800)
    add_compile_options(/wd5026 /wd5027)
  endif()

  # This is warning that has been fixed in later versions of MSVC:
  #
  # 4640 = construction of local static object is not thread-safe

  if (MSVC_VERSION LESS 1900)
    add_compile_options(/wd4640)
  endif()

  #
  # Warnings that used to be there in older versions of MSVC but we don't see
  # them any more.
  #
  # Note: 4512 is disabled because according to C++11 standard the situations
  # that triggers this warning should be handled automatically by the compiler
  # (and this is the case for MSVC 2015).
  # See: http://en.cppreference.com/w/cpp/language/copy_assignment
  #
  # Note: 4711 is only informative (see https://msdn.microsoft.com/en-us/library/k402bt7y.aspx)
  #
  # Note (*): We use this a lot when inheriting from foundation::nocopy - this
  # is our C++ way to disable copy semantics.
  #
  # Note: 4520 only present on Visual Studio 12

  if (MSVC_VERSION LESS 1800)
    add_compile_options(
      /wd4127 # conditional expression is constant (needed for do {...} while(false))
      /wd4512 # assignment operator could not be generated
      /wd4100 # unreferenced formal parameter
      /wd4820 # byte padding added
      /wd4571 # SEH exceptions not caught by catch(...)
      /wd4710 # function not inlined
      /wd4514 # unreferenced inline function was removed
      /wd4464 # relative incl. path contains '..'
      /wd4625 # copy ctor was implicitly deleted (*)
      /wd4626 # copy assignment was implicitly deleted (*)
      /wd4711 # function 'function' selected for inline expansion
      /wd4520 # The class has multiple default constructors.
              # The first constructor is used.
    )
  endif()

ELSE()

  if(SUNPRO)
    # TODO: Enable -Wall after fixing warnings
    add_compile_options(-errtags=yes -erroff=hidevf)
  else()
    add_compile_options(-Wall)
  endif()

ENDIF()


if(0)
  message("flags: ${CMAKE_C_FLAGS}")
  message("c++ flags: ${CMAKE_CXX_FLAGS}")
  foreach(TYPE DEBUG RELEASE RELWITHDEBINFO MINSIZEREL)
    message("${TYPE} flags: ${CMAKE_C_FLAGS_${TYPE}}")
    message("c++ ${TYPE} flags: ${CMAKE_CXX_FLAGS_${TYPE}}")
  endforeach()
endif()



#
# Clean exports file
#

file(REMOVE ${PROJECT_BINARY_DIR}/exports.cmake)

#
# Project's public headers
#

ADD_SUBDIRECTORY(include)
INCLUDE_DIRECTORIES(include)

#
# Add the binary directory to include path so that generated
# headers like config.h are found
#
INCLUDE_DIRECTORIES(${PROJECT_BINARY_DIR}/include)
set(configuration_header "${PROJECT_BINARY_DIR}/include/mysql/cdk/config.h")


#
# Testing framework
#

#INCLUDE(testing)
#SETUP_TESTING()
ADD_SUBDIRECTORY(extra)


#
# Parser library
#

ADD_SUBDIRECTORY(parser)

#
# CDK components
#

ADD_SUBDIRECTORY(foundation)
ADD_SUBDIRECTORY(protocol)
ADD_SUBDIRECTORY(mysqlx)
ADD_SUBDIRECTORY(core)

#
# Write configuration with detected settings.
#

WRITE_CONFIG_HEADER(${configuration_header})

#
# Tests
#

IF(0) #WITH_TESTS)

  add_test_includes(${PROJECT_SOURCE_DIR}/mysqlx/tests)

  # Target (run_unit_tests) for running all tests declared with ADD_NG_TESTS()
  # (see cmake/testing.cmake)

  ADD_TEST_TARGET()

  # Checks for public headers declared with ADD_HEADERS() (see cmake/headers.cmake)

  ADD_HEADERS_TEST()

ENDIF()

#
# Sample code to try things out
#

IF(cdk_stand_alone)
  ADD_EXECUTABLE(try try.cc)
  TARGET_LINK_LIBRARIES(try cdk)
  #ADD_COVERAGE(try)
ENDIF()


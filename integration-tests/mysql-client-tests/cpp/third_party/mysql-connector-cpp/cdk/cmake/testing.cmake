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
# Infrastructure for defining unit tests for the project
# ======================================================
#
# We use gtest framework. Add unit tests using:
#
#   ADD_NG_TEST(<name> <sources>)
#
# This creates an object library target with given name which compiles
# sources for this test and which will become part of run_unit_tests
# executable (see below).
#
# If unit tests require a library, add it using:
#
#   ADD_TEST_LIBRARIES(<list of libraries>)
#
# Additional include paths required by all unit tests should be specified with:
#
#   ADD_TEST_INCLUDES(<list of include paths>)
#
# Macro ADD_TEST_TARGET() should be called after all unit tests are registered
# with above macros. It generates run_unit_tests target which will run all the
# tests and update_test_groups target which will generate file with ctest
# definitions used to integrate with ctest framework.
#

IF(NOT DEFINED WITH_TESTS)
  OPTION(WITH_TESTS "Build project's unit tests" OFF)
ENDIF()

IF(NOT DEFINED WITH_COVERAGE)
  OPTION(WITH_COVERAGE "Enable coverage support for gcc" OFF)
ENDIF()


#
# Note: On Windows the runtime type must match the one used by gtest.
#

if(MSVC AND NOT DEFINED STATIC_TESTS_MSVCRT)
 option(STATIC_TESTS_MSVCRT "Compile test code using static runtime" OFF)
endif()


IF(WITH_TESTS)
  IF(WITH_COVERAGE)
    MESSAGE("Building unit tests (with coverage, if supported)")
  ELSE()
    MESSAGE("Building unit tests")
  ENDIF()
ENDIF()


MACRO(SETUP_TESTING)
IF(WITH_TESTS)
  INCLUDE(CTest)
  INCLUDE(gtest)
  SETUP_GTEST()
ENDIF()
ENDMACRO(SETUP_TESTING)

#
# Determine location of files accompanying this testing.cmake module which are
# in testing/ sub-folder of the location where the module is located
#

GET_FILENAME_COMPONENT(tests_dir ${CMAKE_CURRENT_LIST_FILE} PATH)
SET(tests_dir "${tests_dir}/testing" CACHE INTERNAL
    "Location of testing.cmake support files" FORCE)

#
# Helper macro which sets global variable (so that its value is visible
# in any cmake file of this project).
#

macro(set_global VAR)

  set(${VAR} ${ARGN} CACHE INTERNAL "global variable" FORCE)
  # so that the cache value is used
  unset(${VAR})

endmacro()

#
# Reset global variables used by testing framework.
#

set_global(test_tests "")
set_global(test_libs  "")
set_global(test_includes "")
set_global(test_environment "")


#
#  Add a unit test.
#
#  This creates a target (object library) with given name which compiles
#  test sources. Test becomes part of run_unit_tests target.
#
#  When compiling test sources, include directories specified with
#  add_test_includes() are in the include path. Additional include directories
#  can be specified for the test target as usual (target_include_directories()).
#
#  Test sources can use libraries specified with add_test_libraries()
#
#
#  Usage:
#   add_ng_test(<test_name> <test_sources>)
#

MACRO(ADD_NG_TEST TEST)
IF(WITH_TESTS)

  list(APPEND test_tests ${TEST})
  set_global(test_tests ${test_tests})

  add_library(${TEST} OBJECT ${ARGN})
  set_target_properties(${TEST} PROPERTIES FOLDER "Tests")

  target_include_directories(${TEST} PRIVATE ${test_includes})

  target_compile_options(${TEST} PRIVATE ${TEST_COMPILE_FLAGS})

  if (MSVC)

    target_compile_definitions(${TEST} PRIVATE
      -D_SCL_SECURE_NO_WARNINGS
      -D_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING
    )

    target_compile_options(${TEST} PRIVATE
      /W3
      /wd4244
      /wd4267
      /wd4701
      /wd4018
      /wd4456  # declaration of hides previous local declaration
    )

    if(STATIC_TESTS_MSVCRT)
      target_compile_options(${TEST} PRIVATE
        $<$<CONFIG:Debug>:/MTd>
        $<$<NOT:$<CONFIG:Debug>>:/MT>
      )
    endif()

  elseif((CMAKE_SYSTEM_NAME MATCHES "SunOS") OR CMAKE_COMPILER_IS_GNUCXX)

    target_compile_options(${TEST} PRIVATE
      -Wno-unused-but-set-variable
      -Wno-maybe-uninitialized
      -Wno-unused-value
    )

  elseif((CMAKE_C_COMPILER_ID MATCHES "Clang") OR (CMAKE_CXX_COMPILER_ID MATCHES "Clang"))

    target_compile_options(${TEST} PRIVATE
      -Wno-unused-value
    )

  else()
    #target_compile_options(${TEST} PRIVATE -Wno-unused-result)
  endif()

  if(CMAKE_CXX_COMPILER_ID MATCHES "SunPro")
    add_definitions(
      -D_POSIX_PTHREAD_SEMANTICS
      -D_REENTRANT
    )
  endif()

  message(STATUS "Added test: ${TEST}")

ENDIF()
ENDMACRO(ADD_NG_TEST)


MACRO(ADD_TEST_LIBRARIES)
IF(WITH_TESTS)

  FOREACH(lib ${ARGN})

    list(APPEND test_libs ${lib})
    set_global(test_libs ${test_libs})
    MESSAGE(STATUS "Added test library: ${lib}")

  ENDFOREACH(lib)

ENDIF()
ENDMACRO(ADD_TEST_LIBRARIES)


MACRO(ADD_TEST_INCLUDES)
IF(WITH_TESTS)

  FOREACH(path ${ARGN})

    list(APPEND test_includes ${path})
    set_global(test_includes ${test_includes})
    MESSAGE(STATUS "Added test include path: ${path}")

  ENDFOREACH(path)

ENDIF()
ENDMACRO(ADD_TEST_INCLUDES)


MACRO(ADD_TEST_ENVIRONMENT)
IF(WITH_TESTS)

  FOREACH(env_var ${ARGN})

    list(APPEND test_environment ${env_var})
    set_global(test_environment ${test_environment})
    MESSAGE(STATUS "Added test env. var: ${env_var}")

  ENDFOREACH(env_var)

ENDIF()
ENDMACRO(ADD_TEST_ENVIRONMENT)


#
# Define run_unit_tests and update_test_groups targets
#

MACRO(ADD_TEST_TARGET)
IF(WITH_TESTS)

  #MESSAGE("Adding run test target for unit tests from: ${test_tests}")
  #MESSAGE("Test libraries: ${test_libs}")

  #
  # Generate main() function for run_unit_tests
  #
  CONFIGURE_FILE(${tests_dir}/test_main.in ${CMAKE_CURRENT_BINARY_DIR}/tests_main.cc @ONLY)

  #
  # Define run_unit_tests target
  #
  SET(target_run_unit_tests ${cdk_target_prefix}run_unit_tests
    CACHE INTERNAL "CDK unit test target")

  set(test_sources)
  foreach(test ${test_tests})
    list(APPEND test_sources "$<TARGET_OBJECTS:${test}>")
  endforeach()

  ADD_EXECUTABLE(${target_run_unit_tests}
                 ${CMAKE_CURRENT_BINARY_DIR}/tests_main.cc
                 ${test_sources}
  )
  set_target_properties(${target_run_unit_tests} PROPERTIES FOLDER "Tests")

  TARGET_LINK_LIBRARIES(${target_run_unit_tests} gtest)

  #
  # Be more lame with warnings when compiling tests
  #

  if (MSVC)

    target_compile_definitions(${target_run_unit_tests} PRIVATE
      -D_SCL_SECURE_NO_WARNINGS
      -D_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING
    )

    target_compile_options(${target_run_unit_tests} PRIVATE
      /wd4244
      /wd4267
      /wd4701
      /wd4018
      /wd4456  # declaration of hides previous local declaration
      /wd4668  # treat undefined macros as 0
    )

    if(STATIC_TESTS_MSVCRT)
      target_compile_options(${target_run_unit_tests} PRIVATE
        $<$<CONFIG:Debug>:/MTd>
        $<$<NOT:$<CONFIG:Debug>>:/MT>
      )
    endif()

  elseif((CMAKE_SYSTEM_NAME MATCHES "SunOS") OR CMAKE_COMPILER_IS_GNUCXX)

    target_compile_options(${target_run_unit_tests} PRIVATE
      -Wno-unused-but-set-variable
      -Wno-maybe-uninitialized
      -Wno-unused-value
    )

  elseif((CMAKE_C_COMPILER_ID MATCHES "Clang") OR (CMAKE_CXX_COMPILER_ID MATCHES "Clang"))

    target_compile_options(${target_run_unit_tests} PRIVATE
      -Wno-unused-value
    )

  else()
    #target_compile_options(${target_run_unit_tests} PRIVATE -Wno-unused-result)
  endif()

  #
  # Link with libraries required by unit tests
  #
  FOREACH(tlib ${test_libs})
    TARGET_LINK_LIBRARIES(${target_run_unit_tests} ${tlib})
  ENDFOREACH()

  #
  #  Add ctest definitions for each gtest group
  #

  SET(test_group_defs ${CMAKE_CURRENT_BINARY_DIR}/TestGroups.cmake)

  set(TEST_ENV ${test_environment})

  IF (NOT EXISTS ${test_group_defs})
    FILE(WRITE ${test_group_defs} "")
  ENDIF()

  INCLUDE(${test_group_defs})

  ADD_CUSTOM_TARGET(${cdk_target_prefix}update_test_groups
     run_unit_tests --generate_test_groups=${test_group_defs}
     SOURCES ${tests_dir}/test_main.in
  )

  set_target_properties(${cdk_target_prefix}update_test_groups PROPERTIES FOLDER "Tests")

ENDIF()
ENDMACRO(ADD_TEST_TARGET)



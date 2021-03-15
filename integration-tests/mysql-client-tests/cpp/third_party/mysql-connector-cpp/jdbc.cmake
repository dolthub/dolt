# Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.
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

message("== Configuring legacy connector build using cmake generator: ${CMAKE_GENERATOR}")

if(NOT EXISTS "${PROJECT_SOURCE_DIR}/jdbc/CMakeLists.txt")
  message(FATAL_ERROR
    "Could not find JDBC API sources at ${PROJECT_SOURCE_DIR}/jdbc."
    " Have you updated git sub-modules?"
  )
endif()

set(JDBC_DIR "${PROJECT_BINARY_DIR}/jdbc")

if(FORCE_REBUILD AND EXISTS "${JDBC_DIR}/CMakeCache.txt")
  file(REMOVE ${JDBC_DIR}/CMakeCache.txt)
endif()
file(MAKE_DIRECTORY "${JDBC_DIR}")


#############################################################################
#
# Configure legacy connector build environment in ${JDBC_DIR}
#

if(BUILD_STATIC)
  list(APPEND jdbc_cmake_opts -DENABLE_BUILD_STATIC=ON)
else()
  list(APPEND jdbc_cmake_opts -DENABLE_BUILD_DYNAMIC=ON)
endif()

if(MAINTAINER_MODE)
  list(APPEND jdbc_cmake_opts -DENABLE_BUILD_STATIC=ON)
endif()

if(MYSQL_DIR)
  list(APPEND jdbc_cmake_opts -DMYSQL_DIR=${MYSQL_DIR})
endif()

if(MYSQL_CONFIG_EXECUTABLE)
  list(APPEND jdbc_cmake_opts -DMYSQL_CONFIG_EXECUTABLE=${MYSQL_CONFIG_EXECUTABLE})
endif()

list(APPEND jdbc_cmake_opts -DMYSQLCLIENT_STATIC_LINKING=ON)

if(CMAKE_BUILD_TYPE)
  if(CMAKE_BUILD_TYPE MATCHES "[Ss][Tt][Aa][Tt][Ii][Cc]")
    list(APPEND jdbc_cmake_opts -DCMAKE_BUILD_TYPE=Release)
  else()
    list(APPEND jdbc_cmake_opts -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})
  endif()
endif()

if(DEFINED STATIC_MSVCRT)
  list(APPEND jdbc_cmake_opts -DSTATIC_MSVCRT=${STATIC_MSVCRT})
endif()

if(VS)
  list(APPEND jdbc_cmake_opts -DVS=${VS})
endif()

#
# Find Boost which is required by legacy connector
#

#include(boost)
if (DEFINED BOOST_ROOT)
  list(APPEND jdbc_cmake_opts -DBOOST_ROOT=${BOOST_ROOT})
endif()
if (DEFINED WITH_BOOST)
  list(APPEND jdbc_cmake_opts -DWITH_BOOST=${WITH_BOOST})
endif()

#
# Generate version info for legacy connector
#

configure_file(
  "${PROJECT_SOURCE_DIR}/jdbc/VersionInfo.cmake.in"
  "${JDBC_DIR}/VersionInfo.cmake"
  @ONLY
)

# Dirty trick to speed up cmake set up time.

file(
  COPY "${CMAKE_BINARY_DIR}/CMakeFiles/${CMAKE_VERSION}"
  DESTINATION "${JDBC_DIR}/CMakeFiles"
)

if(1)
execute_process(
  COMMAND ${CMAKE_COMMAND} -Wno-dev
          -G "${CMAKE_GENERATOR}"
          ${jdbc_cmake_opts}
          -DCMAKE_INSTALL_PREFIX=${JDBC_DIR}/install
          -DCMAKE_POSITION_INDEPENDENT_CODE=${CMAKE_POSITION_INDEPENDENT_CODE}
          -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
          -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
          -DCMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}
          -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE}
          -DCMAKE_CXX_FLAGS_RELWITHDEBINFO=${CMAKE_CXX_FLAGS_RELWITHDEBINFO}
          -DCMAKE_CXX_FLAGS_MINSIZEREL=${CMAKE_CXX_FLAGS_MINSIZEREL}
          -DCMAKE_STATIC_LINKER_FLAGS=${CMAKE_STATIC_LINKER_FLAGS}
          -DCMAKE_SHARED_LINKER_FLAGS=${CMAKE_SHARED_LINKER_FLAGS}
          ${PROJECT_SOURCE_DIR}/jdbc
  WORKING_DIRECTORY ${JDBC_DIR}
  RESULT_VARIABLE jdbc_config
)
endif()

if(jdbc_config)
  message(FATAL_ERROR "Could not configure legacy connector build: ${jdbc_config}")
endif()

message("== Legacy connector build configuration ready")

return()

#############################################################################
#
# Target build_jdbc which triggers build and installation of the legacy
# connector.
#

#
#  jdbc.buildstamp
#

set(JDBC_BUILD_STAMP "${PROJECT_BINARY_DIR}/jdbc.buildstamp")
file(REMOVE "${JDBC_BUILD_STAMP}")
#message("JDBC_BUILD_STAMP: ${JDBC_BUILD_STAMP}")

set(CONFIG_EXPR
  $<$<CONFIG:Static>:Release>$<$<NOT:$<CONFIG:Static>>:$<CONFIGURATION>>
)

add_custom_command(OUTPUT ${JDBC_BUILD_STAMP}

  COMMAND ${CMAKE_COMMAND} -E remove_directory install
  COMMAND ${CMAKE_COMMAND}
    --build . --target install --config ${CONFIG_EXPR} --clean-first

  # Move installed headers from include/ to include/jdbc and rename lib/
  # to lib64/ for 64-bit platform

  COMMAND ${CMAKE_COMMAND} -E remove_directory install/jdbc
  COMMAND ${CMAKE_COMMAND} -E rename install/include install/jdbc
  COMMAND ${CMAKE_COMMAND} -E make_directory install/include
  COMMAND ${CMAKE_COMMAND} -E copy_directory install/jdbc install/include/jdbc
  COMMAND ${CMAKE_COMMAND} -E remove_directory install/jdbc

  COMMAND ${CMAKE_COMMAND} -E touch ${JDBC_BUILD_STAMP}

  WORKING_DIRECTORY ${JDBC_DIR}
  COMMENT "Building legacy connector library using configuration: $(Configuration)"
)

# Collect sources of legacy connector and specify them in the build
# target.

unset(jdbc_sources)
foreach(dir driver cppconn thread)

  file(
    GLOB_RECURSE sources
    #RELATIVE "${PROJECT_SOURCE_DIR}/jdbc/${dir}"
    "${PROJECT_SOURCE_DIR}/jdbc/${dir}/*.cpp"
    "${PROJECT_SOURCE_DIR}/jdbc/${dir}/*.h"
    "${PROJECT_SOURCE_DIR}/jdbc/${dir}/*.cmake"
    "${PROJECT_SOURCE_DIR}/jdbc/${dir}/*.cm"
    "${PROJECT_SOURCE_DIR}/jdbc/${dir}/CMakeLists.txt"
  )

  # TODO: Split into Headers/Sources/Other sub-groups

  source_group(${dir} FILES ${sources})

  list(APPEND jdbc_sources ${sources})

endforeach()

add_custom_target(build_jdbc ALL
  DEPENDS ${JDBC_BUILD_STAMP}
  SOURCES ${jdbc_sources}
)


#############################################################################
#
# Import legacy connector library so that it can be used by other targets.
#

set(JDBC_INCLUDES ${Boost_INCLUDE_DIRS} "${JDBC_DIR}/install/include")
#message("legacy connector includes: ${JDBC_INCLUDES}")

#
# Import library targets exported from the legacy connector project.
#

include("${JDBC_DIR}/exports.cmake")

if(BUILD_STATIC)
  set(JDBC_LIB mysqlcppconn-static)
else()
  set(JDBC_LIB mysqlcppconn)
endif()

add_dependencies(${JDBC_LIB} build_jdbc)

set(JDBC_LIBS ${JDBC_LIB} CACHE INTERNAL "legacy connector library")


#############################################################################
#
# Installation specs for the legacy connector
#

get_target_property(configurations ${JDBC_LIB} IMPORTED_CONFIGURATIONS)

foreach(config ${configurations})

  get_target_property(location ${JDBC_LIB} IMPORTED_LOCATION_${config})
  message("jdbc installing: ${location}")

  if(config STREQUAL DEBUG)
    set(loc "${INSTALL_LIB_DIR}/debug")
    set(loc_static "${INSTALL_LIB_DIR_STATIC}/debug")
  else()
    set(loc "${INSTALL_LIB_DIR}")
    set(loc_static "${INSTALL_LIB_DIR_STATIC}")
  endif()

  if(BUILD_STATIC)

    install(
      FILES ${location}
      CONFIGURATIONS ${config}
      DESTINATION ${loc_static}
      COMPONENT JDBCDev
    )

  else()

    if(WIN32)

      install(FILES ${location} CONFIGURATIONS ${config} DESTINATION ${loc} COMPONENT JDBCDll)

      # install import library for the DLL

      get_target_property(imp_location ${JDBC_LIB} IMPORTED_IMPLIB_${config})

      install(
        FILES ${imp_location}
        CONFIGURATIONS ${config}
        DESTINATION ${loc_static}
        COMPONENT JDBCDev
      )

    else()

      # We need to install soname and linkname links together with the shared
      # library.

      get_filename_component(name "${location}" NAME)
      get_filename_component(loc_path "${location}" PATH)
      get_target_property(soname ${JDBC_LIB} IMPORTED_SONAME_${config})
      set(linkname "${CMAKE_SHARED_LIBRARY_PREFIX}mysqlcppconn${CMAKE_SHARED_LIBRARY_SUFFIX}")

      #
      # Warning: Using undocumented file(INSTALL ...) signature which creates
      # required symlinks. This command is used in install scripts generated
      # by cmake.
      #

      install(CODE
       "file(INSTALL
          DESTINATION \"\${CMAKE_INSTALL_PREFIX}/${loc}\"
          TYPE SHARED_LIBRARY
          FILES
            \"${location}\"
            \"${loc_path}/${soname}\"
            \"${loc_path}/${linkname}\"
       )"
      )

    endif()

  endif()

endforeach()


install(
  DIRECTORY ${JDBC_DIR}/install/include/jdbc
  DESTINATION ${INSTALL_INCLUDE_DIR}
  COMPONENT JDBCDev
)

#
# In maintainer mode add specifications for installing the static library
# which is always built in this mode.
#

if(MAINTAINER_MODE)

  add_dependencies(mysqlcppconn-static build_jdbc)
  get_target_property(location mysqlcppconn-static IMPORTED_LOCATION_RELEASE)
  message("jdbc installing: ${location} (MAINTAINER_MODE)")

  install(
    FILES ${location}
    CONFIGURATIONS Static
    DESTINATION "${INSTALL_LIB_DIR_STATIC}"
    COMPONENT JDBCDev
  )

endif()


#
#  Install external dependencies of MySQL client library, such as OpenSSL,
#  if bundled with client library installation.
#
#  Note: if main connector uses OpenSSL, then we will use the same libraries
#  to satisfy client library dependency. But if main connector does not use
#  OpenSSL, we copy required dependencies from MySQL installation.
#

if(BUNDLE_DEPENDENCIES AND WITH_SSL STREQUAL "bundled")

  message("Bundling OpenSSL libraries from: ${MYSQL_DIR}")

  install(DIRECTORY "${MYSQL_DIR}/bin/" DESTINATION lib64
    FILES_MATCHING PATTERN "*${CMAKE_SHARED_LIBRARY_SUFFIX}"
    COMPONENT JDBCDev
  )

endif()

#############################################################################
#
#  Public header checks
#

if(WITH_HEADER_CHECKS)

  # Prepare location where checks will be performed.

  set(CHECK_DIR "${JDBC_DIR}/headers_check")

  file(REMOVE_RECURSE "${CHECK_DIR}")
  file(MAKE_DIRECTORY "${CHECK_DIR}")

  # Dirty trick to speed up cmake set up time.

  file(
    COPY "${CMAKE_BINARY_DIR}/CMakeFiles/${CMAKE_VERSION}"
    DESTINATION "${CHECK_DIR}/CMakeFiles"
  )

  #
  # Target to run header checks.
  #

  ADD_CUSTOM_TARGET(Headers_jdbc
    COMMAND ${CMAKE_COMMAND}
      -DCMAKE_GENERATOR=${CMAKE_GENERATOR}
      -DJDBC_DIR=${JDBC_DIR}
      -DJDBC_INCLUDES="${JDBC_INCLUDES}"
      -DHEADERS_DIR=${PROJECT_SOURCE_DIR}/cdk/cmake/headers
      -DCHECK_DIR=${CHECK_DIR}
      -P ${PROJECT_SOURCE_DIR}/cmake/jdbc_headers_check.cmake
    SOURCES ${all_headers}
  )

  add_dependencies(Headers_jdbc build_jdbc)


  add_test(NAME Headers_jdbc
    COMMAND cmake --build . --target Headers_jdbc
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR}
  )
  message(STATUS "Added JDBC public headers test")

endif()

message("== legacy connector build configured")

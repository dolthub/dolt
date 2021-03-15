# Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0, as
# published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation. The authors of MySQL hereby grant you an
# additional permission to link the program and your derivative works
# with the separately licensed software that they have included with
# MySQL.
#
# Without limiting anything contained in the foregoing, this file,
# which is part of MySQL Connector/ODBC, is also subject to the
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
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

##########################################################################

FUNCTION(GENERATE_INFO_BIN)

  IF(DEFINED ENV{PRODUCT_ID} AND "$ENV{PRODUCT_ID}" MATCHES "source-dist")
    MESSAGE("Generating INFO_BIN is skipped for the source package")
    RETURN()
  ENDIF()

  MESSAGE("Generating INFO_BIN")
  string(TIMESTAMP INFO_DATE "%Y-%m-%d")

  IF (CMAKE_BUILD_TYPE)
    SET(INFO_BUILD_TYPE "build-type           :  ${CMAKE_BUILD_TYPE}\n")
  ENDIF()

  IF(OPENSSL_VERSION_GLOBAL)
      SET(INFO_SSL "ssl-library          :  OpenSSL ${OPENSSL_VERSION_GLOBAL}\n")
  ENDIF()

  IF(CMAKE_OSX_DEPLOYMENT_TARGET)
    SET(INFO_MACOS_TARGET "macos-target         :  ${CMAKE_OSX_DEPLOYMENT_TARGET}\n")
  ENDIF()

  IF(APPLE)
    execute_process(
      COMMAND sw_vers -productVersion
      ERROR_QUIET
      OUTPUT_VARIABLE _prod_version
      RESULT_VARIABLE _result_code1
      OUTPUT_STRIP_TRAILING_WHITESPACE
    )

    IF(_prod_version)
      SET(INFO_MACOS_VERSION "macos-version        :  ${_prod_version}\n")
    ENDIF()

    execute_process(
      COMMAND sw_vers -buildVersion
      ERROR_QUIET
      OUTPUT_VARIABLE _build_version
      RESULT_VARIABLE _result_code2
      OUTPUT_STRIP_TRAILING_WHITESPACE
    )

    IF(_build_version)
      SET(INFO_MACOS_BUILD "macos-build          :  ${_build_version}\n")
    ENDIF()
  ENDIF()

  IF(WITH_JDBC)
    IF(Boost_VERSION)
      SET(INFO_BOOST "boost-version        :  ${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}\n")
    ENDIF()

    SET(INFO_MACOS_BUILD "mysql-version        :  ${MYSQL_VERSION}\n")
  ENDIF()

  CONFIGURE_FILE(INFO_BIN.in "${CMAKE_BINARY_DIR}/INFO_BIN")

  install(
    FILES "${CMAKE_BINARY_DIR}/INFO_BIN"
    DESTINATION ${INSTALL_DOC_DIR}
    COMPONENT Readme
  )

ENDFUNCTION()

include(version.cmake)

FUNCTION(GENERATE_INFO_SRC)
  MESSAGE("Generating INFO_SRC")

  IF (NOT EXISTS INFO_SRC)
    SET(INFO_VERSION "${CONCPP_VERSION}")

    find_program(GIT_FOUND NAMES git)

    IF(GIT_FOUND AND IS_DIRECTORY "${PROJECT_SOURCE_DIR}/.git")
      execute_process(
        COMMAND git symbolic-ref --short HEAD
        ERROR_QUIET
        OUTPUT_VARIABLE GIT_BRANCH_NAME
        RESULT_VARIABLE _result_code3
        OUTPUT_STRIP_TRAILING_WHITESPACE
      )

      IF (GIT_BRANCH_NAME)
        execute_process(
          COMMAND git log -1 --format=%cd --date=short
          ERROR_QUIET
          OUTPUT_VARIABLE GIT_DATE
          RESULT_VARIABLE _result_code4
          OUTPUT_STRIP_TRAILING_WHITESPACE
        )

        IF(GIT_DATE)
          SET(INFO_DATE "date                 :  ${GIT_DATE}\n")
        ENDIF()

        IF(GIT_BRANCH_NAME)
          SET(INFO_BRANCH "branch               :  ${GIT_BRANCH_NAME}\n")
        ENDIF()


        execute_process(
          COMMAND git rev-parse HEAD
          ERROR_QUIET
          OUTPUT_VARIABLE GIT_COMMIT
          RESULT_VARIABLE _result_code5
          OUTPUT_STRIP_TRAILING_WHITESPACE
        )

        IF(GIT_COMMIT)
          SET(INFO_COMMIT "commit               :  ${GIT_COMMIT}\n")
        ENDIF()

        execute_process(
          COMMAND git rev-parse --short HEAD
          ERROR_QUIET
          OUTPUT_VARIABLE GIT_SHORT
          RESULT_VARIABLE _result_code6
          OUTPUT_STRIP_TRAILING_WHITESPACE
        )

        IF(GIT_SHORT)
          SET(INFO_SHORT "short                :  ${GIT_SHORT}\n")
        ENDIF()
      ENDIF()
    ELSE()
      # git local repository does not exist, but the env variables might be set
      IF(DEFINED ENV{PUSH_REVISION})
          SET(INFO_COMMIT "commit               :  $ENV{PUSH_REVISION}\n")
          STRING(SUBSTRING $ENV{PUSH_REVISION} 0 8 GIT_SHORT)
          SET(INFO_SHORT "short                :  ${GIT_SHORT}\n")
      ENDIF()

      IF(DEFINED ENV{BRANCH_SOURCE})
          STRING(REGEX MATCH " [^ ]+" GIT_BRANCH_NAME $ENV{BRANCH_SOURCE})
          SET(INFO_BRANCH "branch               : ${GIT_BRANCH_NAME}\n")
      ENDIF()
    ENDIF()

    CONFIGURE_FILE(INFO_SRC.in "${CMAKE_SOURCE_DIR}/INFO_SRC")
  ENDIF()

  install(
    FILES "${CMAKE_SOURCE_DIR}/INFO_SRC"
    DESTINATION ${INSTALL_DOC_DIR}
    COMPONENT Readme
  )

ENDFUNCTION()

GENERATE_INFO_SRC()
GENERATE_INFO_BIN()

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

IF (WITH_TESTS)

ADD_NG_TEST(parser-t parser-t.cc)

#
# The expr_test program
#

# Parser headers
INCLUDE_DIRECTORIES(..)

# expr_test uses protobuf message builders
INCLUDE_DIRECTORIES(
  ${PROJECT_SOURCE_DIR}/protocol/mysqlx
  ${PROJECT_BINARY_DIR}/protocol/mysqlx
  ${PROTOBUF_INCLUDE_DIR}
)

# expr_test uses expression converters
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/mysqlx)

add_executable(expr_test expr_test.cc)
TARGET_LINK_LIBRARIES(expr_test cdk)

if(MSVC)
  target_compile_options(expr_test PRIVATE /W3)
endif()

#
# Add unit test that runs expr_test using runtests.py script
#

# Copy test files to build location so that files produced by runtest.py
# do not pollute source tree.

FILE(COPY "${PROJECT_SOURCE_DIR}/extra/exprtest"
     DESTINATION "${CMAKE_BINARY_DIR}")

find_program(PYTHON_EXECUTABLE NAMES python
             PATHS "c:/python" "c:/python25" "c:/python26" "c:/python27" "c:/python32" "c:/python33")

MESSAGE("Python path: ${PYTHON_EXECUTABLE}")

if(PYTHON_EXECUTABLE)

add_test(NAME Expr_test
         WORKING_DIRECTORY "${PROJECT_BINARY_DIR}/exprtest"
         COMMAND ${PYTHON_EXECUTABLE} runtests.py $<TARGET_FILE:expr_test>)
         MESSAGE("Added Expr_test")

endif()


ENDIF(WITH_TESTS)

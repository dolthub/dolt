# Copyright (c) 2017, 2019, Oracle and/or its affiliates. All rights reserved.
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

file(GLOB HEADERS *.h)

add_library(common STATIC
  session.cc result.cc collection.cc value.cc
  ${HEADERS}
)

target_link_libraries(common cdk)

#
#  Generate version info
#

set(CLIENT_VERSION ${CONCPP_VERSION})

if(EXTRA_NAME_SUFFIX STREQUAL "-commercial")
  SET(CONCPP_LICENSE "COMMERCIAL")
else()
  SET(CONCPP_LICENSE "GPL-2.0")
endif()

#
# Note: generated version_info.h is placed in the build location
# and the common target is configured to include it before the
# original version in the sources
#

configure_file(version_info.h version_info.h @ONLY)

# FIXME: This include path is made public, because other components
# such as devapi use private headers of the common component.

target_include_directories(common
  BEFORE PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
)

add_subdirectory(tests)

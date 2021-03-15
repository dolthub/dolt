# Copyright (c) 2018, 2019, Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have included with MySQL.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

IF(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  RETURN()
ENDIF()
IF (NOT DEFINED DEB_CODENAME)
  execute_process(
    COMMAND lsb_release -cs
    OUTPUT_VARIABLE DEB_CODENAME
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  SET (DEB_CODENAME ${DEB_CODENAME} CACHE STRING "")
ENDIF()
# Timestamp for use in debian/changelog
IF (NOT DEFINED DEB_CHANGELOG_TIMESTAMP)
  execute_process(
    COMMAND date --rfc-2822
    OUTPUT_VARIABLE DEB_CHANGELOG_TIMESTAMP
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  SET (DEB_CHANGELOG_TIMESTAMP ${DEB_CHANGELOG_TIMESTAMP} CACHE STRING "")
ENDIF()

# Commercial or community
IF (DEB_PRODUCT STREQUAL "commercial")
  SET (DEB_COPYRIGHT_UPSTREAMNAME "MySQL Commercial ${MYSQL_BASE_VERSION}")
  SET (DEB_PRODUCTNAME "-commercial")
  SET (DEB_LICENSENAME "Commercial")
  SET (DEB_VERSION "+commercial-1")
ELSE()
  SET (DEB_COPYRIGHT_UPSTREAMNAME "MySQL ${MYSQL_BASE_VERSION}")
  SET (DEB_PRODUCTNAME "")
  SET (DEB_LICENSENAME "GPL")
  SET (DEB_VERSION "-1")
ENDIF()



# Platform specifics. The differences are generally only distro name used in version string
IF(DEB_CODENAME STREQUAL "stretch")
  SET (DEB_PLATFORMRELEASE "debian9")
ELSEIF(DEB_CODENAME STREQUAL "buster")
  SET (DEB_PLATFORMRELEASE "debian10")
ELSEIF(DEB_CODENAME STREQUAL "sid")
  IF (DEFINED DEB_GCC_SNAPSHOT)
    SET (DEB_CMAKE_EXTRAS "${DEB_CMAKE_EXTRAS} -DCMAKE_C_COMPILER=/usr/lib/gcc-snapshot/bin/gcc -DCMAKE_CXX_COMPILER=/usr/lib/gcc-snapshot/bin/g++ -DMYSQL_MAINTAINER_MODE=0 -DCMAKE_CXX_COMPILER_LAUNCHER=ccache")
  ENDIF()
  SET (DEB_PLATFORMRELEASE "debianunstable")
ELSEIF(DEB_CODENAME STREQUAL "xenial")
  SET (DEB_PLATFORMRELEASE "ubuntu16.04")
  # Generate debug symbol packages (this is done automatically in Debian9+)
  SET (DEB_RULES_STRIP
"
override_dh_strip:
	dh_strip -plibmysqlcppconn7 --dbg-package=libmysqlcppconn7-dbgsym
	dh_strip -plibmysqlcppconn8-2 --dbg-package=libmysqlcppconn8-2-dbgsym
")
  SET (DEB_CONTROL_DBGSYM
"
Package: libmysqlcppconn7-dbgsym
Architecture: any
Section: debug
Depends: libmysqlcppconn7 (=\${binary:Version}), \${misc:Depends}
Description: Debugging symbols for compat library

Package: libmysqlcppconn8-2-dbgsym
Architecture: any
Section: debug
Depends: libmysqlcppconn8-2 (= \${binary:Version}), \${misc:Depends}
Description: Debugging symbols for main library
")
ELSEIF(DEB_CODENAME STREQUAL "bionic")
  SET (DEB_PLATFORMRELEASE "ubuntu18.04")
ELSEIF(DEB_CODENAME STREQUAL "eoan")
  SET (DEB_PLATFORMRELEASE "ubuntu19.10")
ELSEIF(DEB_CODENAME STREQUAL "focal")
  SET (DEB_PLATFORMRELEASE "ubuntu20.04")
ELSE()
  MESSAGE(STATUS
    "Skipping deb packaging on unsupported platform ${DEB_CODENAME}.")
  RETURN()
ENDIF()

# All files are configured and copied to the debian/ directory, which is used 
# by debuild to make the packages
SET (DEB_ROOT ${CMAKE_SOURCE_DIR}/packaging/deb-in)
FILE(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/debian)
FILE(GLOB_RECURSE SOURCEFILES RELATIVE "${DEB_ROOT}" "${DEB_ROOT}/*")
# Skip the copyright file from the "other" product type
LIST(REMOVE_ITEM SOURCEFILES "copyright.${DEB_REMOVEPATTERN}")
LIST(REMOVE_ITEM SOURCEFILES "CMakeLists.txt")
FOREACH (SOURCEFILE ${SOURCEFILES})
  STRING(REGEX REPLACE "-packagesource-" "${DEB_PRODUCTNAME}-"
    TMPFILE ${SOURCEFILE})
  # Strip away the input file endings from the destination filenames
  STRING(REGEX REPLACE ".in$" "" DESTFILE ${TMPFILE})
  CONFIGURE_FILE("${DEB_ROOT}/${SOURCEFILE}"
    "${CMAKE_BINARY_DIR}/debian/${DESTFILE}" @ONLY)
ENDFOREACH()
EXECUTE_PROCESS(
  COMMAND chmod +x ${CMAKE_BINARY_DIR}/debian/rules
)


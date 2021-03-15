#
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

# ======================================================================
# Install manifest
# ======================================================================

# If requested, generate install manifest file which contains information
# about install components and files in each component. INSTALL_MANIFEST
# should be a path to the manifest file.
# See the install_mainfest.cmake script in this directory.

if(DEFINED INSTALL_MANIFEST)

add_custom_target(generate_manifest
  ALL
  COMMAND ${CMAKE_COMMAND}
    -D MANIFEST_FILE=${INSTALL_MANIFEST}
    -D BUILD_DIR=${PROJECT_BINARY_DIR}
    -D CONFIG=$<CONFIG>
    -P "${CMAKE_CURRENT_SOURCE_DIR}/install_manifest.cmake"
  WORKING_DIRECTORY ${PROJECT_BINARY_DIR}
  COMMENT "Generating install manifest"
)

endif()


# ======================================================================
# Licenses for binary packages
# ======================================================================

if(EXISTS "${CMAKE_SOURCE_DIR}/LICENSE.mysql.txt")
  set(LIC_FILE "LICENSE.mysql")       # Without ".txt" extension
else()
  set(LIC_FILE "LICENSE") # Without ".txt" extension
endif()

if(WIN32)
  set(info_ext ".txt")
  set(newline WIN32)
else()
  set(info_ext "")
  set(newline UNIX)
endif()

set(info_files README ${LIC_FILE})

foreach(file ${info_files})

  set(file_src "${CMAKE_SOURCE_DIR}/${file}.txt")
  set(file_bin "${CMAKE_BINARY_DIR}/${file}${info_ext}")

  configure_file("${file_src}" "${file_bin}" NEWLINE_STYLE ${newline})
  install(FILES "${file_bin}" DESTINATION ${INSTALL_DOC_DIR} COMPONENT Readme)

endforeach()

set(CPACK_RESOURCE_FILE_README  "${CMAKE_BINARY_DIR}/README${info_ext}")
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_BINARY_DIR}/${LIC_FILE}${info_ext}")
#set(CPACK_RESOURCE_FILE_INSTALL "...")    # FIXME


# ======================================================================
# Package specifications
# ======================================================================

if(APPLE AND NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  message(FATAL_ERROR "To create packages for OSX, build with clang compiler.")
endif()


include(PackageSpecs.cmake)

CONFIGURE_FILE(mysql-connector-c++.spec.in ${CMAKE_CURRENT_BINARY_DIR}/mysql-connector-c++.spec @ONLY)
ADD_SUBDIRECTORY(deb-in)


# ======================================================================
# Custom target to build packages.
# ======================================================================
#
# Note: target build_packages builds TGZ/ZIP packages using cpack.
# Other package types are built outside of cmake/cpack, cmake is only
# used to prepare information necessary to build them.
#
# Note: For information on building MSI packages look at WiX/CMakeLists.txt
#


add_custom_target(build_packages
  COMMAND cpack --config CPackConfig.cmake --verbose -C $<CONFIGURATION>
  COMMAND cpack --config CPackSourceConfig.cmake
  DEPENDS clean_source_tree
)

add_custom_target(clean_source_tree
  COMMAND git clean -x -d -f
  COMMAND git submodule foreach --recursive git clean -x -d -f
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
  COMMENT "Cleaning source tree"
)

set_property(TARGET clean_source_tree build_packages
  PROPERTY EXCLUDE_FROM_ALL 1
)

set_property(TARGET clean_source_tree build_packages
  PROPERTY EXCLUDE_FROM_DEFAULT_BUILD 1
)


include(CPack)

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
# This script collects information about install components and files
# inside each components and writes it to install_mainfest.cmake file
# in form of set() commands that set the following variables:
#
# - COMPONENTS    -- list of install components
# - FILES_${CCC}  -- list of files in component CCC
#
# The information is collected from CPackConfig.cmake and cmake_install.cmake
# files that are generated in project's build location. This location must
# be give via BUILD_DIR variable.
#
# If INSTALL_DIR is defined and contains install_manifest.cmake, the
# information collected here is merged with the information from
# ${INSTALL_DIR}/install_manifest.cmake. This way information from several
# builds can be merged together (easiest if all these builds install to
# the same install location).
#


# Check if requiret input files exist

if(NOT EXISTS "${BUILD_DIR}/CPackConfig.cmake")
  message(FATAL_ERROR "Could not find CPackConfig.cmake at: ${BUILD_DIR}")
endif()

if(NOT EXISTS "${BUILD_DIR}/cmake_install.cmake")
  message(FATAL_ERROR "Could not find cmake_install.cmake at: ${BUILD_DIR}")
endif()

# Read existing information to be extended (if present) and CPackConfig.cmake
# to get list of install components. Update COMPONENTS basead on this.

include("${MANIFEST_FILE}" OPTIONAL)
include("${BUILD_DIR}/CPackConfig.cmake")

list(APPEND COMPONENTS ${CPACK_COMPONENTS_ALL})
list(REMOVE_DUPLICATES COMPONENTS)

#
# Now we will include cmake_install.cmake which uses file(INSTALL ...) commands
# to install all files. We redefine file() command to extract file information
# instead of copying it to the destination.
#

function(file CMD)

  # we only look at file(INSTALL ...) variant

  if(NOT CMD STREQUAL "INSTALL")
    return()
  endif()

  #
  # example file(INSTALL ...) invocation in cmake_install.cmake:
  #
  #  file(INSTALL DESTINATION "..." TYPE STATIC_LIBRARY OPTIONAL FILES ...)
  #
  # we read destination location and remove all arguments up to the list of
  # files
  #

  list(GET ARGN 1 DEST)
  list(REMOVE_AT ARGN 0 1 2 3 4)

  # Because of things like OPTIONAL, which can be present or not, the first
  # item in ARGN can be the FILES keyword - remove it if this is the case.

  list(GET ARGN 0 FIRST)
  if(FIRST STREQUAL "FILES")
    list(REMOVE_AT ARGN 0)
  endif()
 
  # Now process the files and append them to FILES_CCC list

  foreach(F ${ARGN})
    get_filename_component(FN ${F} NAME)
    #message("- adding (${CMAKE_INSTALL_COMPONENT}): ${DEST}/${FN}")
    list(APPEND FILES_${CMAKE_INSTALL_COMPONENT} "${DEST}/${FN}")
  endforeach()

  list(REMOVE_DUPLICATES FILES_${CMAKE_INSTALL_COMPONENT})
  #message("-- files: ${FILES_${CMAKE_INSTALL_COMPONENT}}")
  set(FILES_${CMAKE_INSTALL_COMPONENT} ${FILES_${CMAKE_INSTALL_COMPONENT}} PARENT_SCOPE)

endfunction(file)


#
# Include cmake_install.cmake one time for each component, each time
# setting CMAKE_INSTALL_COMPONENT to the component being processed. This
# way only files from that component are processed inside cmake_install
# script.
#

if(CONFIG)
  set(CMAKE_INSTALL_CONFIG_NAME ${CONFIG})
else()
  set(CMAKE_INSTALL_CONFIG_NAME "Release")
endif()
set(CMAKE_INSTALL_PREFIX ".")

message("Install manifest for build configuration: ${CMAKE_INSTALL_CONFIG_NAME}")

foreach(COMP ${CPACK_COMPONENTS_ALL})
  set(CMAKE_INSTALL_COMPONENT ${COMP})
  #message("\nComponent: ${CMAKE_INSTALL_COMPONENT}")
  include(${BUILD_DIR}/cmake_install.cmake)
  #message("-- files: ${FILES_${CMAKE_INSTALL_COMPONENT}}")
endforeach()

# Write gathered information to the output file.
# Note: Original file() command was redefined above!

_file(WRITE ${MANIFEST_FILE}
  "# Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.\n"
  "# This is generated file.\n\n"
  "set(COMPONENTS ${COMPONENTS})\n\n"
)

foreach(COMP ${COMPONENTS})
  _file(APPEND ${MANIFEST_FILE}
    "set(FILES_${COMP} ${FILES_${COMP}})\n"
  )
endforeach()

message("Wrote: ${MANIFEST_FILE}")

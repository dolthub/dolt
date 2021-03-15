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
# Infrastructure for managing public headers of the project
# =========================================================
#
# This infrastructure assumes that all public headers are located in
# a single folder of the project and its sub-folders. The top-level
# folder should contain a CMakeLists.txt with the following declarations:
#
#   SETUP_HEADERS(<top-level dir>)
#
#   <public header declarations>
#
#   ADD_HEADERS_TARGET()
#
# Command ADD_HEADERS_TARGET() adds to the project a target for doing
# sanity checks on headers. In GUI systems the corresponding project
# contains all declared public headers for easy access (implemented with
# SOURCE_GROUP).
#
# Public header declarations are either:
#
# ADD_HEADERS(<list of headers>) - add given headers in the current folder
# ADD_HEADERS_DIR(<dir name>)    - add all headers declared in the named sub-folder
#

if(NOT DEFINED WITH_HEADER_CHECKS)
  option(WITH_HEADER_CHECKS "Add Public header checks to the project" OFF)
endif()

#
# Determine location of files accompanying this headers.cmake module which are
# in heders/ sub-folder of the location where headers.cmake is stored
#

GET_FILENAME_COMPONENT(headers_dir ${CMAKE_CURRENT_LIST_FILE} PATH)
SET(headers_dir "${headers_dir}/headers")
#MESSAGE("headers.cmake: ${headers_dir}")


#
# Check if given list of headers includes all headers that can be found in
# the current directory.
#

function(check_headers)

  file(GLOB all_headers RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.h")

  foreach(header IN LISTS ARGV)
    #message("- checking header: ${header}\n")
    list(REMOVE_ITEM all_headers ${header})
  endforeach()

  list(LENGTH all_headers remains)

  if(remains GREATER 0)
    message(WARNING "Extra headers found in ${CMAKE_CURRENT_SOURCE_DIR}: ${all_headers}")
  endif()

endfunction()


#
# Set-up header declarations with given folder as a base location for all
# public headers.
#
# A CMakeLists.txt file for a project doing sanity header checks
# is created in the corresponding build location. This file is generated
# from check.cmake.in template. When headers are declared later
# appropriate commands are written to this CMakeLists.txt file.
#
# Variable hdrs_init is used to ensure single initialization of each sub-folder
# declaring public headers (see HEADERS_DIR()).
#

MACRO(SETUP_HEADERS base_dir)
if(WITH_HEADER_CHECKS)

  GET_FILENAME_COMPONENT(hdr_base_dir ${base_dir} ABSOLUTE)
  MESSAGE(STATUS "Public headers directory: ${hdr_base_dir}")
  SET(hdr_include_dir ${CMAKE_CURRENT_SOURCE_DIR})

  file(RELATIVE_PATH headers_check_base_dir ${CMAKE_CURRENT_SOURCE_DIR} ${hdr_base_dir})
  set(headers_check_base_dir "${CMAKE_CURRENT_BINARY_DIR}/${headers_check_base_dir}/check"
      CACHE INTERNAL "locations of headers check project")
  file(REMOVE_RECURSE ${headers_check_base_dir})
  FILE(MAKE_DIRECTORY ${headers_check_base_dir})
  CONFIGURE_FILE(${headers_dir}/check.cmake.in
                 ${headers_check_base_dir}/CMakeLists.txt @ONLY)
  #message("headers: top-level CMakeLists.txt generated in: ${headers_check_base_dir}")

endif()
ENDMACRO(SETUP_HEADERS)


#
# Initialize current folder for public header declarations.
#
# A sub-folder foo/bar/baz of the base headers folder adds its headers to group
# named "foo\\bar\\baz". The name of the grup and its variant of the form
# "foo_bar_baz" are computed in hdr_group and hdr_prefix variables. If this is the
# base headers folder then hdr_group is ".". If this folder is outside of the headers
# base folder then hdr_group is "".
#
# The header group of this folder and all header sub-folders declared here are
# collected in hdr_groups variable. All header files declared in this folder are
# collected in hdr_list variable.
#
# Headers declared here are added to the project which does header sanity checks.
# This is done by writing commands to CMakeLists.txt file in the corresponding build
# location. The file is initialized here.
#
# Macro HEADERS_DIR() is protected with hdrs_init variable so that it can be called
# several times but initializes given folder only once.
#

MACRO(HEADERS_DIR)

IF(NOT hdrs_init)

  IF(NOT hdr_base_dir)
    MESSAGE(FATAL_ERROR "Header declarations without prior SETUP_HEADERS()")
  ENDIF()

  SET(hdrs_init 1)

  #
  # Compute header group name and prefix.
  #

  FILE(RELATIVE_PATH rel_path ${hdr_base_dir} ${CMAKE_CURRENT_SOURCE_DIR})

  IF(rel_path STREQUAL "")
    SET(rel_path ".")
  ELSEIF(rel_path MATCHES "^\\.\\.")
    #MESSAGE("outside headers dir")
    SET(rel_path "")
  ENDIF()

  STRING(REPLACE "/" "\\" hdr_group "${rel_path}")
  STRING(REPLACE "/" "_"  hdr_prefix "${rel_path}")

  #
  # Add header group of this folder to hdr_groups list, reset hdr_list.
  #

  LIST(APPEND hdr_groups ${hdr_group})
  #MESSAGE("Current list of header groups: ${hdr_groups}")

  SET(hdr_list "")

  #
  # Initialize CMakeLists.txt file for the headers check project.
  #

  if(rel_path)

    set(current_check_dir "${headers_check_base_dir}/${rel_path}")
    set(check_cmakelists   "${current_check_dir}/CMakeLists.txt")
    #message("current check dir: ${current_check_dir}")

    # Note: the top-level CMakeLists.txt was generated by
    # SETUP_HEADERS()

    if(NOT rel_path STREQUAL ".")
      file(MAKE_DIRECTORY ${current_check_dir})
      file(WRITE ${check_cmakelists} "# Auto generated file\n")
    endif()

  else()
    set(check_cmakelists "")
  endif()

ENDIF()

ENDMACRO(HEADERS_DIR)


#
# Declare list of headers in the current folder as public headers of the project.
#
# Declared headers are appended to hdr_list variable and global variable headers_GGG
# is set to the current value of hdr_list where GGG is the name of header group of
# this folder. Also hdr_groups variable in parent scope is updated to make sure that
# it contains the current list of all header groups defined so far in this folder.
#
# Note that ADD_HEADERS() can be called several times in a given folder adding new
# headers to the list.
#

MACRO(ADD_HEADERS)
if(WITH_HEADER_CHECKS)

  HEADERS_DIR()

  IF(hdr_group STREQUAL "")
    MESSAGE(ERROR "Headers added from outside of header base dir"
                  " (${hdr_base_dir}) were ignored")

  ELSE()

    MESSAGE(STATUS "Adding public headers in: ${hdr_group}")

    FOREACH(hdr ${ARGV})
      GET_FILENAME_COMPONENT(hdrn ${hdr} NAME)
      GET_FILENAME_COMPONENT(hdr ${hdr} ABSOLUTE)
      MESSAGE(STATUS " - ${hdrn}")
      LIST(APPEND hdr_list ${hdr})
    ENDFOREACH(hdr)

    #
    # Set/update global headers_GGG variable to hold the current list
    # of headers in this group.
    #
    SET(headers_${hdr_group} ${hdr_list}
        CACHE INTERNAL "Public headers from ${hdr_group}"
        FORCE)

    #
    # Update parent's hdr_groups list to make sure that it contains
    # all header groups collected so far
    #
    SET(hdr_groups ${hdr_groups} PARENT_SCOPE)
    #MESSAGE("Current list of header groups: ${hdr_groups}")

  ENDIF()

endif()
ENDMACRO(ADD_HEADERS list)


#
# Add public header declarations from a named sub-folder.
#
# Variable hdr_groups in parent scope is updated to include new header
# groups introduced in the sub-folder.
#
# The sub-folder is also added to the header sanity-checks project.
#

MACRO(ADD_HEADERS_DIR dir)
if(WITH_HEADER_CHECKS)

  HEADERS_DIR()

  #
  # Save current value of hdr_groups in all_hdr_groups because it will be changed
  # by included sub-folder.
  #
  SET(all_hdr_groups ${hdr_groups})

  #
  # Reset hdrs_init to 0 because we want folder initialization to happen in the
  # sub-folder.
  #
  SET(hdrs_init 0)

  #
  # Headers declared in the sub-folder will be added to new header groups. A list
  # of these new groups will be appended to hdr_groups.
  #
  ADD_SUBDIRECTORY(${dir})

  SET(hdrs_init 1)

  #
  # Update parent's hdr_groups to hold the extended list of groups.
  #
  SET(hdr_groups ${hdr_groups} PARENT_SCOPE)
  #MESSAGE("Current list of header groups: ${hdr_groups}")

  #
  # Add sub-folder to header sanity checks project
  #
  if(check_cmakelists)
    FILE(APPEND ${check_cmakelists} "ADD_SUBDIRECTORY(${dir})\n")
  endif()

else()

  #
  # Even if header checks are disabled, we still need to include header
  # subdirectories to execute other cmake commands that might be present there.
  #
  ADD_SUBDIRECTORY(${dir})

endif()
ENDMACRO(ADD_HEADERS_DIR dir)


#
# Add all headers declared in the current folder to sanity checks project.
#
# Sanity check consists of compiling a simple file which includes given header alone.
# This file is generated from check.source.in template with @HEADER@ placeholder for
# header name (without extension).
#
# Note: this command should be executed after declaring all the headers in the folder.
#

MACRO(ADD_HEADER_CHECKS)
if(WITH_HEADER_CHECKS)

  #
  # For each header HHH generate test source file check_HHH.cc and add it to ceck_sources
  # list
  #

  SET(check_sources "")
  SET(hdr_names "")
  FOREACH(hdr ${hdr_list})
    GET_FILENAME_COMPONENT(hdrn ${hdr} NAME_WE)
    #MESSAGE("processing header: ${hdrn}.h")
    LIST(APPEND check_sources "check_${hdrn}.cc")
    LIST(APPEND hdr_names "${hdrn}.h")
    SET(HEADER "${CMAKE_CURRENT_SOURCE_DIR}/${hdrn}.h")
    SET(HEADERN "${hdrn}")
    CONFIGURE_FILE(${headers_dir}/check.source.in "${current_check_dir}/check_${hdrn}.cc" @ONLY)
  ENDFOREACH(hdr)

  #
  # Add static library check_GGG (where GGG is the header groups of this folder) built from
  # test sources for all the headers. Put this folder in include path as the check project
  # can be in different location.
  #

  FILE(APPEND ${check_cmakelists} "INCLUDE_DIRECTORIES(\"${CMAKE_CURRENT_BINARY_DIR}\")\n")
  FILE(APPEND ${check_cmakelists} "ADD_LIBRARY(check_${hdr_prefix} STATIC ${check_sources} ${hdr_list})\n")

endif()
ENDMACRO(ADD_HEADER_CHECKS)



#
# Add a target for public headers.
#
# Building this target will execute sanity checks for all declared public headers.
#
# Note: this macro should be called from the base headers folder and after declaring
# all public headers of the project.
#

MACRO(ADD_HEADERS_TARGET)
if(WITH_HEADER_CHECKS)

  #MESSAGE("groups: ${hdr_groups}")

  #
  # Collect all declared public headers in all_headers list. Headers are collected from
  # all header groups listed in all_hdr_groups variable. For group GGG the list of public
  # headers in that group is stored in headers_GGG variable. For each header group a
  # corresponding SOURCE_GROUP() is declared.
  #

  SET(all_headers "")
  FOREACH(group ${hdr_groups})
    #MESSAGE("Headers in ${group}: ${headers_${group}}")
    LIST(APPEND all_headers ${headers_${group}})
    IF(group STREQUAL ".")
      SET(group_name "Headers")
    ELSE()
      SET(group_name "Headers\\${group}")
    ENDIF()
    SOURCE_GROUP(${group_name} FILES ${headers_${group}})
  ENDFOREACH(group)



  #
  # Add the Header target which builds the sanity check project. All public headers are
  # listed as sources of this target (which gives easy access to them in GUI systems).
  #

  ADD_CUSTOM_TARGET(Headers
    COMMAND ${CMAKE_COMMAND} --build . --clean-first
    WORKING_DIRECTORY ${headers_check_base_dir}
    COMMENT "Header checks"
    SOURCES ${all_headers}
  )
  set_target_properties(Headers PROPERTIES FOLDER "Tests")

  #
  # Configure the sanity checks project. All CMakeLists.txt files defining the project
  # have been created while declaring public headers.
  #

  # Dirty trick to speed up cmake set up time.

  file(
    COPY "${CMAKE_BINARY_DIR}/CMakeFiles/${CMAKE_VERSION}"
    DESTINATION "${headers_check_base_dir}/CMakeFiles"
  )

  MESSAGE(STATUS "Configuring header checks using cmake generator: ${CMAKE_GENERATOR}")
  EXECUTE_PROCESS(
    COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
    WORKING_DIRECTORY ${headers_check_base_dir}
  )

endif()
ENDMACRO(ADD_HEADERS_TARGET)


MACRO(ADD_HEADERS_TEST)
if(WITH_HEADER_CHECKS)
    add_test(NAME Headers
      COMMAND ${CMAKE_COMMAND} --build . --clean-first
      WORKING_DIRECTORY ${headers_check_base_dir})
    message(STATUS "Added public headers test")
endif()
ENDMACRO()

#
# If public headers of the project use external headers and/or require some pre-processor
# definitions to work correctly then the santiy check project must define these macros
# and set required include paths. This can be done with HEADER_CHECKS_INCLUDE() and
# HEADER_CHECKS_DEFINITIONS() macros.
#


MACRO(HEADER_CHECKS_INCLUDE)
if(WITH_HEADER_CHECKS)

  FOREACH(dir ${ARGV})
    #message("headers (${headers_check_base_dir}): adding inc dir:${dir}")
    FILE(APPEND ${headers_check_base_dir}/CMakeLists.txt "INCLUDE_DIRECTORIES(\"${dir}\")\n")
  ENDFOREACH(dir)

endif()
ENDMACRO(HEADER_CHECKS_INCLUDE)


MACRO(HEADER_CHECKS_DEFINITIONS)
if(WITH_HEADER_CHECKS)

  FOREACH(def ${ARGV})
    FILE(APPEND ${headers_check_base_dir}/CMakeLists.txt "ADD_DEFINITIONS(${def})\n")
  ENDFOREACH(def)

endif()
ENDMACRO(HEADER_CHECKS_DEFINITIONS)

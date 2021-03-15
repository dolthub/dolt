# Copyright (c) 2008, 2018, Oracle and/or its affiliates. All rights reserved.
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

##########################################################################
#
#  Macros for declaring global configuration options for the build system.
#
#
# TODO: Consider implementation based on properties
# <https://cmake.org/cmake/help/v3.0/command/define_property.html>
#

if(COMMAND add_config_option)
  return()
endif()

set(CONFIG_OPTIONS "" CACHE INTERNAL "")

#
# add_config_option(OPT TYPE [ADVANCED] [DEFAULT val] documentation)
#

function(add_config_option OPT TYPE)

  list(FIND CONFIG_OPTIONS "${OPT}" idx)
  if(NOT idx LESS 0)
    message(
      "Skipping second declaration of config option: ${OPT}"
      " (found in: ${CMAKE_CURRENT_LIST_FILE})"
    )
    return()
  endif()

  list(APPEND CONFIG_OPTIONS ${OPT})
  set(CONFIG_OPTIONS ${CONFIG_OPTIONS} CACHE INTERNAL "")

  #message("-- new config ${TYPE} option: ${OPT} (${ARGN})")
  #message("--- declared in: ${CMAKE_CURRENT_LIST_FILE}")

  unset(default_value)
  unset(doc_string)
  unset(advanced)

  while(ARGN)

    list(GET ARGN 0 opt)
    list(REMOVE_AT ARGN 0)

    if(opt STREQUAL "DEFAULT")
      list(GET ARGN 0 default_value)
      list(REMOVE_AT ARGN 0)
      #message("--- default value: ${default_value}")
    elseif(opt STREQUAL "ADVANCED")
      set(${OPT}_ADVANCED YES CACHE INTERNAL "")
      #message("--- advanced")
    else()
      break()
    endif()

  endwhile()

  foreach(doc ${opt} ${ARGN})
    set(doc_string "${doc_string}${doc}")
  endforeach()

  if(default_value)
    set(doc_string "${doc_string} (default : ${default_value})")
  endif()

  #message("--- documentation: ${doc_string}")

  set(${OPT}_DOC "${doc_string}" CACHE INTERNAL "")

  if(NOT DEFINED ${OPT} AND DEFINED ENV{${OPT}})
    set(${OPT} $ENV{${OPT}})
  endif()

  if(NOT DEFINED ${OPT} AND DEFINED default_value)
    set(${OPT} ${default_value})
  endif()

  if(${TYPE} STREQUAL "PATH" AND "${${OPT}}" AND NOT EXISTS "${${OPT}}")
    message(FATAL_ERROR "Option ${OPT} set to path that does not exist: ${${OPT}}")
  endif()

  if(${TYPE} MATCHES "BOOL|BOOLEAN")
    set(TYPE "STRING")
  endif()

  if(DEFINED ${OPT})
    set(${OPT} "${${OPT}}" CACHE ${TYPE} ${doc_string})
  endif()

  if(advanced)
    mark_as_advanced(${OPT})
  endif()

endfunction(add_config_option)


#
# Print values of all declared (non-advanced) options. Should be called
# at the end of the project.
#

function(show_config_options)

  message("\nProject configuration options:\n")

  foreach(opt ${CONFIG_OPTIONS})

    if(NOT "${${opt}_ADVANCED}")
      message(": ${opt}: ${${opt}}")
      message("${${opt}_DOC}\n")
    endif()

  endforeach()

endfunction(show_config_options)

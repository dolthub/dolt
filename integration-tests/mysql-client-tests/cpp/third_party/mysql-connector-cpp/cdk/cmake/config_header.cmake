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
# Infrastructure for generating configuration header
# ==================================================
#
# Call this macro to add define to the configuration header based on
# a cmake variable.
#
#  ADD_CONNFIG(<var> [<val>])
#
# Like with #cmakedefine, the define will have the same name as cmake
# variable used and will be defined only if this variable is true (at
# the time of generating the configuration header). If value is provided,
# the variable is set to the given value.
#

set(CONFIG_VARS "" CACHE INTERNAL "configuration settings" FORCE)
set(CONFIG_VARS_VAL "" CACHE INTERNAL "configuration settings" FORCE)

function(ADD_CONFIG var)
  #message("- adding configuration setting: ${var} (${ARGN}, ${ARGV0}, ${ARGV1})")

  if(DEFINED ARGV1)

    set(${var} ${ARGV1} CACHE INTERNAL "config setting" FORCE)
    list(APPEND CONFIG_VARS_VAL ${var})
    list(REMOVE_DUPLICATES CONFIG_VARS_VAL)
    set(CONFIG_VARS_VAL ${CONFIG_VARS_VAL} CACHE INTERNAL "configuration settings" FORCE)

  else()

    list(APPEND CONFIG_VARS ${var})
    list(REMOVE_DUPLICATES CONFIG_VARS)
    set(CONFIG_VARS ${CONFIG_VARS} CACHE INTERNAL "configuration settings" FORCE)

  endif()

endfunction(ADD_CONFIG)

#
# Call this macro to write a configuration header containing defines
# declared with ADD_CONFIG() calls. The header is generated from config.h.in
# template with @GENERATED_CONFIG_DEFS@ replaced by previously declared
# defines.
#
#  WRITE_CONFIG_HEADER(<path relative to PROJECT_BINARY_DIR>)
#

macro(WRITE_CONFIG_HEADER path)

  set(GENERATED_CONFIG_DEFS)

  foreach(var ${CONFIG_VARS})
    if(${var})
      set(DEFINE "#define ${var}")
    else()
      set(DEFINE "/* #undef ${var} */")
    endif()
    #message("writing to config.h: ${DEFINE}")
    set(GENERATED_CONFIG_DEFS "${GENERATED_CONFIG_DEFS}\n${DEFINE}")
  endforeach()

  foreach(var ${CONFIG_VARS_VAL})

    set(DEFINE "#define ${var} ${${var}}")
    #message("writing to config.h: ${DEFINE}")
    set(GENERATED_CONFIG_DEFS "${GENERATED_CONFIG_DEFS}\n${DEFINE}")
  endforeach()

  configure_file(${PROJECT_SOURCE_DIR}/config.h.in ${path})
  message("Wrote configuration header: ${path}")

endmacro(WRITE_CONFIG_HEADER)





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
# Settings specific to MSVC compiler.
#

set(ARCH ${MSVC_CXX_ARCHITECTURE_ID} CACHE INTERNAL "architecture id")

#
# Set VS and VS_VER (MSVC toolset version)
#

if(DEFINED MSVC_TOOLSET_VERSION)
  string(REGEX REPLACE "^(..).*$" "\\1" VS ${MSVC_TOOLSET_VERSION})
else()

  #message("-- msvc version: ${MSVC_VERSION}")
  set(VS_18 12)
  set(VS_19 14)

  string(REGEX REPLACE "^(..).*$" "\\1" VS ${MSVC_VERSION})
  set(VS ${VS_${VS}})

endif()

#message("-- vs: ${VS}")

set(VS_VER ${VS} CACHE INTERNAL "")
set(VS     "vs${VS}" CACHE INTERNAL "")

#
# Commands for global compiler options.
#

function(enable_pic)
endfunction()

# C++11 is enabled by default for compilers that we support.

function(enable_cxx11)
endfunction()

# Note: Needs to be implemented if we ever want to change the default
# visibility of MSVC - for now we are happy with the default.

function(set_visibility)
endfunction()


function(set_msvcrt TYPE)

  if(TYPE MATCHES "^(STATIC|Static|static)$")
    set(flag "/MT")
  elseif(TYPE MATCHES "^(DYNAMIC|Dynamic|dynamic)$")
    set(flag "/MD")
  else()
    return()
  endif()

  foreach(LANG C CXX)

    set(CMAKE_${LANG}_FLAGS "${CMAKE_${LANG}_FLAGS} ${flag}" PARENT_SCOPE)

    foreach(TYPE RELEASE RELWITHDEBINFO MINSIZEREL)
      set(CMAKE_${LANG}_FLAGS_${TYPE}
        "${CMAKE_${LANG}_FLAGS_${TYPE}} ${flag}"
        PARENT_SCOPE
      )
    endforeach()

    set(CMAKE_${LANG}_FLAGS_DEBUG
      "${CMAKE_${LANG}_FLAGS_DEBUG} ${flag}d"
      PARENT_SCOPE
    )

  endforeach(LANG)

endfunction(set_msvcrt)

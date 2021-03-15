# Copyright (c) 2008, 2019, Oracle and/or its affiliates. All rights reserved.
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
# Determine basic os/platform settings.
#
# Sets the following global variables (CACHE INTERNAL):
#
#  ARCH
#  IS64BIT
#  UNIX/WIN32/SUNOS/MACOS  - if defined, set to OS version
#  MSVC/GCC/CLANG/SUNPRO   - if defined, set to compiler version
#  SPARC
#
# Defines the following commands:
#
#  enable_pic()
#  enable_cxx11()
#  set_arch(A)
#  set_visibility()
#

if(COMMAND platform_detection_completed)
 return()
endif()


if((CMAKE_SIZEOF_VOID_P EQUAL 8))
  set(IS64BIT TRUE)
else()
  set(IS64BIT FALSE)
endif()

set(IS64BIT ${IS64BIT} CACHE INTERNAL "")


include(TestBigEndian)
test_big_endian(BIG_ENDIAN)
set(BIG_ENDIAN ${BIG_ENDIAN} CACHE INTERNAL "")
message(STATUS "BIG_ENDIAN: ${BIG_ENDIAN}")


#######################################################################
#

if(APPLE)
  set(MACOS ${CMAKE_SYSTEM_VERSION})
endif()


if(CMAKE_SYSTEM_NAME MATCHES "SunOS")
  set(SUNOS ${CMAKE_SYSTEM_VERSION})
endif()

if(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  set(FREEBSD TRUE CACHE INTERNAL "")
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "sparc")
  set(SPARC TRUE CACHE INTERNAL "")
endif()


########################################################################
#
#  Default definitions for enable_xxx() commands that should work with
#  gcc toolchain. Below they are re-defined for other compilers, if needed.
#

macro(add_flags LANG)
  string(REPLACE ";" " " flags "${ARGN}")
  set(CMAKE_${LANG}_FLAGS "${flags} ${CMAKE_${LANG}_FLAGS}")
  set(CMAKE_${LANG}_FLAGS "${CMAKE_${LANG}_FLAGS}" PARENT_SCOPE)
endmacro()


# -----------------------------------------------------------------


if(CMAKE_VERSION VERSION_LESS "3.1.3")

function(enable_cxx11)
  add_flags(CXX -std=c++11)
endfunction()

else()

function(enable_cxx11)
  set(CMAKE_CXX_EXTENSIONS OFF PARENT_SCOPE)
  set(CMAKE_CXX_STANDARD 11 PARENT_SCOPE)
  set(CMAKE_CXX_STANDARD_REQUIRED ON PARENT_SCOPE)
endfunction()

endif()


# -----------------------------------------------------------------

#
# Note: cmake property POSITION_INDEPENDENT_CODE takes effect only
# for executable and shared library targets, but we want to also
# build static libraries under -fPIC since they are later merged into
# a shared library.
#

function(enable_pic)
  add_compile_options(-fPIC)
endfunction()


# TODO: Consider using CMAKE_CXX_VISIBILITY_PRESET instead

function(set_visibility)
  add_compile_options(-fvisibility=hidden)
endfunction()

# -----------------------------------------------------------------


function(set_arch_m64)

  add_flags(CXX -m64)
  add_flags(C -m64)

endfunction()


########################################################################
#
# Compiler specific settings.
#

if(MSVC)

  # VS_VER  -- MSVC toolchain version (14,15 etc)
  # VS      -- string "vsNN" where NN is toolchain version
  #
  # set_msvcrt(X) - X = static/dynamic

  set(MSVC ${CMAKE_CXX_COMPILER_VERSION} CACHE INTERNAL "")
  include(compiler/MSVC OPTIONAL)

elseif(CMAKE_CXX_COMPILER_ID MATCHES "SunPro")

  set(SUNPRO ${CMAKE_CXX_COMPILER_VERSION} CACHE INTERNAL "")
  include(compiler/SUNPRO OPTIONAL)

elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")

  set(CLANG ${CMAKE_CXX_COMPILER_VERSION} CACHE INTERNAL "")
  include(compiler/CLANG OPTIONAL)

else()

  if(CMAKE_COMPILER_IS_GNUCXX)
    set(GCC ${CMAKE_CXX_COMPILER_VERSION} CACHE INTERNAL "")
  endif()
  include(compiler/GCC OPTIONAL)

endif()


########################################################################

function(platform_detection_completed)
endfunction()


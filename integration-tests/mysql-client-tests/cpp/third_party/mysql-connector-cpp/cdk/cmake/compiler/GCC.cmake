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
# Ensure relative source locations in debug entries
#

option(DEBUG_PREFIX_MAP
  "Set -fdebug-prefix-map option to ensure relative source locations in debug entries."
  ON
)

if(DEBUG_PREFIX_MAP)

  foreach(LANG C CXX)
  foreach(TYPE DEBUG RELWITHDEBINFO)
    set(CMAKE_${LANG}_FLAGS_${TYPE}
      "-fdebug-prefix-map=${CMAKE_SOURCE_DIR}=. ${CMAKE_${LANG}_FLAGS_${TYPE}}"
    )
  endforeach(TYPE)
  endforeach(LANG)

endif(DEBUG_PREFIX_MAP)

#
# Deal with broken optimization in gcc 4.8.
#
# We observed very strange behaviour of exceptions when compiling
# fully optimized code wtih gcc 4.8. Downgrade optimization to -O1
# in this case. To get trully optimized code use gcc 4.9+ or clang.
#

if(GCC VERSION_LESS "4.9")
  foreach(LANG C CXX)
  foreach(TYPE RELEASE RELWITHDEBINFO)
    string(REPLACE "-O3" "-O1" CMAKE_${LANG}_FLAGS_${TYPE} "${CMAKE_${LANG}_FLAGS_${TYPE}}")
    string(REPLACE "-O2" "-O1" CMAKE_${LANG}_FLAGS_${TYPE} "${CMAKE_${LANG}_FLAGS_${TYPE}}")
  endforeach(TYPE)
  endforeach(LANG)
endif()


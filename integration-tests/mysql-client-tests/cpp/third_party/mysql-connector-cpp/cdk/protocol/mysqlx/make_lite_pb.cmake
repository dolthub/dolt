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

# Transform protobuf sources at ${src} into form appropriate for use with
# protobuf-lite. Transformed sources are written to ${tgt} location.

if(NOT EXISTS ${src})
  message(FATAL_ERROR "make_lite_pb: source location does not exist: ${src}")
endif()

if(NOT EXISTS ${tgt})
  message(FATAL_ERROR "make_lite_pb: target location does not exist: ${tgt}")
endif()


file(GLOB pb_defs "${src}/*.proto")

foreach(pb_def ${pb_defs})

  get_filename_component(name ${pb_def} NAME)
  #message("Generating lite version of protobuf source: ${name}")

  file(READ "${pb_def}" CONTENTS)
  unset(NEW_CONTENTS)

  FOREACH(LINE ${CONTENTS})
    STRING(REGEX REPLACE
      "([\\t ]*)//[\\t ]*ifdef[\\t ]+(PROTOBUF_LITE)[\\t ]*:[\\t ]*(.*)" "\\1\\3"
      LINE ${LINE}
    )
    LIST(APPEND NEW_CONTENTS "${LINE}")
  ENDFOREACH()

  STRING(REGEX REPLACE
    "//[\\t ]+ifndef[\\t ]+PROTOBUF_LITE.*//[\\t ]+endif[\\t ]*" ""
    NEW_CONTENTS "${NEW_CONTENTS}")

  STRING(REGEX REPLACE
    "(\r*\n)([^\r\n]*//[\\t ]+comment_out_if[\\t ]+PROTOBUF_LITE)" "\\1// \\2"
    NEW_CONTENTS "${NEW_CONTENTS}")

  file(WRITE "${tgt}/${name}" "${NEW_CONTENTS}")

endforeach()

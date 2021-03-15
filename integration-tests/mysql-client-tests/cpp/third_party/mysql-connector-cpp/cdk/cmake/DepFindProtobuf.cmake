# Copyright (c) 2009, 2019, Oracle and/or its affiliates. All rights reserved.
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

##############################################################################
#
# Targets:
#
# rebuild-protobuf
#
# Imported/alias targets:
#
# Protobuf::pb    - main library
# Protobuf::pb-lite
# Protobuf::pb-full
# Protobuf::protoc  -- compiler
#
# Commands:
#
# mysqlx_protobuf_generate_cpp()
#

if(TARGET Protobuf::pb)
  return()
endif()

message(STATUS "Setting up Protobuf.")

set(PB_SOURCE_DIR "${PROJECT_SOURCE_DIR}/protobuf")
set(PB_BINARY_DIR "${PROJECT_BINARY_DIR}/protobuf")
set(config_stamp "${PB_BINARY_DIR}/config.stamp")
set(build_stamp "${PB_BINARY_DIR}/build.stamp")


#
# Pick build configuration for the protobuf build. Normally we build using the
# same build configuration that is used for building CDK (Release/Debug/etc.).
# But we also support building CDK under non-standard build configuration
# named 'Static' (this is a dirty trick we use to simplify building our MSIs).
# Since protobuf does not know 'Static' build configuration, we build protobuf
# under 'Release' configuration in that case.
#
# We need to handle two cases. For some build systems, like Makefiles,
# the build configuration is specified at cmake time using CMAKE_BUILD_TYPE
# variable. In that case we also set it during protobuf build configuration.
# Another case is a multi-configuration build system like MSVC. In this case
# we use generator expression to pick correct  configuration when the build
# command is invoked below.
#

set(build_type)
if(CMAKE_BUILD_TYPE)
  if(CMAKE_BUILD_TYPE MATCHES "[Ss][Tt][Aa][Tt][Ii][Cc]")
    set(set_build_type -DCMAKE_BUILD_TYPE=Release)
  else()
    set(set_build_type -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})
  endif()
endif()

set(CONFIG_EXPR
  $<$<CONFIG:Static>:Release>$<$<NOT:$<CONFIG:Static>>:$<CONFIG>>
)

set(set_arch)
if(CMAKE_GENERATOR_PLATFORM)
  set(set_arch -A ${CMAKE_GENERATOR_PLATFORM})
endif()

set(set_toolset)
if(CMAKE_GENERATOR_TOOLSET)
  set(set_toolset -T ${CMAKE_GENERATOR_TOOLSET})
endif()

if(NOT EXISTS "${PB_BINARY_DIR}/exports.cmake")

  message("==== Configuring Protobuf build using cmake generator: ${CMAKE_GENERATOR} ${set_arch} ${set_toolset}")

  file(REMOVE "${PB_BINARY_DIR}/CMakeCache.txt")
  file(MAKE_DIRECTORY "${PB_BINARY_DIR}")

  # Dirty trick to speed up cmake set up time.
  #file(
  #  COPY "${CMAKE_BINARY_DIR}/CMakeFiles/${CMAKE_VERSION}"
  #  DESTINATION "${PB_BINARY_DIR}/CMakeFiles"
  #)

  execute_process(
    COMMAND ${CMAKE_COMMAND}
      -G "${CMAKE_GENERATOR}"
      ${set_arch}
      ${set_toolset}
      ${set_build_type}
      -DSTATIC_MSVCRT=${STATIC_MSVCRT}
      -DCMAKE_POSITION_INDEPENDENT_CODE=${CMAKE_POSITION_INDEPENDENT_CODE}
      -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
      -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
      -DCMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}
      -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE}
      -DCMAKE_CXX_FLAGS_RELWITHDEBINFO=${CMAKE_CXX_FLAGS_RELWITHDEBINFO}
      -DCMAKE_CXX_FLAGS_MINSIZEREL=${CMAKE_CXX_FLAGS_MINSIZEREL}
      -DCMAKE_STATIC_LINKER_FLAGS=${CMAKE_STATIC_LINKER_FLAGS}
      ${PB_SOURCE_DIR}
    WORKING_DIRECTORY ${PB_BINARY_DIR}
    RESULT_VARIABLE protobuf_config
  )

  if(protobuf_config)
    message(FATAL_ERROR "Could not configure Protobuf build: ${protobuf_config}")
  endif()

  message("==== Protobuf build configured.")

endif()


include(${PB_BINARY_DIR}/exports.cmake)

#
# Protobuf library targets imported above (pb_protobuf
# and pb_protobuf-lite) are local to the directory from which
# they were imported. This is not good if cdk is used as
# a sub-project of a parent project, because the parent project
# must have access to these targets.
#
# For that reason below we create global protobuf/protobuf-lite targets
# and copy their locations from the imported targets.
#
# Note: we can't use ALIAS library because it does not work with imported
# targets
#

add_library(Protobuf::pb-full STATIC IMPORTED GLOBAL)
add_library(Protobuf::pb-lite STATIC IMPORTED GLOBAL)
add_executable(Protobuf::protoc IMPORTED GLOBAL)

set(TGT_protobuf Protobuf::pb-full)
set(TGT_protobuf-lite Protobuf::pb-lite)
set(TGT_protoc Protobuf::protoc)

foreach(tgt protobuf protobuf-lite protoc)

  #message("processing: ${tgt}")

  set(loc_list)

  foreach(CONF NOCONFIG DEBUG RELEASE MINSIZEREL RELWITHDEBINFO)

    #message("- CONF: ${CONF}")

    get_target_property(LOC pb_${tgt} IMPORTED_LOCATION_${CONF})

    if(LOC)

      #message("- setting imported location to: ${LOC}")
      list(APPEND loc_list "${LOC}")

      set_target_properties(${TGT_${tgt}} PROPERTIES
        IMPORTED_LOCATION_${CONF} "${LOC}"
      )

      set_property(TARGET ${TGT_${tgt}} APPEND PROPERTY
        IMPORTED_CONFIGURATIONS ${CONF}
      )

    endif()

  endforeach(CONF)

  #
  # To support 'Static' build configuration the targets imported from the
  # Protobuf project need to have IMPORTED_LOCATION_STATIC defined. We use
  # 'Release' locations as Protobuf is built using 'Release' configuration in
  # that case.
  #

  get_target_property(LOC ${TGT_${tgt}} IMPORTED_LOCATION_RELEASE)
  set_property(TARGET ${TGT_${tgt}} PROPERTY IMPORTED_LOCATION_STATIC ${LOC})

endforeach(tgt)


#message("Protobuf include path: ${PROTOBUF_INCLUDE_DIR}")
set(PROTOBUF_INCLUDE_DIR ${PROTOBUF_INCLUDE_DIR} CACHE INTERNAL "")

set_target_properties(Protobuf::pb-lite PROPERTIES
  INTERFACE_INCLUDE_DIRECTORIES "${PROTOBUF_INCLUDE_DIR}"
)

set_target_properties(Protobuf::pb-full PROPERTIES
  INTERFACE_LINK_LIBRARIES Protobuf::pb-lite
)

# On UNIX Protobuf uses pthread library
if(UNIX)
  set_property(TARGET Protobuf::pb-lite APPEND PROPERTY
    INTERFACE_LINK_LIBRARIES pthread
  )
endif()

#
# Note: This is needed to correctly compile headers generated by protobuf
# with sunpro compiler.
#

if(SUNPRO)
  set_property(TARGET Protobuf::pb-lite APPEND PROPERTY
    INTERFACE_COMPILE_DEFINITIONS SOLARIS_64BIT_ENABLED
  )
endif()

#
# Try using parallel builds for protobuf.
#

include(ProcessorCount)
ProcessorCount(N)

MESSAGE("Processor Count: ${N}")

set(opt_build)
set(opt_tool)
if(NOT N EQUAL 0)
  if(NOT CMAKE_VERSION VERSION_LESS 3.12)
    set(opt_build --parallel ${N})
  elseif(CMAKE_MAKE_PROGRAM MATCHES "make")
    set(opt_tool -j${N})
  endif()
endif()

add_custom_command(OUTPUT "${build_stamp}"
  COMMAND ${CMAKE_COMMAND} --build . ${opt_build} --config ${CONFIG_EXPR} -- ${opt_tool}
  COMMAND ${CMAKE_COMMAND} -E touch "${build_stamp}"
  WORKING_DIRECTORY "${PB_BINARY_DIR}"
  COMMENT "Building protobuf"
)

add_custom_target(build_protobuf
  SOURCES "${build_stamp}"
)

set_target_properties(build_protobuf PROPERTIES FOLDER "Misc")

add_dependencies(Protobuf::pb-full build_protobuf)
add_dependencies(Protobuf::pb-lite build_protobuf)
add_dependencies(Protobuf::protoc  build_protobuf)

# TODO: Handle lite/full version



# Standard PROTOBUF_GENERATE_CPP modified to our usage

function(mysqlx_protobuf_generate_cpp SRCS HDRS)

  IF(NOT ARGN)
    MESSAGE(SEND_ERROR
      "Error: MYSQLX_PROTOBUF_GENERATE_CPP() called without any proto files")
    RETURN()
  ENDIF()

  SET(srcs)
  SET(hdrs)

  FOREACH(FIL ${ARGN})

    GET_FILENAME_COMPONENT(ABS_FIL ${FIL} ABSOLUTE)
    GET_FILENAME_COMPONENT(FIL_WE ${FIL} NAME_WE)
    GET_FILENAME_COMPONENT(ABS_PATH ${ABS_FIL} PATH)

    LIST(APPEND srcs "${CMAKE_CURRENT_BINARY_DIR}/protobuf/${FIL_WE}.pb.cc")
    LIST(APPEND hdrs "${CMAKE_CURRENT_BINARY_DIR}/protobuf/${FIL_WE}.pb.h")

    ADD_CUSTOM_COMMAND(
      OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/protobuf/${FIL_WE}.pb.cc"
             "${CMAKE_CURRENT_BINARY_DIR}/protobuf/${FIL_WE}.pb.h"
      COMMAND ${CMAKE_COMMAND}
            -E make_directory "${CMAKE_CURRENT_BINARY_DIR}/protobuf"
      COMMAND Protobuf::protoc
      ARGS --cpp_out "${CMAKE_CURRENT_BINARY_DIR}/protobuf"
           -I ${ABS_PATH} ${ABS_FIL}
           --proto_path=${PROJECT_SOURCE_DIR}/protobuf/protobuf-3.6.1/src
      DEPENDS ${ABS_FIL} #${PROTOBUF_PROTOC_EXECUTABLE}
      COMMENT "Running C++ protocol buffer compiler on ${FIL}"
      VERBATIM
    )

  ENDFOREACH()

  SET_SOURCE_FILES_PROPERTIES(
    ${srcs} ${hdrs}
    PROPERTIES GENERATED TRUE)

  #
  # Disable compile warnings in code generated by Protobuf
  #

  IF(UNIX)
    set_source_files_properties(${srcs}
      APPEND_STRING PROPERTY COMPILE_FLAGS "-w"
    )
  ELSE(WIN32)
    set_source_files_properties(${srcs}
      APPEND_STRING PROPERTY COMPILE_FLAGS
      "/W1 /wd4018 /wd4996 /wd4244 /wd4267"
    )
  ENDIF()


  SET(${SRCS} ${srcs} PARENT_SCOPE)
  SET(${HDRS} ${hdrs} PARENT_SCOPE)

endfunction(mysqlx_protobuf_generate_cpp)

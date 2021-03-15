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

##########################################################################
#
# Library utilities:
#
# merge_libraries()
#   Merge several static libraries into single static or shared library.
#   Includes all transitive dependencies of the libraries being merged.
#

# Include this script only once.

if(COMMAND libutils_setup)
  return()
endif()


macro(libutils_setup)

  get_filename_component(LIBUTILS_SCRIPT_DIR ${CMAKE_CURRENT_LIST_FILE} PATH)
  set(LIBUTILS_SCRIPT_DIR "${LIBUTILS_SCRIPT_DIR}/libutils")
  set(LIBUTILS_BIN_DIR "${CMAKE_CURRENT_BINARY_DIR}/libutils" CACHE INTERNAL "")

  #
  # Locate required tools.
  #

  if(CMAKE_BUILD_TOOL MATCHES "MSBuild")

    set(MSBUILD ON)

    # Use lib.exe from the same location as other compiler tools

    get_filename_component(path "${CMAKE_LINKER}" DIRECTORY)
    set(LIB_TOOL "${path}/lib.exe")

  endif()


  if(APPLE)

    find_program(LIB_TOOL libtool)

    # We need install_name_tool to do rpath mangling (see below)

    find_program(INSTALL_NAME_TOOL install_name_tool)

    # If available, otool is used to show runtime dependencies for libraries we
    # build

    find_program(OTOOL otool)

  endif()


  #
  # Infrastructure for merging static libraries
  # ===========================================
  #
  # It is used to merge a static library with all other static libraries
  # on which it depends, so that, when using the merged library, one does
  # not have to worry about dependencies.
  #
  # The main logic for mering static libraries on different platforms is
  # in the merge_archives.cmake script. Calling merge_static_library() on
  # a library target arranges for this script to be called with all required
  # parameters every time the library is (re-)built.
  #
  # Extra effort is needed to get the list of all dependencies of the library.
  # These dependencies are computed by cmake, but there is no easy way to
  # get them out of cmake. We use the trick with custom language linker. Hovewer,
  # it does not work with MSBuild generator where we do other tricks. In either
  # case the idea is to define a phony target that depends on the static library
  # and capture link options that cmake uses to build this phony target.
  #

  #
  # Create merge script from template, setting required internal variables in it.
  #

  configure_file(
    ${LIBUTILS_SCRIPT_DIR}/merge_archives.cmake.in
    ${LIBUTILS_BIN_DIR}/merge_archives.cmake
    @ONLY
  )

  #
  # This small program saves in a file all command line options that
  # were passed to it. It is used to capture linker invocation options.
  #

  if(NOT MSBUILD AND NOT TARGET save_linker_opts)
    add_executable(save_linker_opts ${LIBUTILS_SCRIPT_DIR}/save_linker_opts.cc)
    set_property(TARGET save_linker_opts PROPERTY
      RUNTIME_OUTPUT_DIRECTORY ${LIBUTILS_BIN_DIR}
    )
  endif()

endmacro(libutils_setup)

libutils_setup()

#
# Merge static libraries into a single static or shared library.
#
# Given a static library target, this function sets up an infrastructure
# for merging this static libraray with its dependencies. It creates
# new target "${TARGET}-merge" which runs the merge process. During merge,
# the original library file on disk ($<TARGET_FILE:${TARGET}>) is replaced
# by the merged library.
#
# Target dependencies are set up so that the -merge target is run only once
# after the original library is generated or re-generated. This is important
# because running it more than once would append the dependencies multiple
# times.
#

function(merge_libraries TARGET)

  list(GET ARGN 0 TYPE)

  if(TYPE MATCHES "STATIC|SHARED")
    list(REMOVE_AT ARGN 0)
  else()
    if(BUILD_SHARED_LIBS)
      set(TYPE SHARED)
    else()
      set(TYPE STATIC)
    endif()
  endif()

  message("Preparing to merge ${TYPE} library: ${TARGET} (${ARGN})")

  #  Note: empty.cc is not really needed, but cmake does not accept a library
  #  target with no sources.

  set_property(SOURCE "${LIBUTILS_SCRIPT_DIR}/empty.cc" PROPERTY LANGUAGE CXX)

  add_library(${TARGET} ${TYPE} "${LIBUTILS_SCRIPT_DIR}/empty.cc")
  target_link_libraries(${TARGET} PRIVATE ${ARGN})

  #
  # Arrange for marge_archives.cmake script to be executed in a POST_BUILD
  # event of the merged library target (however, this is not needed when
  # merging into DLL on Windows). This scripts does the job of merging
  # all static libraries into single library. The list of libraries to merge
  # is extracted from a build log file that is generated prior to invoking
  # this script (see below). A different build log is used when merging
  # into static or shared library, therefore it has corresponding extension
  # .STATIC or .SHARED.
  #
  # If INFO is set to a file path, external dependency information detected
  # by merge_archives script is appended to that file.
  #

  if(NOT MSBUILD OR NOT TYPE STREQUAL "SHARED")

    set(log_name "${TARGET}.log")
    set(log_file
      "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/${log_name}"
    )
    #message("-- build log: ${log_file}")

    add_custom_command(TARGET ${TARGET} POST_BUILD
      COMMAND ${CMAKE_COMMAND}
      -DTARGET=$<TARGET_FILE:${TARGET}>
      -DTYPE=${TYPE}
      -DBUILD_LOG=${log_file}.${TYPE}
      -DMSBUILD=${MSBUILD}
      -DINFO=${INFO}
      -DINFO_PREFIX=${INFO_PREFIX}
      -P ${LIBUTILS_BIN_DIR}/merge_archives.cmake
    )

  endif()

  #
  # The general way of detecting all dependencies is by building a helper
  # target ${TARGET}-deps which depends on the libraries that are to be merged.
  # When cmake builds this -deps target, it computes all transitive dependencies
  # and passes them to the linker. We intercept this information and store it
  # in a buld log file that is later processed by merge_archives script.
  #
  # The exact technique for storing linker invocation parameters into a file
  # differs for different build tools.
  #

  if(NOT MSBUILD)

    # TODO: Will it work with XCode?

    add_library(${TARGET}-deps SHARED EXCLUDE_FROM_ALL "${LIBUTILS_SCRIPT_DIR}/empty.cc")
    target_link_libraries(${TARGET}-deps ${ARGN})
    set_target_properties(${TARGET}-deps PROPERTIES FOLDER "Misc")

    #
    # We set RULE_LAUNCH_LINK property of the -deps target to intercept the
    # linker invocation line and save it in the log file. Helper program
    # save_linker_opts is used to save its invocation parameters into a file
    # (name of the file is passed as the first parameter).
    #

    add_dependencies(${TARGET}-deps save_linker_opts)
    set_target_properties(${TARGET}-deps PROPERTIES
      RULE_LAUNCH_LINK "${LIBUTILS_BIN_DIR}/save_linker_opts ${log_file}.STATIC "
    )

    # Arrange for ${TARGET}-deps to be built before ${TARGET}

    add_dependencies(${TARGET} ${TARGET}-deps)

    #
    # When merging into static library, we must use the -deps target to get
    # dependency info. Cmake computes all transitive dependencies only when
    # building an executable or a shared library, not for static libraries.
    # If we merge into shared library, we can directly intercept linker
    # invocation for that shared library and it gets stored in .SHARED build log.
    # For static library, the .SHARED build log is useless, but merge_archives
    # will use .STATIC build log generated from -deps target in this case.
    #

    set_target_properties(${TARGET} PROPERTIES
      RULE_LAUNCH_LINK "${LIBUTILS_BIN_DIR}/save_linker_opts ${log_file}.SHARED "
    )

  else(NOT MSBUILD)

    # TODO: macOS case

    if(TYPE STREQUAL "SHARED")

      #
      # If merging into DLL on windows, we do not use merge_archives script
      # (which will do nothing in that case) because MSVC linker does the
      # required job for us. However, we need to pass /wholearhive:${lib}
      # options to the linker to ensures that all symbols exported from
      # ${lib} will be included in the resulting DLL even if they are not
      # referenced anywhere.
      # See <https://docs.microsoft.com/en-us/cpp/build/reference/wholearchive-include-all-library-object-files?view=vs-2017>
      #
      # Note: For simplicity, we rely here on the fact that library name is
      # the same as cmake target name.
      #

      set(link_flags)
      foreach(lib ${ARGN})
        list(APPEND link_flags "/wholearchive:${lib}")
      endforeach()

      string(REPLACE ";" " " link_flags "${link_flags}")
      #message("-- additional link flags: ${link_flags}")

      set_property(TARGET ${TARGET} APPEND PROPERTY LINK_FLAGS ${link_flags})

      return()

    endif()

    #
    # Merging into static library on Windows is done by merge_archives script
    # and in this case we need the build log with dependency information.
    # We can not use RULE_LAUNCH_LINK trick because it does not work with
    # MSBuild tools. Instead, we invoke a build of a -deps target passing
    # options to MSBuild that tell it to generate file with linker invocation
    # lines.
    #

    add_library(${TARGET}-deps SHARED EXCLUDE_FROM_ALL "${LIBUTILS_SCRIPT_DIR}/empty.cc")
    set_target_properties(${TARGET}-deps PROPERTIES FOLDER "Misc")
    target_link_libraries(${TARGET}-deps ${ARGN})

    add_custom_command(TARGET ${TARGET} PRE_BUILD

      COMMAND ${CMAKE_COMMAND}
        --build .
        --target ${TARGET}-deps
        --config $<CONFIG>
        --
          /nologo /v:q /filelogger /flp:Verbosity=q /flp:ShowCommandLine
          /flp:LogFile=\"${log_file}.STATIC\"

      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      COMMENT "Extracting dependency info for target ${TARGET}"
    )

  endif(NOT MSBUILD)

endfunction(merge_libraries)


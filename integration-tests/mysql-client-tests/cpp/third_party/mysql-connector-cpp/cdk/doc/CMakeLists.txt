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
# Generate project documentation using Sphinx system.
# Assumes that Sphinx is installed and sphinx-build command
# is in the path.
#
# TODO: Detect Sphinx installation.
#

IF(NOT DEFINED WITH_DOC)
  OPTION(WITH_DOC "Build Project's documentation" ON)
ENDIF()

IF(WITH_DOC)
MESSAGE("Building CDK documentation")

find_package(Sphinx)
#MESSAGE("SPHINX_EXECUTABLE: ${SPHINX_EXECUTABLE}")

# configured documentation tools and intermediate build results
set(BINARY_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/_build")

# Sphinx cache with pickled ReST documents
set(SPHINX_CACHE_DIR "${CMAKE_CURRENT_BINARY_DIR}/_doctrees")

# HTML output directory
set(SPHINX_HTML_DIR "${CMAKE_CURRENT_BINARY_DIR}/html")


FILE(GLOB_RECURSE sources *.rst)
LIST(APPEND sources conf.py.in)


#
# If WITH_PLANTUML option is set then the default conf.py is overwritten
# with one generated from conf.py.in, with contents taking into account
# the setting.
#

SET(WITH_PLANTUML $ENV{WITH_PLANTUML} CACHE PATH "Folder where plantuml.jar is stored")

IF(WITH_PLANTUML)

  SET(PLANTUML_JAR "${WITH_PLANTUML}/plantuml.jar")

  IF(NOT EXISTS ${PLANTUML_JAR})

    IF (SPHINX_EXECUTABLE)
      MESSAGE(WARNING "Could not find plantuml at this location: ${PLANTUML_JAR}")
    ENDIF()
    SET(PLANTUML_COMMAND)

  ELSE()

    LIST(APPEND SPHINX_EXTENSIONS "'sphinxcontrib.plantuml'")
    LIST(APPEND plantuml_opts "-Dplantuml.include.path=\"${CMAKE_CURRENT_SOURCE_DIR}/\"")
    SET(PLANTUML_COMMAND "java ${plantuml_opts} -jar ${PLANTUML_JAR}")

  ENDIF()


  #
  #  sphinx-build has -c <config dir> option which allows storing
  #  conf.py in a different location than documentation sources
  #  (see http://sphinx-doc.org/invocation.html#cmdoption-sphinx-build-c)
  #
  #  When I (rafal) used it on Windows, I got nasty exceptions from
  #  sphinx. Since I could not solve it, for the moment I generate
  #  conf.py in the source tree (and tell git to ignore it).
  #
  #  Variables used in conf.py.in:
  #
  #  SPHINX_EXTENSIONS
  #  PLANTUML_COMMAND
  #

  #MESSAGE("SPHINX_EXTENSIONS: ${SPHINX_EXTENSIONS}")
  #MESSAGE("PLANTUML_COMMAND: ${PLANTUML_COMMAND}")
  CONFIGURE_FILE(conf.py.in ${CMAKE_CURRENT_SOURCE_DIR}/conf.py @ONLY)

ENDIF(WITH_PLANTUML)


#
# Target for building documentation using Sphinx
#

#LIST(APPEND sphinxopts -c "${CMAKE_CURRENT_BINARY_DIR}/")      # location of conf.py
LIST(APPEND sphinxopts -d ${SPHINX_CACHE_DIR}) # location of index caches

#MESSAGE("sphinxopts: ${sphinxopts}")
ADD_CUSTOM_TARGET(build_docs
  COMMAND ${SPHINX_EXECUTABLE} -b html ${sphinxopts}
          ${CMAKE_CURRENT_SOURCE_DIR}
          ${SPHINX_HTML_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  COMMENT "Building project documentation"
  SOURCES ${sources}
)

ENDIF(WITH_DOC)

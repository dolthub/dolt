/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0, as
 * published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an
 * additional permission to link the program and your derivative works
 * with the separately licensed software that they have included with
 * MySQL.
 *
 * Without limiting anything contained in the foregoing, this file,
 * which is part of MySQL Connector/C++, is also subject to the
 * Universal FOSS Exception, version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#ifndef MYSQLX_COMMON_API_H
#define MYSQLX_COMMON_API_H


/*
  X DevAPI ABI version and revision
  =================================

  All public symbols inside mysqlx namespace should be defined inside
  MYSQLX_ABI_BEGIN(X,Y) ... MYSQLX_ABI_END(X,Y) block, where X.Y is the
  ABI version of the symbol. This puts the symbol inside mysqlx::abiX::rY
  namespace.

  The current ABI version is determined by MYSQLX_ABI_X_Y macros below. Using
  inline namespace ensures that symbol reference mysqlx::foo resolves
  to mysqlx::abiX::rY::foo, where X.Y is the current ABI version.

  Declarations below ensure, that each ABI revision namespace includes all
  symbols from previous revisions (via using namespace declaration).

  If the same symbol is defined for several revisions of the ABI, the latest
  one will shadow other definitions but earlier revisions will be also present
  to be used by old code. This way backward ABI compatibility can be maintained.
*/

/*
  When new ABI version or revision is added, the corresponding
  MYSQLX_ABI_MAJOR/MINOR_X macro needs to be added below. The macros for the
  latest ABI version and revision should expand to "inline", other MSQLX_ABI_*
  macros should have empty expansion. For example,
  after adding revision ABI 2.1 these macros should look as follows:

    #define MYSQLX_ABI_MAJOR_2  inline  // current ABI version
    #define MYSQLX_ABI_MINOR_0
    #define MYSQLX_ABI_MINOR_1  inline  // current ABI revision

  TODO: Auto-generate this based on information in version.cmake
*/

#define MYSQLX_ABI_MAJOR_2  inline     // current ABI version
#define MYSQLX_ABI_MINOR_0  inline     // current ABI revision


#define MYSQLX_ABI_BEGIN(X,Y) \
  MYSQLX_ABI_MAJOR_ ## X namespace abi ## X { \
  MYSQLX_ABI_MINOR_ ## Y namespace r ## Y {

#define MYSQLX_ABI_END(X,Y)   }}

#define MYSQLX_ABI(X,Y) mysqlx::abi##X::r##Y

#ifdef __cplusplus

namespace mysqlx {


MYSQLX_ABI_BEGIN(2,0)

  namespace internal {
  }

  namespace common {
  }

MYSQLX_ABI_END(2,0)

/*
  When new revision 1 of the current ABI 2 is added, the following declarations
  should be added. They import all revision 0 symbols into revision 1. Symbols
  that have changed should be defined inside
  MYSQLX_ABI_BEGIN(2,1) ... MYSQLX_ABI_END(2,1) and they will
  shadow the corresponding revision 0 symbol.

    MYSQLX_ABI_BEGIN(2,1)

    using namespace r0;

    namespace internal {
      using namespace r0::internal;
    }

    namespace common {
      using namespace r0::common;
    }

    MYSQLX_ABI_END(2,1)
*/

}

#endif  //  __cplusplus


/*
  Macros for declaring public API
  ===============================

  API function declarations should be decorated with PUBLIC_API prefix. API
  classes should have PUBLIC_API marker between the "class" keyword and
  the class name.

  See: https://gcc.gnu.org/wiki/Visibility

  TODO: Use better name than PUBLIC_API - not all public API classes should
  be decorated with these declarations but only these whose implementation
  is inside the library (so, not the ones which are implemented in headers).
*/

#if defined _MSC_VER

 #define DLL_EXPORT __declspec(dllexport)
 #define DLL_IMPORT __declspec(dllimport)
 #define DLL_LOCAL

#elif __GNUC__ >= 4

 #define DLL_EXPORT __attribute__ ((visibility ("default")))
 #define DLL_IMPORT
 #define DLL_LOCAL  __attribute__ ((visibility ("hidden")))

#elif defined __SUNPRO_CC || defined __SUNPRO_C

 #define DLL_EXPORT __global
 #define DLL_IMPORT __global
 #define DLL_LOCAL  __hidden

#else

 #define DLL_EXPORT
 #define DLL_IMPORT
 #define DLL_LOCAL

#endif


#if defined CONCPP_BUILD_SHARED
  #define PUBLIC_API  DLL_EXPORT
  #define INTERNAL    DLL_LOCAL
#elif defined CONCPP_BUILD_STATIC
  #define PUBLIC_API
  #define INTERNAL
#elif !defined STATIC_CONCPP
  #define PUBLIC_API  DLL_IMPORT
  #define INTERNAL
#else
  #define PUBLIC_API
  #define INTERNAL
#endif


#endif

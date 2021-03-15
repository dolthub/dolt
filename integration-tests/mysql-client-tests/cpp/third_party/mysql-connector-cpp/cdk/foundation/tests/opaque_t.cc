/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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
 * which is part of <MySQL Product>, is also subject to the
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

/**
  Unit tests for opaque implementation infrastructure.
*/

#include "test.h"
#include "opaque_t.h"
#include <iostream>


using namespace ::std;


/*
  Class deriving from Y with hidden implementation
  (defined in separate translation uint opaque_t_impl.cc)
*/

class U : public Y
{
public:

  U() : Y(5)
  {}

  void foo()
  {
    cout <<"U: ";
    Y::foo();
  }
};



/*
  Implementation of class Z (to have it in a different
  translation unit). Copy constructor for Z is generated
  by compiler. Default constructor is defined explicitly.
*/


#include <mysql/cdk/foundation/opaque_impl.i>

using namespace cdk::foundation;

struct Z_impl
{
  void foo()
  {
    cout <<"Z: foo()" <<endl;
  }
};

IMPL_TYPE(Z, Z_impl);
IMPL_DEFAULTCOPY(Z);


void Z::foo() { get_impl().foo(); }

Z::Z()
{
  cout <<"Z: default constructor" <<endl;
}



TEST(Opaque, basic)
{
  X x(7);
  x.foo();
  Y y(x);
  y.foo();
  Z z;
  Z zz(z);
  zz.foo();
  U u;
  u.foo();
  cout <<"Done!" <<endl;
}




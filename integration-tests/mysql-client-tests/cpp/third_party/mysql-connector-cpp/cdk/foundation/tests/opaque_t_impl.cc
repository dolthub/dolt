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

#include "test.h"
#include "opaque_t.h"
#include <mysql/cdk/foundation/opaque_impl.i>
#include <iostream>

/*
  Implementation of test classes declared in opaque_t.h
*/


using namespace cdk::foundation;
using namespace std;

/*
  Implementation class for X and Y. This class does not
  have default constructor (and compiler won't generate
  one, because there are other explicit constructors).
*/

class Impl
{
  int m_val;
public:

  Impl(int val) : m_val(val)
  { cout <<"Impl created: " <<val <<endl; }

  Impl(const Impl &i) : m_val(10*i.m_val)
  {
    cout <<"Impl copied: " <<i.m_val <<endl;
  }

  ~Impl()
  { cout <<"Impl destroyed" <<endl; }

  const char* foo() const
  { return "Impl::foo()"; }

  int bar() const
  { return m_val; }
};


// Implement X using Impl

IMPL_TYPE(X, Impl);
IMPL_PLAIN(X);


X::X(int x) : opaque_impl<X>(NULL, x)
{}

void X::foo()
{
  Impl &i= get_impl();
  cout <<"X: " <<i.foo() <<": " <<i.bar() <<endl;
}

int X::bar() const
{
  return get_impl().bar();
}

/*
  Implement Y using Impl. We want Y to be constructible
  from X and for this we need to copy internal implementation of
  X to become the implementation of new Y instance.

  Thus we need a "copy: constructor for opaque_impl<Y> which accepts
  internal implementation reference as input argument. This copy
  constructor is declared with IMPL_COPY(Y).
*/

IMPL_TYPE(Y, Impl);
IMPL_COPY(Y);


Y::Y(int x) : opaque_impl<Y>(NULL, x)
{
  cout <<"Y constructed: " <<x <<endl;
}

Y::Y(const X &x) : opaque_impl<Y>((Impl&)x.get_impl())
{
  cout <<"Y constructed from X: " <<x.bar() <<endl;
}

void Y::foo()
{
  Impl &i= get_impl();
  cout <<"Y: " <<i.foo() <<": " <<i.bar() <<endl;
}





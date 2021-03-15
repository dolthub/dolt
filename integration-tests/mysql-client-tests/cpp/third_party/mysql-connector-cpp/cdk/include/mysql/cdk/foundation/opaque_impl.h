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

#ifndef CDK_FOUNDATION_OPAQUE_IMPL_H
#define CDK_FOUNDATION_OPAQUE_IMPL_H

/*
  Infrastructure for hiding implementation details from public headers
  ====================================================================

  The idea is that implementation details of a class X that is part of public
  API are not declared in public headers together with X class. Instead, class X
  holds an opaque pointer to object holding implementation details. This internal
  implementation object is defined in .cc files that implement class X, not in
  public headers.

  For example, if class Socket, declared in socket.h, implements public
  interface Connection then all the details of this implementation need not
  to be declared in socket.h. Instead, class Socket can be declared like this:

    #include <opaque_impl.h>

    class Socket: public api::Connection,
                  opaque_impl<Socket>
    {
      // Declare only public interface methods without any implementation
      // details.
    }

  Base class opaque_impl<Socket> takes care of creating, destroying and maintaining
  a pointer to internal implementation object which is defined together with Socket
  class in the corresponding .cc file (say, socket.cc). Definition of this internal
  implementation object is not public.

  Suppose that internal implementation of Socket is defined in socket.cc as
  class Socket_impl. First, socket.cc must #include <opaque_impl.i> - note the
  .i extension, not .h. This file defines templates for opaque_impl<Socket>
  constructors and destructors.

  Then somewhere in socket.cc, the following declaration must be placed:

    IMPL_TYPE(Socket, Socket_impl)

  This macro declares internal implementation type for Socket by creating instance
  of impl_traits<Socket> template. Once this is done, methods of Socket class
  can use the internal implementation object via get_impl() method which returns
  reference to it:

    void Socket::foo()
    {
      Socket_impl &impl= get_impl();

      if (impl.use_bar)
        impl.bar();
      else
        impl.baz();
    }

  Note that to use get_impl() method one has to declare implementation type
  with IMPL_TYPE() macro first. This declaration can be placed at the beginning
  of the .cc file that implements Socket methods, or it can be put in a private
  header if implementation of Socket is spread among many compilation units.
  Note however, that IMPL_TYPE() declaration should not be put in any public
  headers - the whole point of using opaque_impl<> template is to hide the exact
  type of the implementation.

  The opaque_impl<Socket> base class takes care of constructing and destructing
  the internal implementation. Arguments for internal implementation constructor
  can be passed from Socket constructor as follows:

    Socket(int x) : opaque_impl<Socket>(NULL, x)
    {
      ...
    }

  This will pas argument x to the constructor of internal implementation. A phony
  NULL argument is  used to distinguish this kind of constructor from copy
  constructors that also take one parameter.

  A final step when defining a class which uses opaque_impl<> template is to
  add to the code definitions for opaque_impl<Socket> specialization of the
  generic template. This is done using one of the following macros:

    IMPL_PLAIN(Socket)
    IMPL_DEFAULT(Socket)
    IMPL_COPY(Socket)
    IMPL_DEFAULTCOPY(Socket)

  These macros differ in handling of special default/copy constructors of the
  implementation type. The PLAIN variant will not handle any special constructors.
  One has to construct implementation by passing arguments to some explicit
  constructor. Also, in this case neither default nor copy constructor can be
  generated by compiler for the final Socket class.

  Variants DEFAULT and COPY provide support for default and copy constructor,
  respectively. If implementation type has the corresponding constructor, then
  the same type of constructor will be generated for the final Socket class, if
  needed. Variant DEFAULTCOPY supports both default and copy constructors.

  Note that IMPL_PLAIN/DEFAULT/COPY() macro should be put in exactly one
  compilation unit - otherwise linker will report duplicate symbols. Thus,
  it is not a good idea to put these macros in a header file. This is different
  from implementation type declaration IMPL_TYPE, which can be put in an
  (internal) header to be included by several compilation units. The location
  of IMPL_PLAIN/DEFAULT/COPY() macros within source file is not relevant, except
  that they should be put outside of any namespaces.

  See declaration and definition of Mem_stream_base class in foundation/stream.{h,cc}
  for example of using this infrastructure.
*/


namespace cdk {
namespace foundation {


/*
  Template impl_traits<X> is specialized for each X to define the type of
  internal implementation used by class X. This internal implementation type
  is given by impl_traits<X>::impl_type.

  Concrete specializations of impl_traits<> are created by IMPL_TYPE() macro.
*/

template <class X>
struct impl_traits;


/*
  Base for classes that want to hide their implementation. This class handles
  creation and destruction of internal implementation object. A reference to
  the internal implementation can be obtained with get_impl() method.

  Since we do not know the exact type of implementation but we need to define
  return type of get_impl() method, we introduce _Impl wrapper around the actual
  implementation type (see opaque_impl.i for definition).
*/

template <class X>
class opaque_impl
{
  struct _Impl;
  _Impl  *m_impl;

protected:

  virtual ~opaque_impl();

  // Default constructor: uses default constructor of internal implementation type.

  opaque_impl();

  /*
    Copy constructors:

    - First is a standard copy constructor for opaque_impl<> class. It copies
      internal implementation object from the source using implementation's type
      copy constructor.

    - Second copy constructor constructs opaque_impl<> directly from a given
      internal implementation object that is copied. Since we do not know the exact
      type of implementation, this constructor is templated by type I. However,
      only the variant with correct type will compile correctly: template checks that
      the type is as expected - see opaque_impl.i).
  */

  opaque_impl(const opaque_impl&);
  template <class I>
  opaque_impl(const I&);

  /*
    Constructors that create internal implementation object passing arguments to
    its constructor.

    To distinguish these constructors from copy ones, an extra parameter of type
    void* is added. This parameter is ignored and can be always set to NULL.
  */

  template <typename A>
  opaque_impl(void*, A);
  template <typename A, typename B>
  opaque_impl(void*, A, B);
  template <typename A, typename B, typename C>
  opaque_impl(void*, A, B, C);

  /*
    Method to get the internal implementation. Type _Impl is a wrapper around
    the concrete implementation type I. The result of get_impl() will be transparently
    converted to to I&.
  */

  _Impl& get_impl() const;
};


}}  // cdk::foundation


#endif

/*
 * Copyright (c) 2017, 2018, Oracle and/or its affiliates. All rights reserved.
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

#ifndef SDK_FOUNDATION_VARIANT_H
#define SDK_FOUNDATION_VARIANT_H

/*
  Minimal implementation of variant<> type template.

  Note: Eventually this implementation should be replaced by std::variant<>.
*/

#include "common.h"

PUSH_SYS_WARNINGS_CDK
#include <type_traits> // std::aligned_storage
#include <typeinfo>
#include <new>
POP_SYS_WARNINGS_CDK


/*
  Note: MSVC 14 does not have certain C++11 constructs that are needed
  here.
*/

#if defined(_MSC_VER) && _MSC_VER < 1900
  #define CDK_CONSTEXPR   const
  #define CDK_ALIGNOF(T)  (std::alignment_of<T>::value)
#else
  #define CDK_CONSTEXPR   constexpr
  #define CDK_ALIGNOF(T)  alignof(T)
#endif



namespace cdk {
namespace foundation {


namespace detail {

template <size_t A, size_t B>
struct max
{
  static CDK_CONSTEXPR size_t value = A > B ? A : B;
};


template <
  size_t Size, size_t Align,
  typename... Types
>
class variant_base;

template <
  size_t Size, size_t Align,
  typename First,
  typename... Rest
>
class variant_base<Size, Align, First, Rest...>
  : public variant_base<
      detail::max<Size, sizeof(First)>::value,
      detail::max<Align, CDK_ALIGNOF(First)>::value,
      Rest...
    >
{
  typedef variant_base<
    detail::max<Size, sizeof(First)>::value,
    detail::max<Align, CDK_ALIGNOF(First)>::value,
    Rest...
  >  Base;

  bool m_owns = false;

protected:

  using Base::m_storage;

  /*
    Default ctor. It constructs an instance which does not hold any value.
  */

  variant_base() = default;

  // Copy/move semantics

  variant_base(const variant_base &other)
    : Base(static_cast<const Base&>(other))
  {
    if (other.m_owns)
      set(*other.get((First*)nullptr));
  }

  variant_base(variant_base &&other)
    : Base(static_cast<Base&&>(other))
  {
    if (other.m_owns)
    {
      /*
        Note: Method other.get() returns const First* pointer. Without
        casting away constness, method set() would be called with argument
        of type const First&& and would not correctly assign the value.
        After casting away the constness, the value passed to set() has
        the expected type First&&.
      */
      set(std::move(*const_cast<First*>(other.get((First*)nullptr))));
    }
    other.m_owns = false;
  }


  /*
    Construct variant from a value of one of the compatible types.

    The logic is handled by set() method.
  */

  template<typename T>
  variant_base(T &&val)
  {
    set(std::forward<T>(val));
  }

  // Copy assignment.

  variant_base& operator=(const variant_base &other)
  {
    if (other.m_owns)
      set(*other.get((First*)nullptr));
    else
      Base::operator=(static_cast<const Base&>(other));
    return *this;
  }

  /*
    Method set(), depending on the type of the value, either stores it
    in this instance or in the base class.
  */

  void set(const First &val)
  {
    m_owns = true;
    new (&m_storage) First(val);
  }

  void set(First&& val)
  {
    m_owns = true;
    new (&m_storage) First(std::move(val));
  }

  template <typename T>
  void set(T&& val)
  {
    m_owns = false;
    Base::set(std::forward<T>(val));
  }


  const First* get(const First*) const
  {
    if (!m_owns)
      throw std::bad_cast();

    return reinterpret_cast<const First*>(&m_storage);
  }

  template <typename T>
  const T* get(const T *ptr) const
  {
    return Base::get(ptr);
  }

  template <class Visitor>
  void visit(Visitor& vis) const
  {
    if (m_owns)
      vis(*reinterpret_cast<const First*>(&m_storage));
    else
      Base::visit(vis);
  }

  void destroy()
  {
    if (m_owns)
    {
      reinterpret_cast<First*>(&m_storage)->~First();
      m_owns = false;
    }
    else
      Base::destroy();
  }

  operator bool()
  {
    if (m_owns)
      return true;
    return Base::operator bool();
  }

};


template <size_t Size, size_t Align>
class variant_base<Size, Align>
{
protected:

  typedef typename std::aligned_storage<Size, Align>::type storage_t;

  storage_t m_storage;

  variant_base() {}

  variant_base(const variant_base&) {}
  variant_base(variant_base &&) {}

  void destroy() {}

  operator bool()
  {
    return false;
  }

  template <class Visitor>
  void visit(Visitor&) const
  {
    /*
      Note: This assertion is hit when visit() method is called on
      a variant object which does not store any data.
    */
    assert(false);
  }

  void operator=(const variant_base&)
  {}

#if defined _MSC_VER

  /*
    The static asserts below are handy for debugging compile-time issues
    with variant<> template usage. But they work only for MSVC which apparently
    does not look at methods which are not needed in the final code. Gcc works
    differently and triggers the asserts even though methods are not actually
    instantiated. So, for GCC we simply do not include these method definitions
    so that compilation would fail (with a more criptic error message) if wrong
    variant usage would require them to exist.
  */

  template<typename T>
  void set(T &&)
  {
    static_assert(false,
      "Trying to set a variant object to an incompatible type"
    );
  }

  template<typename T>
  void operator=(const T&)
  {
    static_assert(false,
      "Trying to set a variant object to an incompatible type"
    );
  }

#endif

};

}  // detail


template <
  typename... Types
>
class variant
  : private detail::variant_base<0,0,Types...>
{
  typedef detail::variant_base<0,0,Types...> Base;

protected:

  template <typename T>
  const T* get_ptr() const
  {
    return Base::get((const T*)nullptr);
  }

public:

  variant() {}

  variant(const variant &other)
    : Base(static_cast<const Base&>(other))
  {}

  variant(variant &&other)
    : Base(std::move(static_cast<Base&&>(other)))
  {}

  template <typename T>
  variant(const T &val)
    : Base(val)
  {}

  template <typename T>
  variant(T &&val)
    : Base(std::move(val))
  {}

  variant& operator=(const variant &other)
  {
    Base::operator=(static_cast<const Base&>(other));
    return *this;
  }

  template <typename T>
  variant& operator=(T&& val)
  {
    Base::set(std::move(val));
    return *this;
  }

  template <typename T>
  variant& operator=(const T& val)
  {
    Base::set(val);
    return *this;
  }

  ~variant()
  {
    Base::destroy();
  }

  operator bool()
  {
    return Base::operator bool();
  }

  template <class Visitor>
  void visit(Visitor& vis) const
  {
    Base::visit(vis);
  }

  template <typename T>
  const T& get() const
  {
    return *Base::get((const T*)nullptr);
  }

  template <typename T>
  const T* operator->() const
  {
    return get_ptr<T>();
  }
};


/*
  Object x of type opt<T> can be either empty or hold value of type T. In
  the latter case reference to stored object can be obtained with x.get()
  and stored object's methods can be called via operator->: x->method().
*/

template < typename Type >
class opt
  : private variant<Type>
{
  typedef variant<Type> Base;

public:

  opt()
  {}

  opt(const opt &other)
    : Base(static_cast<const Base&>(other))
  {}

  template <typename... T>
  opt(T... args)
    : Base(Type(args...))
  {}

  opt& operator=(const opt &other)
  {
    Base::operator=(static_cast<const Base&>(other));
    return *this;
  }

  opt& operator=(const Type& val)
  {
    Base::operator=(val);
    return *this;
  }

  using Base::operator bool;
  using Base::get;

  const Type* operator->() const
  {
    return Base::template get_ptr<Type>();
  }
};


}}  // cdk::foundation

#endif

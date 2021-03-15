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

#ifndef MYSQLX_COMMON_H
#define MYSQLX_COMMON_H


#include "../common.h"

PUSH_SYS_WARNINGS
#include <string>
#include <stdexcept>
#include <ostream>
#include <memory>
#include <forward_list>
#include <string.h>  // for memcpy
#include <utility>   // std::move etc
POP_SYS_WARNINGS


#define CATCH_AND_WRAP \
  catch (const ::mysqlx::Error&) { throw; }       \
  catch (const std::out_of_range&) { throw; }     \
  catch (const std::exception &e)                 \
  { throw ::mysqlx::Error(e.what()); }            \
  catch (const char *e)                           \
  { throw ::mysqlx::Error(e); }                   \
  catch (...)                                     \
  { throw ::mysqlx::Error("Unknown exception"); } \


namespace mysqlx {
MYSQLX_ABI_BEGIN(2,0)

using std::out_of_range;

using common::byte;
class Value;


/**
  Base class for connector errors.

  @internal
  TODO: Derive from std::system_error and introduce proper
  error codes.
  @endinternal

  @ingroup devapi
*/

// TODO: Make it header-only class somehow...

DLL_WARNINGS_PUSH

class PUBLIC_API Error : public common::Error
{

  DLL_WARNINGS_POP

public:

  Error(const char *msg)
    : common::Error(msg)
  {}
};


inline
void throw_error(const char *msg)
{
  throw Error(msg);
}


/**
  A wrapper around std::wstring that can perform
  conversions from/to different character encodings
  used by MySQL.

  Currently only utf-8 encoding is supported.

  @ingroup devapi_aux
*/

class string : public std::u16string
{

  struct Impl
  {
    PUBLIC_API static std::string to_utf8(const string&);
    PUBLIC_API static void from_utf8(string&, const std::string&);

    PUBLIC_API static std::u32string to_ucs4(const string&);
    PUBLIC_API static void from_ucs4(string&, const std::u32string&);

    PUBLIC_API static std::wstring to_wide(const string&);
    PUBLIC_API static void from_wide(string&, const std::wstring&);
  };

  template <typename T>
  struct traits
  {};


public:

  string() {}
  string(const string&) = default;
  string(string&&) = default;

  string& operator=(const string&) = default;
  string& operator=(string&&) = default;

  using std::u16string::basic_string;

  string(const std::u16string &other) : std::u16string(other) {}
  string(std::u16string &&other) : std::u16string(std::move(other)) {}

  template <typename C>
  string(const C *other)
  {
    try {
      if (!other)
        return;
      std::basic_string<C> str(other);
      traits<C>::from_str(*this, str);
    }
    CATCH_AND_WRAP
  }

  template <typename C>
  string(const std::basic_string<C> &other)
  {
    try {
      traits<C>::from_str(*this, other);
    }
    CATCH_AND_WRAP
  }

  template <typename C>
  operator std::basic_string<C>() const
  {
    try {
      return traits<C>::to_str(*this);
    }
    CATCH_AND_WRAP
  }


  friend bool operator==(const string&lhs, const string&rhs)
  {
    return operator==((const std::u16string&)lhs, (const std::u16string&)rhs);
  }

  friend bool operator!=(const string&lhs, const string&rhs)
  {
    return !(lhs == rhs);
  }

  // Note: These are needed to help overload resolution :/

  friend bool operator==(const string &lhs, const char16_t *rhs)
  {
    return lhs == string(rhs);
  }

  friend bool operator==(const char16_t *lhs, const string &rhs)
  {
    return string(lhs) == rhs;
  }

  friend bool operator!=(const string &lhs, const char16_t *rhs)
  {
    return !(lhs == rhs);
  }

  friend bool operator!=(const char16_t *lhs, const string &rhs)
  {
    return !(lhs == rhs);
  }

};


template<>
struct string::traits<char>
{
  using string = std::string;

  static void from_str(mysqlx::string &to, const string &from)
  {
    Impl::from_utf8(to, from);
  }

  static string to_str(const mysqlx::string &from)
  {
    return Impl::to_utf8(from);
  }
};

template<>
struct string::traits<wchar_t>
{
  using string = std::wstring;

  static void from_str(mysqlx::string &to, const string &from)
  {
    Impl::from_wide(to, from);
  }

  static string to_str(const mysqlx::string &from)
  {
    return Impl::to_wide(from);
  }
};

template<>
struct string::traits<char32_t>
{
  using string = std::u32string;

  static void from_str(mysqlx::string &to, const string &from)
  {
    Impl::from_ucs4(to, from);
  }

  static string to_str(const mysqlx::string &from)
  {
    return Impl::to_ucs4(from);
  }
};


inline
std::ostream& operator<<(std::ostream &out, const string &str)
{
  const std::string utf8(str);
  out << utf8;
  return out;
}


typedef unsigned long col_count_t;
typedef unsigned long row_count_t;


/**
  Class representing a region of memory holding raw bytes.

  Method `begin()` returns pointer to the first byte in the
  region, `end()` to one past the last byte in the region.

  @note An instance of `bytes` does not store the bytes -
  it merely describes a region of memory and is equivalent
  to a pair of pointers. It is very cheap to copy `bytes` and
  pass them by value.

  @note This class extends std::pair<byte *, size_t> to make
  it consistent with how memory regions are described by
  std::get_temporary_buffer(). It is also possible to initialize
  a `bytes` instance by buffer returned from
  std::get_temporary_buffer(), as follows:

    bytes buf = std::get_temporary_buffer<byte>(size);

  @ingroup devapi_aux
*/

class bytes : public std::pair<const byte*, size_t>
{

public:

  bytes(const byte *beg_, const byte *end_)
    : pair(beg_, end_ - beg_)
  {}

  bytes(const byte *beg, size_t len) : pair(beg, len)
  {}

  bytes(const char *str) : pair((const byte*)str, 0)
  {
    if (nullptr != str)
      second = strlen(str);
  }

  bytes(std::pair<const byte*, size_t> buf) : pair(buf)
  {}

  bytes() : pair(nullptr, 0)
  {}

  bytes(const bytes &) = default;

  virtual const byte* begin() const { return first; }
  virtual const byte* end() const { return first + second; }

  size_t length() const { return second; }
  size_t size() const { return length(); }

  class Access;
  friend Access;
};


/*
  Infrastructure for type-agnostic handling of lists
  ==================================================

  Template internal::List_initializer<> defined below is used to return lists
  of values from public API method so that user can store this list in
  a container of his choice. The only requirement is that the container instance
  should be constructible from two iterators defining a range of elements
  (such constructors exists for standard STL containers, for example).

  Thus, given a public API method foo() which returns a List_initializer<> for
  lists of elements of type X, user can do the following:

     My_container cont = foo();

  The container will be constructed as if this code was executed:

     My_container cont = My_container(begin, end);

  where begin and end are STL iterators defining a range of elements of type X.
  This is implemented by defining templated conversion operator.

  Apart from initializing containers, values of List_initializer<> type can
  be iterated using a range loop:

    for(X &el : foo()) { ... }

  Otherwise, user should not be able to use List_initializer<> values directly.
*/

namespace internal {

/*
  Iterator template.

  It defines an STL input iterator which is implemented using an
  implementation object of some type Impl. It is assumed that Impl
  has the following methods:

  void iterator_start() - puts iterator in "before begin" position;
  bool iterator_next() - moves iterator to next position, returns
                        false if it was not possible;
  Value_type iterator_get() - gets current value.

  An implementation object must be passed to iterator constructor. Iterator
  stores only a pointer to this implementation (so it must exist as long as
  iterator is used).
*/

template<
  typename Impl,
  typename T          = typename std::iterator_traits<Impl>::value_type,
  typename Distance   = typename std::iterator_traits<T*>::difference_type,
  typename Pointer    = typename std::iterator_traits<T*>::pointer,
  typename Reference  = typename std::iterator_traits<T*>::reference
>
struct Iterator
  : std::iterator < std::input_iterator_tag, T, Distance, Pointer, Reference >
{
protected:

  typename std::remove_reference<Impl>::type *m_impl = NULL;
  bool m_at_end = false;

public:

  Iterator(Impl& impl)
    : m_impl(&impl)
  {
    m_impl->iterator_start();
    m_at_end = !m_impl->iterator_next();
  }

  Iterator()
    : m_at_end(true)
  {}

  bool operator==(const Iterator &other) const
  {
    return (m_at_end && other.m_at_end);
  }

  bool operator !=(const Iterator &other) const
  {
    /*
      Compares only if both iterators are at the end
      of the sequence.
    */
    return !(m_at_end && other.m_at_end);
  }

  Iterator& operator++()
  {
    try {
      if (m_impl && !m_at_end)
        m_at_end = !m_impl->iterator_next();
      return *this;
    }
    CATCH_AND_WRAP
  }

  T operator*() const
  {
    if (!m_impl || m_at_end)
      THROW("Attempt to dereference null iterator");

    try {
      return m_impl->iterator_get();
    }
    CATCH_AND_WRAP
  }

  friend Impl;
};


/*
  List_initializer object can be used to initialize a container of
  arbitrary type U with list of items taken from a source object.

  It is assumed that the source object type Source defines iterator
  type and that std::begin/end() return iterators to the beginning
  and end of the sequence. The container type U is assumed to have
  a constructor from begin/end iterator.

  List_iterator defines begin/end() methods, so it is possible to
  iterate over the sequence without storing it in any container.
*/

template <class Source>
class List_initializer
{
protected:

  Source m_src;

  friend Source;

public:


  /*
    Arguments given to the constructor are passed to the internal
    m_src object.
  */

  template <typename... Ty>
  List_initializer(Ty&&... args)
    : m_src(std::forward<Ty>(args)...)
  {}

  /*
    Narrow the set of types for which this template is instantiated
    to avoid ambiguous conversion errors. It is important to disallow
    conversion to std::initializer_list<> because this conversion path
    is considered when assigning to STL containers.
  */

  template <
    typename U
    , typename std::enable_if<
        !std::is_same< U, std::initializer_list<typename U::value_type> >::value
      >::type* = nullptr
  >
  operator U()
  {
    try {
      return U(std::begin(m_src), std::end(m_src));
    }
    CATCH_AND_WRAP
  }

  auto begin() -> decltype(std::begin(m_src))
  {
    try {
      return std::begin(m_src);
    }
    CATCH_AND_WRAP
  }

  auto end() const -> decltype(std::end(m_src))
  {
    try {
      return std::end(m_src);
    }
    CATCH_AND_WRAP
  }
};


template <typename T>
struct iterator_traits
{
  using value_type = typename std::remove_reference<T>::type;
  using difference_type
    = typename std::iterator_traits<value_type*>::difference_type;
  using pointer
    = typename std::iterator_traits<value_type*>::pointer;
  using reference
    = typename std::iterator_traits<value_type*>::reference;
};


/*
  This helper template adapts class Impl to be used as a source for
  List_initializer<> template.

  Class Impl should be suitable for the Iterator<> template which is used to
  build iterators required by List_initializer<>. That is, Impl should
  implement iterator_start(), iteratore_next() etc (see Iterator<>).
*/

template<
  typename Impl,
  typename Value_type = typename Impl::Value,
  typename Distance   = typename iterator_traits<Value_type>::difference_type,
  typename Pointer    = typename iterator_traits<Value_type>::pointer,
  typename Reference  = typename iterator_traits<Value_type>::reference
>
class List_source
{
protected:

  Impl m_impl;

public:

  template <typename... Ty>
  List_source(Ty&&... args)
    : m_impl(std::forward<Ty>(args)...)
  {}

  using iterator = Iterator<Impl, Value_type, Distance, Pointer, Reference>;

  iterator begin()
  {
    return iterator(m_impl);
  }

  iterator end() const
  {
    return iterator();
  }
};


/*
  A template used to adapt an object of class Impl that represents an array of
  values accessed via operator[] to be used as source for List_initializer<>
  template. This template uses instance of Impl to implement the iterator
  methods iterator_start(), so that it can be used with Iterator<> template.
*/

template <typename Impl, typename Value_type = typename Impl::Value>
class Array_src_impl
{
protected:

  Impl m_impl;
  size_t m_pos = 0;
  bool   m_at_begin = true;

public:

  template <typename... Ty>
  Array_src_impl(Ty&&... args)
    : m_impl(std::forward<Ty>(args)...)
  {}

  void iterator_start()
  {
    m_pos = 0;
    m_at_begin = true;
  }

  bool iterator_next()
  {
    if (m_at_begin)
      m_at_begin = false;
    else
      m_pos++;
    return m_pos < size();
  }

  Value_type iterator_get()
  {
    return operator[](m_pos);
  }

  Value_type operator[](size_t pos)
  {
    return m_impl[pos];
  }

  size_t size() const
  {
    return m_impl.size();
  }
};


/*
  This template adapts an object of type Impl holding an array of values as
  a source for List_initializer<> template. It combines List_source<> and
  Array_src_impl<> adapters.
*/

template<
  typename Impl,
  typename Value_type = typename Impl::Value,
  typename Distance   = typename iterator_traits<Value_type>::difference_type,
  typename Pointer    = typename iterator_traits<Value_type>::pointer,
  typename Reference  = typename iterator_traits<Value_type>::reference
>
class Array_source
  : public List_source<
      Array_src_impl<Impl, Value_type>,
      Value_type,
      Distance,
      Pointer,
      Reference
    >
{
  using Base = List_source<
    Array_src_impl<Impl, Value_type>,
    Value_type,
    Distance,
    Pointer,
    Reference
  >;

  using Base::m_impl;

public:

  using
  List_source<
    Array_src_impl<Impl, Value_type>,
    Value_type,
    Distance,
    Pointer,
    Reference
  >::List_source;

  Value_type operator[](size_t pos)
  {
    return m_impl[pos];
  }

  size_t size() const
  {
    return m_impl.size();
  }
};

}  // internal


/*
  Infrastructure for handling variable argument lists
  ===================================================

  See documentation of Args_processor<> template.
*/

namespace internal {

/*
  Type trait which checks if std::begin()/end() work on objects of given
  class C, so that it can be used as a range to iterate over.

  TODO: Make it work also with user-defined begin()/end() functions.
  TODO: Make it work with plain C arrays. For example:

      int vals[] = { 1, 2, 3 }
      process_args(data, vals)
*/

template <class C>
class is_range
{
  /*
    Note: This overload is taken into account only if std::begin(X) and
    std::end(X) expressions are valid.
  */
  template <class X>
  static std::true_type
  test(
    decltype(std::begin(*((X*)nullptr)))*,
    decltype(std::end(*((X*)nullptr)))*
  );

  template <class X>
  static std::false_type test(...);

public:

  static const bool value = std::is_same<
    std::true_type,
    decltype(test<C>(nullptr, nullptr))
  >::value;
};


/*
  Class template to be used for uniform processing of variable argument lists
  in public API methods. This template handles the cases where arguments
  are specified directly as a list:

    method(arg1, arg2, ..., argN)

  or they are taken from a container such as std::list:

    method(container)

  or they are taken from a range of items described by two iterators:

    method(begin, end)

  A class B that is using this template to define a varargs method 'foo'
  should define it as follows:

    template <typename... T>
    X foo(T... args)
    {
      Args_processor<B>::process_args(m_impl, args...);
      return ...;
    }

  Process_args() is a static method of Args_processor<> and therefore
  additional context data is passed to it as the first argument. By default
  this context is a pointer to internal implementation object, as defined
  by the base class B. The process_args() methods does all the necessary
  processing of the variable argument list, passing the resulting items
  one-by-one to B::process_one() method. Base class B must define this
  static method, which takes the context and one data item as arguments.
  B::process_one() method can have overloads that handle different types
  of data items.

  See devapi/detail/crud.h for usage examples.
*/

template <class Base, class D = typename Base::Impl*>
class Args_processor
{
public:

  /*
    Check if item of type T can be passed to Base::process_one()
  */

  template <typename T>
  class can_process
  {
    template <typename X>
    static std::true_type
    test(decltype(Base::process_one(*(D*)nullptr, *(X*)nullptr))*);

    template <typename X>
    static std::false_type test(...);

  public:

    static const bool value
      = std::is_same< std::true_type, decltype(test<T>(nullptr)) >::value;
  };

public:

  /*
    Process items from a container.
  */

  template <
    typename C,
    typename std::enable_if<is_range<C>::value>::type* = nullptr,
    typename std::enable_if<!can_process<C>::value>::type* = nullptr
  >
  static void process_args(D data, C container)
  {
    // TODO: use (const) reference to avoid copying instances?
    for (auto el : container)
    {
      Base::process_one(data, el);
    }
  }

  /*
    If process_args(data, a, b) is called and a,b are of the same type It
    which can not be passed to Base::process_one() then we assume that a and
    b are iterators that describe a range of elements to process.
  */

  template <
    typename It,
    typename std::enable_if<!can_process<It>::value>::type* = nullptr
  >
  static void process_args(D data, const It &begin, const It &end)
  {
    for (It it = begin; it != end; ++it)
    {
      Base::process_one(data, *it);
    }
  }

  /*
    Process elements given as a varargs list.
  */

  template <
    typename T,
    typename... R,
    typename std::enable_if<can_process<T>::value>::type* = nullptr
  >
  static void process_args(D data, T first, R&&... rest)
  {
    process_args1(data, first, std::forward<R>(rest)...);
  }

private:

  template <
    typename T,
    typename... R,
    typename std::enable_if<can_process<T>::value>::type* = nullptr
  >
  static void process_args1(D data, T first, R&&... rest)
  {
    Base::process_one(data, first);
    process_args1(data, std::forward<R>(rest)...);
  }

  static void process_args1(D)
  {}

};

}  // internal namespace

MYSQLX_ABI_END(2,0)
}  // mysqlx


#endif

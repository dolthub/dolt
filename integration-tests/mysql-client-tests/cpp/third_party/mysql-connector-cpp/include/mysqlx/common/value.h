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

#ifndef MYSQLX_COMMON_VALUE_H
#define MYSQLX_COMMON_VALUE_H


#include "api.h"
#include "error.h"
#include "util.h"

PUSH_SYS_WARNINGS
#include <string>
POP_SYS_WARNINGS


namespace mysqlx {
MYSQLX_ABI_BEGIN(2,0)

namespace common {

class Value_conv;

/*
  Class representing a polymorphic value of one of the supported types.

  TODO: Extend it with array and document types (currently these are implemented
  in derived mysqlx::Value class of DevAPI).

  TODO: When storing raw bytes, currently they are copied inside the Value
  object. Consider if this can be avoided.
*/

class PUBLIC_API Value
  : public virtual Printable
{
public:

  enum Type
  {
    VNULL,      ///< Null value
    UINT64,     ///< Unsigned integer
    INT64,      ///< Signed integer
    FLOAT,      ///< Float number
    DOUBLE,     ///< Double number
    BOOL,       ///< Boolean
    STRING,     ///< String (utf8)
    USTRING,    ///< Wide string (utf16)
    RAW,        ///< Raw bytes
    EXPR,       ///< String to be interpreted as an expression
    JSON,       ///< JSON string
  };

  using string = std::string;

protected:

  Type m_type;

  // TODO: Use std::variant to save space

  DLL_WARNINGS_PUSH

  std::string     m_str;
  std::u16string  m_ustr;

  DLL_WARNINGS_POP

  union {
    double   v_double;
    float    v_float;
    int64_t  v_sint;
    uint64_t v_uint;
    bool     v_bool;
  } m_val;

  void print(std::ostream&) const override;

  template <typename T>
  Value(Type type, T &&init)
    : Value(std::forward<T>(init))
  {
    m_type = type;
  }

public:

  // Construct a NULL item
  Value() : m_type(VNULL)
  {}


  // Construct an item from a string
  Value(const std::string& str) : m_type(STRING), m_str(str)
  {
    m_val.v_bool = false;
  }

  Value(const std::u16string &str)
    : m_type(USTRING), m_ustr(str)
  {
    m_val.v_bool = false;
  }


  // Construct an item from a signed 64-bit integer
  Value(int64_t v) : m_type(INT64)
  { m_val.v_sint = v; }

  // Construct an item from an unsigned 64-bit integer
  Value(uint64_t v) : m_type(UINT64)
  { m_val.v_uint = v; }

  // Construct an item from a float
  Value(float v) : m_type(FLOAT)
  { m_val.v_float = v; }

  // Construct an item from a double
  Value(double v) : m_type(DOUBLE)
  { m_val.v_double = v; }


  // Construct an item from a bool
  Value(bool v) : m_type(BOOL)
  { m_val.v_bool = v; }

  // Construct an item from bytes
  Value(const byte *ptr, size_t len) : m_type(RAW)
  {
    // Note: bytes are copied to m_str member.
    m_str.assign((const char*)ptr, len);
  }

  // Other numeric conversions

  template <
    typename T,
    typename std::enable_if<std::is_unsigned<T>::value>::type* = nullptr
  >
  Value(T val)
    : Value(uint64_t(val))
  {}

  template <
    typename T,
    typename std::enable_if<!std::is_unsigned<T>::value>::type* = nullptr,
    typename std::enable_if<std::is_integral<T>::value>::type* = nullptr
  >
  Value(T val)
    : Value(int64_t(val))
  {}

  bool is_null() const
  {
    return VNULL == m_type;
  }

  bool get_bool() const
  {
    switch (m_type)
    {
    case BOOL:   return m_val.v_bool;
    case UINT64: return 0 != m_val.v_uint;
    case INT64:  return 0 != m_val.v_sint;
    default:
      throw Error("Can not convert to Boolean value");
    }
  }

  uint64_t get_uint() const
  {
    if (UINT64 != m_type && INT64 != m_type && BOOL != m_type)
      throw Error("Can not convert to integer value");

    if (BOOL == m_type)
      return m_val.v_bool ? 1 : 0;

    if (INT64 == m_type && 0 > m_val.v_sint)
      throw Error("Converting negative integer to unsigned value");

    uint64_t val = (UINT64 == m_type ? m_val.v_uint : (uint64_t)m_val.v_sint);

    return val;
  }

  int64_t get_sint() const
  {
    if (INT64 == m_type)
      return m_val.v_sint;

    uint64_t val = get_uint();

    if (!check_num_limits<int64_t>(val))
      throw Error("Value cannot be converted to signed integer number");

    return val;
  }

  float get_float() const
  {
    switch (m_type)
    {
    case INT64:  return 1.0F*m_val.v_sint;
    case UINT64: return 1.0F*m_val.v_uint;
    case FLOAT:  return m_val.v_float;
    default:
      throw Error("Value cannot be converted to float number");
    }
  }

  double get_double() const
  {
    switch (m_type)
    {
    case INT64:  return 1.0*m_val.v_sint;
    case UINT64: return 1.0*m_val.v_uint;
    case FLOAT:  return m_val.v_float;
    case DOUBLE: return m_val.v_double;
    default:
      throw Error("Value can not be converted to double number");
    }
  }

  /*
    Note: In general this method returns raw value representation as obtained
    from the server, which is stored in m_str member. If a non-string value was
    not obtained from the server, there is no raw representation for it and
    error is thrown. String values always have raw representation which is
    either utf8 or utf16 encoding. Strings obtained from the server use utf8 as
    raw representation. For strings created by user code this might be either
    utf8 or utf16, depending on how string was created.
  */

  const byte* get_bytes(size_t *size) const
  {
    switch (m_type)
    {
    case USTRING:
      if (!m_ustr.empty())
      {
        if (size)
          *size = m_ustr.size() * sizeof(char16_t);
        return (const byte*)m_ustr.data();
      }
      FALLTHROUGH;

    default:
      if (m_str.empty())
        throw Error("Value cannot be converted to raw bytes");
      FALLTHROUGH;

    case RAW:
    case STRING:
      if (size)
        *size = m_str.length();
      return (const byte*)m_str.data();

    }
  }

  // Note: these methods perform utf8 conversions as necessary.

  const std::string&    get_string() const;
  const std::u16string& get_ustring() const;

  Type get_type() const
  {
    return m_type;
  }

private:

  /*
    Note: Avoid implicit conversion from pointer types to bool.
    Without this declaration, Value(bool) constructor is invoked
    for pointer types. Here we declare and hide an explicit constructor
    for pointer types which prevents compiler to pick Value(bool).
  */

  template <typename T>
  Value(const T*);

public:

  friend Value_conv;

  struct Access;
  friend Access;
};

}  // common
MYSQLX_ABI_END(2,0)
}  // mysqlx

#endif

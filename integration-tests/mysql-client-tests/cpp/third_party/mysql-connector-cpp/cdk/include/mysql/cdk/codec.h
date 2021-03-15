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

#ifndef CDK_CODEC_H
#define CDK_CODEC_H

#include "common.h"
#include "charsets.h"

namespace cdk {


class Format_base
{
public:

  bool for_type(Type_info ti) const { return ti == m_type; }

protected:

  Type_info m_type;
  const Format_info &m_fi;

  Format_base(Type_info ti, const Format_info &fi)
    : m_type(ti), m_fi(fi)
  {
    if (!fi.for_type(ti))
      THROW("incompatible data encoding format");
  }
};


/*
  Encoding format descriptions for different types
  ================================================
*/

template<>
class Format<TYPE_INTEGER> : public Format_base
{
public:

  enum Fmt { UINT, SINT };

  Format(const Format_info &fi)
    : Format_base(TYPE_INTEGER, fi)
    , m_fmt(SINT)
  {
    fi.get_info(*this);
  }

  bool is_unsigned() const { return UINT == m_fmt; }
  size_t length() const { return m_length; }

protected:

  Fmt m_fmt;
  size_t m_length;

public:

  struct Access;
  friend struct Access;
};


template<>
class Format<TYPE_FLOAT> : public Format_base
{
public:

  enum Fmt { FLOAT, DOUBLE, DECIMAL };

  Format(const Format_info &fi)
    : Format_base(TYPE_FLOAT, fi)
  {
    fi.get_info(*this);
  }

  Fmt type() const { return m_fmt; }

protected:

  Fmt  m_fmt;

public:

  struct Access;
  friend struct Access;
};


template<>
class Format<TYPE_BYTES> : public Format_base
{
public:

  Format(const Format_info &fi)
    : Format_base(TYPE_BYTES, fi)
    , m_width(0)
  {
    fi.get_info(*this);
  }

  uint64_t pad_width() const { return m_width; }

protected:

  /*
    If not zero and actual bytes are shorter than m_width
    then they should be right-padded with 0x00 byte to this
    width.
  */
  uint64_t m_width;

public:

  struct Access;
  friend struct Access;
};


/*
  String encoding formats (charsets)
  ----------------------------------
*/

/*
  Constants for the known character set encodings. There is constant
  Charset::CS for each charset CS listed in CDK_CD_LIST() macro (see
  charsets.h).
*/

struct Charset
{

#undef  CS_ENUM
#undef  CS
#define CS_ENUM(CS) CS,

  enum value
  {
    CDK_CS_LIST(CS_ENUM)
  };
};


template<>
class  Format<TYPE_STRING> : public Format_base
{
public:

  Format(const Format_info &fi)
    : Format_base(TYPE_STRING, fi)
    , m_cs(Charset::value(0)), m_width(0), m_kind(STRING)
  {
    fi.get_info(*this);
  }

  Charset::value charset() const { return m_cs; }
  uint64_t pad_width() const { return m_width; }
  bool     is_enum() const { return m_kind == ENUM; }
  bool     is_set()  const { return m_kind == SET; }

  foundation::api::String_codec *codec() const;

protected:

  // Character set encoding.

  Charset::value m_cs;

  /**
    If not zero and actual string is shorter than m_width
    then it should be right-padded with space characters
    to this width.
  */

  uint64_t m_width;

  enum { STRING, ENUM, SET } m_kind;

public:

  struct Access;
  friend struct Access;
};


template<>
class Format<TYPE_DOCUMENT> : public Format_base
{
public:

  Format(const Format_info &fi)
    : Format_base(TYPE_DOCUMENT, fi)
  {}
};


template<>
class Format<TYPE_DATETIME> : public Format_base
{
public:

  enum Fmt { TIMESTAMP, TIME, DATETIME };

  Format(const Format_info &fi)
    : Format_base(TYPE_DATETIME, fi)
  {
    fi.get_info(*this);
  }

  Fmt type() const { return m_fmt; }
  bool has_time() const { return m_has_time; }

private:

  Fmt m_fmt;
  bool m_has_time;

public:

  struct Access;
  friend struct Access;
};


// TODO: Cover all supported types


/*
  Codecs
  ======
*/


template <Type_info TI>
class Codec_base
{
protected:

  Format<TI> m_fmt;

public:

  Codec_base(const Format_info &fi)
    : m_fmt(fi)
  {}
};


template <>
class Codec<TYPE_STRING>
  : public foundation::api::String_codec
  , Codec_base<TYPE_STRING>
{

  foundation::api::String_codec& get_codec()
  {
    foundation::api::String_codec *codec = m_fmt.codec();
    if (!codec)
      throw_error("undefined string conversion");
    return *codec;
  }

public:

  Codec(const Format_info &fi)
    : Codec_base<TYPE_STRING>(fi)
  {}

  /// Return number of bytes required to encode given string.
  //size_t measure(const string&);

  size_t from_bytes(bytes raw, string& str);
  size_t to_bytes(const string&, bytes);
};


template <>
class Codec<TYPE_BYTES>
  : Codec_base<TYPE_STRING>
{
public:

  Codec(const Format_info &fi) : Codec_base<TYPE_STRING>(fi) {}

  size_t from_bytes(bytes raw, std::string& str);
  size_t to_bytes(const std::string&, bytes);
};


template <>
class Codec<TYPE_INTEGER>
  : public foundation::api::Number_codec
  , Codec_base<TYPE_INTEGER>
{

  foundation::Codec<foundation::Type::NUMBER> m_cvt;

  template <typename T>
  size_t internal_from_bytes(bytes buf, T &val);

  template <typename T>
  size_t internal_to_bytes(T val, bytes buf);

public:

  Codec(const Format_info &fi) : Codec_base<TYPE_INTEGER>(fi) {}

  virtual size_t from_bytes(bytes buf, int8_t &val);
  virtual size_t from_bytes(bytes buf, int16_t &val);
  virtual size_t from_bytes(bytes buf, int32_t &val);
  virtual size_t from_bytes(bytes buf, int64_t &val);
  virtual size_t from_bytes(bytes buf, uint8_t &val);
  virtual size_t from_bytes(bytes buf, uint16_t &val);
  virtual size_t from_bytes(bytes buf, uint32_t &val);
  virtual size_t from_bytes(bytes buf, uint64_t &val);

  virtual size_t to_bytes(int8_t val, bytes buf);
  virtual size_t to_bytes(int16_t val, bytes buf);
  virtual size_t to_bytes(int32_t val, bytes buf);
  virtual size_t to_bytes(int64_t val, bytes buf);

  virtual size_t to_bytes(uint8_t val, bytes buf);
  virtual size_t to_bytes(uint16_t val, bytes buf);
  virtual size_t to_bytes(uint32_t val, bytes buf);
  virtual size_t to_bytes(uint64_t val, bytes buf);
};


template <>
class Codec<TYPE_FLOAT>
  : Codec_base<TYPE_FLOAT>
{

  foundation::Codec<foundation::Type::NUMBER> m_cvt;

  std::string internal_decimal_to_string(bytes buf);

public:

  Codec(const Format_info &fi) : Codec_base<TYPE_FLOAT>(fi) {}

  virtual ~Codec() {}

  virtual size_t from_bytes(bytes buf, float &val);
  virtual size_t from_bytes(bytes buf, double &val);

  virtual size_t to_bytes(float val, bytes buf);
  virtual size_t to_bytes(double val, bytes buf);

};


template <>
class Codec<TYPE_DOCUMENT>
  : Codec_base<TYPE_DOCUMENT>
{
  class Doc_format : public Format_info
  {
    bool for_type(Type_info ti) const
    { return TYPE_DOCUMENT == ti; }
  };

  static Doc_format m_format;

public:

  Codec() : Codec_base<TYPE_DOCUMENT>(m_format) {}

  size_t from_bytes(bytes data, JSON::Processor &);
};


}  // cdk


#endif

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

#ifndef SDK_FOUNDATION_CODEC_H
#define SDK_FOUNDATION_CODEC_H

#include "types.h"
#include "error.h"
#include "string.h"

PUSH_SYS_WARNINGS_CDK

#include <limits>         // for std::numeric_limits
#include <string.h>       // for memset

POP_SYS_WARNINGS_CDK


namespace cdk {
namespace foundation {

struct Type
{
  enum value { STRING, NUMBER };
};

// Codecs for different types
template <Type::value T>
class Codec;


/*
  String codecs
  =============
*/

namespace api {

/*
  Generic interface to be implemented by string codecs.
*/

class String_codec
{
public:

  virtual ~String_codec() {}

  //virtual size_t measure(const string&) =0;
  virtual size_t from_bytes(bytes, string&) =0;
  virtual size_t to_bytes(const string&, bytes) =0;
};

} // api namespace



template <class ENC>
class String_codec
  : api::String_codec
{
public:

  size_t from_bytes(bytes in, string &out) override
  {
    return str_decode<ENC>(in.begin(), in.size(), out);
  }

  size_t to_bytes(const string &in, bytes out) override
  {
    return str_encode<ENC>(in, out.begin(), out.size());
  }

};


// String utf8 codec

template<>
class Codec<Type::STRING>
  : public String_codec<String_encoding::UTF8>
{};



/*
  Number codecs
  =============
*/

#ifndef CDK_BIG_ENDIAN
#error Unknown endianess!
#endif


struct Endianess
{
  enum value {
    BIG,
    LITTLE,
    NATIVE =
#if CDK_BIG_ENDIAN
      BIG
#else
      LITTLE
#endif
    ,
    NETWORK = BIG
  };
};


/*
  Template num_size<N,Signed>::type defines numeric type which is
  good for storing values encoded using N bytes.
*/

template<size_t N, bool Signed>
struct num_type;

template<> struct num_type<8,true>  { typedef int64_t  type; };
template<> struct num_type<8,false> { typedef uint64_t type; };
template<> struct num_type<4,true>  { typedef int32_t  type; };
template<> struct num_type<4,false> { typedef uint32_t type; };
template<> struct num_type<2,true>  { typedef int16_t  type; };
template<> struct num_type<2,false> { typedef uint16_t type; };
template<> struct num_type<1,true>  { typedef int8_t  type; };
template<> struct num_type<1,false> { typedef uint8_t type; };


namespace api {

/*
  Generic interface implemented by numeric codecs.
*/

class Number_codec
{
public:

  virtual ~Number_codec() {}

  virtual size_t from_bytes(bytes buf, int8_t &val)  =0;
  virtual size_t from_bytes(bytes buf, int16_t &val) =0;
  virtual size_t from_bytes(bytes buf, int32_t &val) =0;
  virtual size_t from_bytes(bytes buf, int64_t &val) =0;

  virtual size_t from_bytes(bytes buf, uint8_t &val)  =0;
  virtual size_t from_bytes(bytes buf, uint16_t &val) =0;
  virtual size_t from_bytes(bytes buf, uint32_t &val) =0;
  virtual size_t from_bytes(bytes buf, uint64_t &val) =0;

  virtual size_t to_bytes(int8_t val, bytes buf)  =0;
  virtual size_t to_bytes(int16_t val, bytes buf) =0;
  virtual size_t to_bytes(int32_t val, bytes buf) =0;
  virtual size_t to_bytes(int64_t val, bytes buf) =0;

  virtual size_t to_bytes(uint8_t val, bytes buf)  =0;
  virtual size_t to_bytes(uint16_t val, bytes buf) =0;
  virtual size_t to_bytes(uint32_t val, bytes buf) =0;
  virtual size_t to_bytes(uint64_t val, bytes buf) =0;

};

} // api namespace

template<Endianess::value E>
class Number_codec;


// Encoding/decoding for native endianess is done by simple type casts

template<>
class Number_codec<Endianess::NATIVE> : public api::Number_codec
{
protected:

  class Wrong_size_error : public Error_class<Wrong_size_error>
  {
    size_t m_int_size, m_buf_size;
    void do_describe(std::ostream&) const;
  public:
    Wrong_size_error(size_t int_size, size_t buf_size)
      : Error_base(NULL, cdkerrc::conversion_error),
        m_int_size(int_size), m_buf_size(buf_size)
    {}
  };


template<typename T>
static size_t convert(bytes buf, T &val)
  {
    /*
      If buf size is smaller than sizeof(T), convert 1,2,4 or 8 initial
      bytes from the buffer: as much as fits into T.
    */

    if (buf.size() >= sizeof(T))
    {
      val= *(T*)buf.begin();
      return sizeof(T);
    }
    else if (buf.size() >= 8)
    {
      val= (T)*((typename num_type< 8, std::numeric_limits<T>::is_signed >::type*)
                buf.begin());
      return 8;
    }
    else if (buf.size() >= 4)
    {
      val= (T)*((typename num_type< 4, std::numeric_limits<T>::is_signed >::type*)
                buf.begin());
      return 4;
    }
    else if (buf.size() >= 2)
    {
      val= (T)*((typename num_type< 2, std::numeric_limits<T>::is_signed >::type*)
                buf.begin());
      return 2;
    }
    else if (buf.size() >= 1)
    {
      val= (T)*((typename num_type< 1, std::numeric_limits<T>::is_signed >::type*)
                buf.begin());
      return 1;
    }

    // TODO: better error description
    throw_error(cdkerrc::conversion_error,
                "Number_codec: no data for conversion");

    return 0;
  }


  template<typename T>
  static size_t convert(T val, bytes buf)
  {
    if (buf.size() < sizeof(T))
      throw Wrong_size_error(sizeof(T), buf.size());
    // note: assumes proper alignment of the buffer
    *(T*)buf.begin() = val;
    return sizeof(T);
  }

public:

  size_t from_bytes(bytes buf, int8_t &val)  { return convert(buf, val); }
  size_t from_bytes(bytes buf, int16_t &val) { return convert(buf, val); }
  size_t from_bytes(bytes buf, int32_t &val) { return convert(buf, val); }
  size_t from_bytes(bytes buf, int64_t &val) { return convert(buf, val); }

  size_t from_bytes(bytes buf, uint8_t &val)  { return convert(buf, val); }
  size_t from_bytes(bytes buf, uint16_t &val) { return convert(buf, val); }
  size_t from_bytes(bytes buf, uint32_t &val) { return convert(buf, val); }
  size_t from_bytes(bytes buf, uint64_t &val) { return convert(buf, val); }

  size_t to_bytes(int8_t val, bytes buf)  { return convert(val, buf); }
  size_t to_bytes(int16_t val, bytes buf) { return convert(val, buf); }
  size_t to_bytes(int32_t val, bytes buf) { return convert(val, buf); }
  size_t to_bytes(int64_t val, bytes buf) { return convert(val, buf); }

  size_t to_bytes(uint8_t val, bytes buf)  { return convert(val, buf); }
  size_t to_bytes(uint16_t val, bytes buf) { return convert(val, buf); }
  size_t to_bytes(uint32_t val, bytes buf) { return convert(val, buf); }
  size_t to_bytes(uint64_t val, bytes buf) { return convert(val, buf); }
};


/*
  For the endianess opposite to the native one. we use native codec and
  reverse the bytes.
*/

template<>
class Number_codec<
#if CDK_BIG_ENDIAN
  Endianess::LITTLE
#else
  Endianess::BIG
#endif
>
  : Number_codec<Endianess::NATIVE>
{
  typedef Number_codec<Endianess::NATIVE> Base;

  template<typename T>
  static size_t convert(bytes buf, T &val)
  {
    // Determine how much bytes to convert

    size_t howmuch = buf.size();
    if (howmuch >= sizeof(T)) howmuch= sizeof(T);
    else if (howmuch >= 8) howmuch= 8;
    else if (howmuch >= 4) howmuch= 4;
    else if (howmuch >= 2) howmuch= 2;
    else if (howmuch >= 1) howmuch= 1;

    // Reverse bytes and use native endianess conversion

    byte buf0[sizeof(T)];
    for (unsigned pos=0; pos < howmuch; ++pos)
      buf0[pos] = *(buf.begin() + howmuch -pos -1);

    return Base::convert(bytes(buf0, howmuch), val);
  }

  template<typename T>
  static size_t convert(T val, bytes buf)
  {
    if (buf.size() < sizeof(T))
      throw Wrong_size_error(sizeof(T), buf.size());

    // Perform native endianess conversion to buf0

    byte buf0[sizeof(T)];
    size_t howmuch= Base::convert(val, bytes(buf0, sizeof(T)));

    // and then reverse the bytes

    for (unsigned pos=0; pos < howmuch; ++pos)
      *(buf.begin() + pos) = buf0[howmuch-pos-1];
    return howmuch;
  }

  /*
    Simpler code for special case of 1- and 2-byte integers.
    Note: normal functions take precedence over templates.
  */

  static size_t convert(bytes buf, uint8_t &val)
  {
    if (buf.size() < 1)
      throw_error(cdkerrc::conversion_error,
                  "Number_codec: no data for conversion");
    val= *buf.begin();
    return 1;
  }

  static size_t convert(uint8_t val, bytes buf)
  {
    if (buf.size() < 1)
      throw Wrong_size_error(1, 0);
    *buf.begin()= val;
    return 1;
  }

  static size_t convert(bytes buf, int8_t &val)
  { return convert(buf, (uint8_t&)val); }

  size_t convert(int8_t val, bytes buf)
  { return convert((uint8_t)val, buf); }


  static size_t convert(bytes buf, uint16_t &val)
  {
    if (buf.size() < 2)
      return Base::convert(buf, val);
    byte buf0[2]= { *(buf.begin()+1), *buf.begin() };
    return Base::convert(bytes(buf0,2), val);
  }

  static size_t convert(uint16_t val, bytes buf)
  {
    if (buf.size() < 2)
      throw Wrong_size_error(2, buf.size());
    byte buf0[2];
    Base::convert(val, bytes(buf0,2));
    *(buf.begin())   = buf0[1];
    *(buf.begin()+1) = buf0[0];
    return 2;
  }

  static size_t convert(bytes buf, int16_t &val)
  { return convert(buf, (uint16_t&)val); }

  static size_t convert(int16_t val, bytes buf)
  { return convert((uint16_t)val, buf); }


public:

  size_t from_bytes(bytes buf, int8_t &val)  { return convert(buf, val); }
  size_t from_bytes(bytes buf, int16_t &val) { return convert(buf, val); }
  size_t from_bytes(bytes buf, int32_t &val) { return convert(buf, val); }
  size_t from_bytes(bytes buf, int64_t &val) { return convert(buf, val); }

  size_t from_bytes(bytes buf, uint8_t &val)  { return convert(buf, val); }
  size_t from_bytes(bytes buf, uint16_t &val) { return convert(buf, val); }
  size_t from_bytes(bytes buf, uint32_t &val) { return convert(buf, val); }
  size_t from_bytes(bytes buf, uint64_t &val) { return convert(buf, val); }

  size_t to_bytes(int8_t val, bytes buf)  { return convert(val, buf); }
  size_t to_bytes(int16_t val, bytes buf) { return convert(val, buf); }
  size_t to_bytes(int32_t val, bytes buf) { return convert(val, buf); }
  size_t to_bytes(int64_t val, bytes buf) { return convert(val, buf); }

  size_t to_bytes(uint8_t val, bytes buf)  { return convert(val, buf); }
  size_t to_bytes(uint16_t val, bytes buf) { return convert(val, buf); }
  size_t to_bytes(uint32_t val, bytes buf) { return convert(val, buf); }
  size_t to_bytes(uint64_t val, bytes buf) { return convert(val, buf); }

};


/*
  Define the default codec for numbers as the one that converts to/from network
  byte order.
*/

template<>
class Codec<Type::NUMBER> : public Number_codec<Endianess::LITTLE>
{};


// Description for Wrong_size_error

inline
void
Number_codec<Endianess::NATIVE>::Wrong_size_error::do_describe(std::ostream &out)
const
{
  out <<"Number_codec: Conversion of " <<8*m_int_size <<"-bit integer requires "
      <<m_int_size <<" bytes but " <<m_buf_size <<" are available "
      <<"(" << code() <<")";
}


}} // cdk::foundation

#endif

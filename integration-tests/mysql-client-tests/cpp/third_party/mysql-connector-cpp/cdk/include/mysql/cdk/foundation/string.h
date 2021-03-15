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

#ifndef SDK_FOUNDATION_STRING_H
#define SDK_FOUNDATION_STRING_H

#include "common.h"
#include "error.h"  // throw_error()

#include <rapidjson/encodings.h>
#include <rapidjson/memorystream.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/encodedstream.h>

PUSH_SYS_WARNINGS_CDK
#include <stdint.h>
#include <string.h>
#include <string>
#include <memory>
POP_SYS_WARNINGS_CDK


namespace cdk {
namespace foundation {


typedef char32_t      char_t;
constexpr char_t   invalid_char = (char_t)-1;
typedef std::basic_string<char16_t> ustring;


struct String_encoding {

  // Note: BE/LE versions are used with byte streams. Otherwise encodings
  // work on single code units and as such are endianess agnostic.

  // Unicode with 4 byte code units (so, code unit = code point)

  using UCS4BE = rapidjson::UTF32BE<char32_t>;  // this is the standard
  using UCS4LE = rapidjson::UTF32LE<char32_t>;
  using UCS4   = rapidjson::UTF32<char32_t>;

  // UTF16 with 2 byte code units.

  using UTF16BE = rapidjson::UTF16BE<char16_t>;  // this is the standard
  using UTF16LE = rapidjson::UTF16BE<char16_t>;
  using UTF16   = rapidjson::UTF16<char16_t>;

  // Single byte encodings.

  using UTF8 = rapidjson::UTF8<char>;
  using ASCII = rapidjson::ASCII<char>;

  using CHR = UCS4;
  using STR = UTF16;

  /*
    Note: We assume that wide strings use UTF encoding. This is usually the case
    but gcc, for example, can be configured to use different encoding for wide
    (and plain) strings. We do not cover such exotic scenarios.
  */

#if WCHAR_SIZE < 4
  using WIDE  = rapidjson::UTF16<wchar_t>;
#else
  using WIDE  = rapidjson::UTF32<wchar_t>;
#endif

};



/*
  Rapidjson compatible stream of characters taken form fixed memory
  region. Supports both input and output.

  Note: Modified code taken from rapidjson memorystream.h
*/

template <typename CHAR>
struct Mem_stream
{
  typedef CHAR Ch; // byte


  Mem_stream(const Ch *beg, const Ch *end)
    : src_(const_cast<Ch*>(beg)), begin_(beg), end_(end)
  {}

  Mem_stream(const Ch *src, size_t size)
    : Mem_stream(src, src + size)
  {}

  Mem_stream()
    : Mem_stream(nullptr, nullptr)
  {}

  bool hasData() const { return src_ && src_ < end_; }

  Ch Peek() const
  {
    return RAPIDJSON_UNLIKELY(src_ == end_) ? Ch(0) : *src_;
  }

  Ch Take()
  {
    return RAPIDJSON_UNLIKELY(src_ == end_) ? Ch(0) : *src_++;
  }

  size_t Tell() const
  {
    return static_cast<size_t>(src_ - begin_);
  }

  void SetPos(const Ch *pos)
  {
    assert(begin_ <= pos);
    assert(pos <= end_);
    src_ = const_cast<Ch*>(pos);
  }

  // TODO: ReserveN() and copy optimization...

  void Put(Ch c)
  {
    if (RAPIDJSON_LIKELY(src_ < end_))
    {
      *src_++ = c;
      return;
    }
    overflow_ = true;
  }

  void Flush() {}

  //Ch* PutBegin() { RAPIDJSON_ASSERT(false); return 0; }
  //size_t PutEnd(Ch*) { RAPIDJSON_ASSERT(false); return 0; }

  Ch* src_;     //!< Current read position.
  const Ch* begin_;   //!< Original head of the string.
  const Ch* end_;     //!< End of stream.
  bool  overflow_ = false;
};


/*
  Rapidjson compatible stream of characters that writes to a string
  object.
*/

template <typename CHAR>
struct Str_stream
{
  typedef CHAR Ch; // byte
  using string = std::basic_string<Ch>;

  Str_stream(string &str)
    : buf_(str), pos_(str.length())
  {}


  bool hasData() const { return pos_ < buf_.length(); }

  Ch Peek() const
  {
    return RAPIDJSON_LIKELY(!hasData()) ? Ch(0) : buf_[pos_];
  }

  Ch Take()
  {
    return RAPIDJSON_LIKELY(!hasData()) ? Ch(0) : buf_[pos_++];
  }

  size_t Tell() const
  {
    return pos_;
  }

  void SetPos(const Ch *pos)
  {
    assert(buf_.data() <= pos);
    assert(pos <= buf_.data() + buf_.length());
    pos_ = pos - buf_.data();
  }

  // TODO: ReserveN() and copy optimization...

  void Put(Ch c)
  {
    if (RAPIDJSON_UNLIKELY(hasData()))
    {
      buf_[pos_++] = c;
      return;
    }
    buf_.push_back(c);
    pos_++;
  }

  void Flush() {}

  //Ch* PutBegin() { RAPIDJSON_ASSERT(false); return 0; }
  //size_t PutEnd(Ch*) { RAPIDJSON_ASSERT(false); return 0; }

  string &buf_;
  size_t pos_ = 0;     //!< Current read position.
};


/*
  Decode sequence of code points of encoding FROM into a string that uses
  encoding TO.

  Returns number of code points consumed.
*/

template<class FROM, class TO = String_encoding::STR>
size_t str_decode(
  const typename FROM::Ch *beg, size_t len,
  std::basic_string<typename TO::Ch> &out
)
{
  if (!len)
    return 0;

  using Transcoder = rapidjson::Transcoder<FROM, TO>;
  Mem_stream<typename FROM::Ch> input(beg, len);
  Str_stream<typename TO::Ch>   output(out);

  while (input.hasData())
  {
    if (!Transcoder::Transcode(input, output))
    {
      // TODO: add some context info from the input stream.
      throw_error("Failed string conversion");
    }
  }

  return input.Tell();
}


/*
  Decode sequence of bytes using encoding FROM into a string that uses
  encoding TO.

  Returns number of bytes consumed.
*/

template<class FROM, class TO = String_encoding::STR>
size_t str_decode(
  const byte *beg, size_t len,
  std::basic_string<typename TO::Ch> &out
)
{
  if (!len)
    return 0;

  using Transcoder = rapidjson::Transcoder<FROM, TO>;
  Mem_stream<char> bytes((const char*)beg, len);
  rapidjson::EncodedInputStream<FROM, Mem_stream<char> > input(bytes);
  Str_stream<typename TO::Ch>   output(out);

  while(input.Peek() != '\0')
  {
    if (!Transcoder::Transcode(input, output))
    {
      // TODO: add some context info from the input stream.
      throw_error("Failed string conversion");
    }
  }

  return bytes.Tell();
}


/*
  Encode a string that uses encoding FROM into a sequence of code points
  of encoding TO. At most as many code points are generated as will fit
  into the output buffer.

  Returns the number of code points generated.
*/

template<class TO, class FROM = String_encoding::STR>
size_t str_encode(
  const std::basic_string<typename FROM::Ch> &in,
  typename TO::Ch *out, size_t len
)
{
  if (in.empty())
    return 0;

  using Transcoder = rapidjson::Transcoder<FROM, TO>;
  Mem_stream<typename FROM::Ch> input(in.data(), in.length());
  Mem_stream<typename TO::Ch>   output(out, len);

  // Note: output.hasData() in fact checks if there is space available

  while (input.hasData() && output.hasData())
  {
    if (!Transcoder::Transcode(input, output))
    {
      // TODO: add some context info from the input stream.
      throw_error("Failed string conversion");
    }
  }

  return output.Tell();
}


/*
  Encode a string that uses encoding FROM into a sequence of bytes that
  represent this string in encoding TO. At most as many bytes are generated
  as will fit into the output buffer.

  Returns the number of bytes generated.
*/

template<class TO, class FROM = String_encoding::STR>
size_t str_encode(
  const std::basic_string<typename FROM::Ch> &in,
  byte *out, size_t len
)
{
  if (in.empty())
    return 0;

  using Transcoder = rapidjson::Transcoder<FROM, TO>;
  Mem_stream<typename FROM::Ch> input(in.data(), in.length());
  Mem_stream<char>   buf((char*)out, len);
  // Note: false means do not put BOM marker in the output
  rapidjson::EncodedOutputStream<TO, Mem_stream<char> > output(buf, false);

  // Note: buf.hasData() in fact checks if there is space available

  while (input.hasData() && buf.hasData())
  {
    if (!Transcoder::Transcode(input, output))
    {
      // TODO: add some context info from the input stream.
      throw_error("Failed string conversion");
    }
  }

  return buf.Tell();
}




/*
  Iterate through a sequence of code units of the given encoding, one character
  at a time (single character can be encoded using one or more code units).
*/

template <class ENC>
class char_iterator_base
  : public std::iterator<
      std::input_iterator_tag,
      char_t,
      long,
      const char_t*,
      const char_t&
    >
{
protected:

  using code_unit = typename ENC::Ch;

  Mem_stream<code_unit>   m_stream;

  /*
    If m_char !=0 then it contains the current character (which was already
    decoded) and m_pos points at the next code unit after the character.
    If m_char == 0 then m_pos points at the first code unit of the next
    character (which is not yet decoded).

           m_char != 0
           |   m_pos                    m_pos (m_char == 0)
           |   |                        |
           v   v                        v
    ------|===|--------            ----|===|-----

    TODO: m_pos is redundant, as it is the same as m_stream.src_ ?
  */

  const code_unit *m_pos = nullptr;
  char_t   m_char = 0;
  bool     m_at_end = false;

public:

  char_iterator_base()
    : m_at_end(true)
  {}

  char_iterator_base(const code_unit *beg, const code_unit *end)
    : m_stream(beg, end), m_pos(beg)
  {}

  char_iterator_base(const code_unit *beg, size_t len)
    : char_iterator_base(beg, beg + len)
  {}

  char_iterator_base(const char_iterator_base &other) = default;
  char_iterator_base& operator=(const char_iterator_base&) = default;

  // examine current character

  reference operator*() const
  {
    /*
      If m_char != 0 then it already contains the current character and
      the corresponding code units have been consumed from the input stream.

      Otherwise, the input stream contains code units of the current character
      and we need to decode it here. The input stream is moved to the next
      code unit after the current character.

      If decoding of the character fails, then the current character is invalid
      and input stream is positioned at the next code unit after error has been
      detected. After increasing iterator, decoding will continue from that
      position.
    */

    if (!m_char && m_stream.hasData())
    {
      auto *self = const_cast<char_iterator_base*>(this);
      if (!ENC::Decode(self->m_stream, (unsigned*)&(self->m_char)))
        self->m_char = invalid_char;
    }
    return m_char;
  }

  code_unit get_unit()
  {
    assert(!at_end());
    return *m_pos;
  }

  // examine current position

  bool at_end() const
  {
    return m_at_end;
  }

  const code_unit* cur_pos() const
  {
    assert(!m_pos || (m_pos <= m_stream.end_));
    return m_pos;
  }

  // change position

  char_iterator_base& operator++()
  {
    if (at_end())
      return *this;

    operator*();  // moves stream to next position, if not already done
    m_pos = m_stream.src_;
    m_char = 0;
    m_at_end = (m_pos == m_stream.end_);

    return *this;
  }

  char_iterator_base& operator++(int)
  {
    return operator++();
  }

  /*
    Move to the next code unit in the input.

    In general this method moves to the next code unit in the input
    sequence. The only exception is if the current character takes more than
    one code unit and was already decoded (and so, the code units were already
    consumed). In this case position is moved to the next character.
  */

  void next_unit()
  {
    assert(!at_end());

    // if m_char is set, then corresponding code unit(s) are already consumed
    // from the stream.

    if (!m_char)
    {
      m_stream.Take();
      m_pos++;
    }
    else
      m_pos = m_stream.src_;

    m_at_end = (m_pos == m_stream.end_);
    m_char = 0;
  }

  void set_pos(const code_unit *pos)
  {
    assert(m_stream.begin_ <= pos);
    assert(pos <= m_stream.end_);
    m_pos = pos;
    m_at_end = (m_pos == m_stream.end_);
    m_stream.SetPos(pos);
  }

  // Other methods

  const code_unit* get_end() const
  {
    return m_stream.end_;
  }

  const code_unit* get_beg() const
  {
    return m_stream.begin_;
  }

  bool operator==(const char_iterator_base &other) const
  {
    // Note: only at end iterators compare - do we need more?

    if (at_end() && other.at_end())
      return true;
    return false;
  }

  bool operator!=(const char_iterator_base &other) const
  {
    return !operator==(other);
  }

};  // char_iterator_base


/*
  String class using UTF16 for internal representation.
*/

class string : public std::basic_string<char16_t>
{
  using Base = std::basic_string<char16_t>;

public:

  string() {}
  string(const Base &str)
    : Base(str)
  {}
  using Base::basic_string;

  // UTF8 string conversions.

  string(const char *str) { set_utf8(str); }
  string(const std::string &str) { set_utf8(str); }

  operator std::string() const
  {
    std::string out;
    str_decode<String_encoding::STR, String_encoding::UTF8>(
      data(), length(), out
    );
    return out;
  }

  string& set_utf8(const std::string &str)
  {
    clear();
    str_decode<String_encoding::UTF8>(str.data(), str.length(), *this);
    return *this;
  }

  string& set_ascii(const char *str, size_t len)
  {
    clear();
    str_decode<String_encoding::ASCII>(str, len, *this);
    return *this;
  }

  // Wide string conversions.

  explicit string(const wchar_t *str)
    : string(std::wstring(str))
  {}

  explicit string(const std::wstring &str)
  {
    clear();
    str_decode<String_encoding::WIDE>(str.data(), str.length(), *this);
  }

  explicit operator std::wstring() const
  {
    std::wstring out;
    str_decode<String_encoding::STR, String_encoding::WIDE>(
      data(), length(), out
    );
    return out;
  }

  // Unicode string conversions.

  explicit string(const char_t *str)
    : string(std::basic_string<char_t>(str))
  {}

  explicit string(const std::basic_string<char_t> &str)
  {
    clear();
    str_decode<String_encoding::UCS4>(str.data(), str.length(), *this);
  }

  explicit operator std::basic_string<char_t>() const
  {
    std::basic_string<char_t> out;
    str_decode<String_encoding::STR, String_encoding::UCS4>(
      data(), length(), out
    );
    return out;
  }

  void push_back(char_t c)
  {
    str_decode<String_encoding::UCS4>(&c, 1, *this);
  }

  // Character iterator.

  class char_iterator;

  char_iterator chars() const;
  char_iterator chars_end() const;

};


/*
  Character iterator.
*/


class string::char_iterator
  : public char_iterator_base<String_encoding::STR>
{
  char_iterator(const string &str)
    : char_iterator_base(str.data(), str.length())
  {}

  char_iterator() = default;

  friend string;
};


inline
auto string::chars() const -> char_iterator
{
  return *this;
}

inline
auto string::chars_end() const -> char_iterator
{
  return{};
}


/*
  Operators to help overload resolution.
*/

inline
string operator+(const string &lhs, const string &rhs)
{
  string ret(lhs);
  ret += rhs;
  return ret;
}

inline
bool operator==(const string &lhs, const string &rhs)
{
  return 0 == lhs.compare(rhs);
}

inline
bool operator!=(const string &lhs, const string &rhs)
{
  return 0 != lhs.compare(rhs);
}


// Note: these two needed to make overload resolution unique.

inline
string operator+(const string &lhs, const char16_t *rhs)
{
  string ret(lhs);
  ret += rhs;
  return ret;
}

inline
string operator+(const char16_t *lhs, const string &rhs)
{
  string ret(lhs);
  ret += rhs;
  return ret;
}


// Output to stream converts to UTF8

inline
std::ostream& operator<<(std::ostream &out, const string &str)
{
  return out << (std::string)str;
}


}} // cdk::foundation

#endif

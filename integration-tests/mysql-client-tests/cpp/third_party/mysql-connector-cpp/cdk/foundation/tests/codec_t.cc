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
#include <mysql/cdk/foundation/codec.h>

#include <algorithm>

using namespace ::std;
using namespace ::cdk::foundation;

/*
  String Codec
  ============
*/


// Samples prepared using: http://www.endmemo.com/unicode/unicodeconverter.php
// and: http://www.columbia.edu/~kermit/utf8.html
// ("I can eat glass" phrase in different languages)

#define SAMPLES(X) \
  X (english, "I can eat glass") \
  X (polish, "Mog\u0119 je\u015B\u0107 szk\u0142o") \
  X (japaneese, \
      "\u79C1\u306F\u30AC\u30E9\u30B9\u3092\u98DF\u3079\u3089\u308C\u307E\u3059\u3002\u305D\u308C\u306F\u79C1\u3092\u50B7\u3064\u3051\u307E\u305B\u3093\u3002") \
  X (ukrainian, \
      "\u042F \u043C\u043E\u0436\u0443 \u0457\u0441\u0442\u0438 \u0441\u043A\u043B\u043E, \u0456 \u0432\u043E\u043D\u043E \u043C\u0435\u043D\u0456 \u043D\u0435 \u0437\u0430\u0448\u043A\u043E\u0434\u0438\u0442\u044C") \
  X (portuguese, "Posso comer vidro, n\u00E3o me faz mal") \

// Samples with characters outside of Unicode BMP
// Note: On Windows such characters can not be handled by wide string

#define SAMPLES_EXT(X) \
  X (banana, "z\u00df\u6c34\U0001f34c") \

// Samples taken from: http://www.php.net/manual/en/reference.pcre.pattern.modifiers.php#54805

#define SAMPLES_BAD_UTF8(X) \
  X("\xc3\x28")                   /* 2 Octet Sequence */ \
  X("\xa0\xa1")                   /* Invalid Sequence Identifier */ \
  X("\xe2\x28\xa1")               /* 3 Octet Sequence (in 2nd Octet) */ \
  X("\xe2\x82\x28")               /* 3 Octet Sequence (in 3rd Octet) */ \
  X("\xf0\x28\x8c\xbc")           /* 4 Octet Sequence (in 2nd Octet) */ \
  X("\xf0\x90\x28\xbc")           /* 4 Octet Sequence (in 3rd Octet) */ \
  X("\xf0\x28\x8c\x28")           /* 4 Octet Sequence (in 4th Octet) */ \
  X("\xf8\xa1\xa1\xa1\xa1")   /* Valid 5 Octet Sequence (but not Unicode!) */ \
  X("\xfc\xa1\xa1\xa1\xa1\xa1")  /* Valid 6 Octet Sequence (but not Unicode!) */


template <typename CHAR>
struct samples
{
  static CHAR *sample[];
  static const char *name;
  static const size_t cnt;
};


#define SET_SAMPLES(CHAR,NAME) \
CHAR* samples<CHAR>::sample[] = { \
  SAMPLES(SAMPLE ## NAME) SAMPLES_EXT(SAMPLE ## NAME ## _EXT) \
}; \
const size_t samples<CHAR>::cnt = sizeof(samples<CHAR>::sample)/sizeof(CHAR*); \
const char* samples<CHAR>::name = # NAME;

#define SAMPLEutf8(X,Y) u8 ## Y,
#define SAMPLEutf16(X,Y)   u ## Y,
#define SAMPLEucs(X,Y)   U ## Y,
#define SAMPLEwide(X,Y)   L ## Y,

#define SAMPLEutf8_EXT(X,Y) u8 ## Y,
#define SAMPLEutf16_EXT(X,Y)   u ## Y,
#define SAMPLEucs_EXT(X,Y)   U ## Y,

#ifdef WIN32
  #define SAMPLEwide_EXT(X,Y)
#else
  #define SAMPLEwide_EXT(X,Y)   L ## Y,
#endif


SET_SAMPLES(char, utf8)
SET_SAMPLES(char16_t, utf16)
SET_SAMPLES(char32_t, ucs)
SET_SAMPLES(wchar_t, wide)



template <typename CHAR>
void string_conv_test()
{
  using char_t = CHAR;
  using string = std::basic_string<CHAR>;
  using cdk_string = cdk::foundation::string;

  cout << "== testing " << samples<char_t>::name << endl;

  for (size_t i = 0; i < samples<char_t>::cnt; ++i)
  {
    string in = samples<char_t>::sample[i];
    cdk_string cdk(in);
    string out(cdk);
    EXPECT_EQ(in, out) << "sample " << i;
  }
}


TEST(Foundation, string)
{
  using cdk_string = cdk::foundation::string;

#if 1

  string_conv_test<char>();
  string_conv_test<char16_t>();
  string_conv_test<char32_t>();
  string_conv_test<wchar_t>();

  cout << endl << "=== Bad UTF8 test ===" << endl << endl;

  const char *bad_utf8[] = {
#define BAD_SAMPLE(X) "bad" X "utf8",
    SAMPLES_BAD_UTF8(BAD_SAMPLE)
  };

  for (const char *sample : bad_utf8)
  {
    cout << "-- checking: " << sample << endl;
    EXPECT_THROW(cdk_string str(sample), Error);
  }

  cout << endl << "=== Invalid character test ===" << endl << endl;

  {
    cdk_string str = "test";
    EXPECT_THROW(str.push_back(cdk::foundation::invalid_char), Error);
    EXPECT_EQ("test", str);
  }

#endif

}


TEST(Foundation, string_iter)
{
  using samples = samples<char_t>;
  using string  = cdk::foundation::string;

  for (unsigned i = 0; i < samples::cnt; ++i)
  {
    cout <<"checking sample " <<i <<endl;

    const std::u32string &s1 = samples::sample[i];
    string s2(samples::sample[i]);
    ASSERT_TRUE(std::equal(s2.chars(), s2.chars_end(), s1.begin(), s1.end()));
    auto res = std::mismatch(s1.begin(), s1.end(), s2.chars());
    ASSERT_EQ(s2.chars_end(), res.second);
  }
}


template <typename ENC>
void test_codec(const char_t *str)
{
  using cdk::foundation::string;

  String_codec<ENC> codec;
  static byte buf[256];

  string a(str);
  string b;

  size_t len = codec.to_bytes(a, { buf, sizeof(buf) });
  size_t len1 = codec.from_bytes({ buf, len }, b);

  EXPECT_EQ(len, len1);
  EXPECT_EQ(a, b);
}


TEST(Foundation, string_codec)
{
  for (unsigned i=0; i < samples<char_t>::cnt; ++i)
  {
    cout << "checking sample " <<i <<endl;

    const char_t *sample = samples<char_t>::sample[i];

    cout << "- UTF8" << endl;
    test_codec<String_encoding::UTF8>(sample);

    cout << "- UTF16" << endl;
    test_codec<String_encoding::UTF16>(sample);

    cout << "- UCS4" << endl;
    test_codec<String_encoding::UCS4>(sample);
  }
}


/*
  Number Codecs
  =============
*/

/*
  Structure to hold test data consisting of byte sequence and
  numeric values when this sequence is treated as little endian
  or big endian integer.
*/

template <size_t S>
struct test_data
{
  typedef typename num_type<S,false>::type uint;
  typedef typename num_type<S,true>::type sint;

  byte buf[S];
  uint le;  // little endian value
  uint be;  // big endian value

  // array with test data
  static test_data samples[];
};


/*
  Perform tests for count entries in test_data<S>::samples[] array.
*/

template<size_t S>
void do_test(unsigned count)
{
  typedef test_data<S> test_data;
  typedef typename test_data::uint uint_t;
  typedef typename test_data::sint sint_t;

  cout <<"== Testing " <<8*S <<"bit integers ==" <<endl;

  Number_codec<Endianess::BIG>    codec_big;
  Number_codec<Endianess::LITTLE> codec_little;

  for (unsigned pos=0; pos < count; ++pos)
  {
    test_data &sample = test_data::samples[pos];

    cout <<endl <<"= sample# " <<pos <<" =" <<endl;
    cout <<"bytes: " <<::testing::PrintToString(sample.buf) <<endl;

    // Conversion from bytes to number

    uint_t uval;
    sint_t sval;

    codec_little.from_bytes(bytes(sample.buf,S), uval);
    cout <<"little unsigned: " <<uval <<endl;
    EXPECT_EQ(sample.le, uval);

    codec_little.from_bytes(bytes(sample.buf,S), sval);
    cout <<"  little signed: " <<sval <<endl;
    uval= (uint_t)sval;
    EXPECT_EQ(sample.le, uval);

    codec_big.from_bytes(bytes(sample.buf,S), uval);
    cout <<"   big unsigned: " <<uval <<endl;
    EXPECT_EQ(sample.be, uval);

    codec_big.from_bytes(bytes(sample.buf,S), sval);
    cout <<"     big signed: " <<sval <<endl;
    uval= (uint_t)sval;
    EXPECT_EQ(sample.be, uval);

    cout <<"- conversion to 64-bit integer value" <<endl;

    uint64_t uval1;
    int64_t  sval1;

    codec_little.from_bytes(bytes(sample.buf,S), uval1);
    cout <<"little unsigned: " <<uval1 <<endl;
    EXPECT_EQ(sample.le, uval1);

    codec_little.from_bytes(bytes(sample.buf,S), sval1);
    cout <<"  little signed: " <<sval1 <<endl;
    uval1= (uint_t)sval1;
    EXPECT_EQ(sample.le, uval1);

    codec_big.from_bytes(bytes(sample.buf,S), uval1);
    cout <<"   big unsigned: " <<uval1 <<endl;
    EXPECT_EQ(sample.be, uval1);

    codec_big.from_bytes(bytes(sample.buf,S), sval1);
    cout <<"     big signed: " <<sval1 <<endl;
    uval1= (uint_t)sval1;
    EXPECT_EQ(sample.be, uval1);

    // Conversion from number to bytes

#define ARRAY_EQ(A,B) \
    for (unsigned i=0; i<S; ++i) EXPECT_EQ(A[i],B[i]);

    byte buf[S];

    memset(buf, 0, S);
    codec_little.to_bytes(sample.le, bytes(buf,S));
    ARRAY_EQ(sample.buf, buf);

    memset(buf, 0, S);
    codec_little.to_bytes((sint_t)sample.le, bytes(buf,S));
    ARRAY_EQ(sample.buf, buf);

    memset(buf, 0, S);
    codec_big.to_bytes(sample.be, bytes(buf,S));
    ARRAY_EQ(sample.buf, buf);

    memset(buf, 0, S);
    codec_big.to_bytes((sint_t)sample.be, bytes(buf,S));
    ARRAY_EQ(sample.buf, buf);
  }

  cout <<endl;
}


/*
  Standard C++ does not have literals for 64-bit numbers (LL suffix is
  introduced in C++11). For that reason we split test values into two
  32-bit numbers. Function do_test<8>() has slightly different code which
  takes this into account.
*/

template <>
struct test_data<8>
{

  byte buf[8];
  uint32_t le_lo, le_hi;  // little endian value
  uint32_t be_lo, be_hi;  // big endian value

  static test_data samples[];
};


template<>
void do_test<8>(unsigned count)
{
  typedef test_data<8> test_data;
  typedef uint64_t uint_t;
  typedef int64_t  sint_t;

  cout <<"== Testing " <<64 <<"bit integers ==" <<endl;

  Number_codec<Endianess::BIG>    codec_big;
  Number_codec<Endianess::LITTLE> codec_little;

  for (unsigned pos=0; pos < count; ++pos)
  {
    test_data &sample = test_data::samples[pos];

    cout <<endl <<"= sample# " <<pos <<" =" <<endl;
    cout <<"bytes: " <<::testing::PrintToString(sample.buf) <<endl;

    // Conversion from bytes to number

    uint_t uval;
    sint_t sval;

    codec_little.from_bytes(bytes(sample.buf,8), uval);
    cout <<"little unsigned: " <<uval <<endl;
    EXPECT_EQ(sample.le_lo, uval & 0xFFFFFFFFU);
    EXPECT_EQ(sample.le_hi, uval >>32);

    codec_little.from_bytes(bytes(sample.buf,8), sval);
    cout <<"  little signed: " <<sval <<endl;
    uval= (uint_t)sval;
    EXPECT_EQ(sample.le_lo, uval & 0xFFFFFFFFU);
    EXPECT_EQ(sample.le_hi, uval >>32);

    codec_big.from_bytes(bytes(sample.buf,8), uval);
    cout <<"   big unsigned: " <<uval <<endl;
    EXPECT_EQ(sample.be_lo, uval & 0xFFFFFFFFU);
    EXPECT_EQ(sample.be_hi, uval >>32);

    codec_big.from_bytes(bytes(sample.buf,8), sval);
    cout <<"     big signed: " <<sval <<endl;
    uval= (uint_t)sval;
    EXPECT_EQ(sample.be_lo, uval & 0xFFFFFFFFU);
    EXPECT_EQ(sample.be_hi, uval >>32);

    // Conversion from number to bytes

#define ARRAY_EQ8(A,B) \
    for (unsigned i=0; i<8; ++i) EXPECT_EQ(A[i],B[i]);

    byte buf[8];

    uval = sample.le_hi;
    uval = (uval<<32) + sample.le_lo;
    sval = (sint_t)uval;
    codec_little.to_bytes(uval, bytes(buf,8));
    ARRAY_EQ8(sample.buf, buf);
    codec_little.to_bytes(sval, bytes(buf,8));
    ARRAY_EQ8(sample.buf, buf);

    uval = sample.be_hi;
    uval = (uval<<32) + sample.be_lo;
    sval = (sint_t)uval;
    codec_big.to_bytes(uval, bytes(buf,8));
    ARRAY_EQ8(sample.buf, buf);
    codec_big.to_bytes(sval, bytes(buf,8));
    ARRAY_EQ8(sample.buf, buf);
  }

  cout <<endl;
}


template<>
test_data<1> test_data<1>::samples[] =
{
  { {0x9c}, 0x9c, 0x9c },
};

template<>
test_data<2> test_data<2>::samples[] =
{
  { {0x9C,0x00}, 0x009C, 0x9C00 },
  { {0x00,0x9C}, 0x9C00, 0x009C },
  { {0x9C,0xFF}, 0xFF9C, 0x9CFF },
};

template<>
test_data<4> test_data<4>::samples[] =
{
  { {0x9C,0x00,0x00,0x00}, 0x0000009C, 0x9C000000 },
  { {0x9C,0xFF,0xFF,0xFF}, 0xFFFFFF9C, 0x9CFFFFFF },
  { {0x01,0x02,0x03,0x04}, 0x04030201, 0x01020304 },
  { {0xF1,0xF2,0xF3,0xF4}, 0xF4F3F2F1, 0xF1F2F3F4 },
};

test_data<8> test_data<8>::samples[] =
{
  { {0x9C,0x00,0x00,0x00,0x00,0x00,0x00,0x00},
    0x0000009C, 0x00000000, 0x00000000, 0x9C000000 },
  { {0x9C,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF},
    0xFFFFFF9C, 0xFFFFFFFF, 0xFFFFFFFF, 0x9CFFFFFF },
  { {0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08},
    0x04030201, 0x08070605, 0x05060708, 0x01020304 },
  { {0xF1,0xF2,0xF3,0xF4,0xF5,0xF6,0xF7,0xF8},
    0xF4F3F2F1, 0xF8F7F6F5, 0xF5F6F7F8, 0xF1F2F3F4 },
};


TEST(Foundation, number)
{
  do_test<1>(1);
  do_test<2>(3);
  do_test<4>(4);
  do_test<8>(4);

  cout <<endl <<"== Negative tests ==" <<endl <<endl;

  Codec<Type::NUMBER> codec;

  int32_t val= -100;
  byte buf[8];

#define EXPECT_ERROR(Code) \
  try { Code; FAIL() <<"Should throw error"; } \
  catch (Error &err) { cout <<"Expected error: " <<err <<endl; }

  EXPECT_ERROR(codec.to_bytes(val, bytes(buf,2)));
  EXPECT_ERROR(codec.from_bytes(bytes(buf,(size_t)0),val));

  // should be OK to convert to a buffer which is too big...

  size_t howmuch= codec.to_bytes(val, bytes(buf,8));
  EXPECT_EQ(sizeof(int32_t),howmuch);
  codec.from_bytes(bytes(buf,howmuch), val);
  EXPECT_EQ(-100,val);

  // when converting to smaller variable, only initial part of buffer
  // is used

  howmuch= codec.from_bytes(bytes(buf,8), val);
  EXPECT_EQ(-100,val);
  EXPECT_EQ(sizeof(val),howmuch);

  int16_t val1;
  howmuch= codec.from_bytes(bytes(buf,8), val1);
  EXPECT_EQ(sizeof(val1),howmuch);

  // The number of bytes converted always equals valid integer type size

  howmuch= codec.from_bytes(bytes(buf,sizeof(val)+1), val);
  EXPECT_EQ(-100,val);
  EXPECT_EQ(sizeof(val),howmuch);

  howmuch= codec.from_bytes(bytes(buf,3), val);
  EXPECT_EQ(2U,howmuch);

}

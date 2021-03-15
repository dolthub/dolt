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

#include "auth_hash.h"
#include <stdint.h>
#include <stdexcept>
#include <string.h>  // memset

// Avoid warnings from protobuf
#if defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-conversion"
#elif defined _MSC_VER
#pragma warning (push)
#endif

#include <openssl/sha.h>

#ifdef __GNUC__
#pragma GCC diagnostic pop
#elif defined _MSC_VER
#pragma warning (pop)
#endif



#define PVERSION41_CHAR '*'
#define SCRAMBLE_LENGTH 20
#define SHA1_HASH_SIZE 20
#define SHA256_HASH_SIZE 32

typedef unsigned char byte;
typedef size_t length_t;

class SHA
{
  SHA_CTX m_sha;

  void init()
  {
    SHA1_Init(&m_sha);
  }

  public:

  enum { DIGEST_SIZE = SHA1_HASH_SIZE };   // in Bytes

  SHA()
  {
    init();
  }

  void Update(byte* data, length_t length)
  {
    SHA1_Update(&m_sha, data, length);
  }

  void Final(byte* hash)
  {
    SHA1_Final(hash, &m_sha);
    init();
  }

  size_t getDigestSize() const {return SHA1_HASH_SIZE; }
};

class SHA256
{
  SHA256_CTX m_sha;

  void init()
  {
    SHA256_Init(&m_sha);
  }

  public:

  enum { DIGEST_SIZE = SHA256_HASH_SIZE };   // in Bytes

  SHA256()
  {
    init();
  }

  void Update(byte* data, length_t length)
  {
    SHA256_Update(&m_sha, data, length);
  }

  void Final(byte* hash)
  {
    SHA256_Final(hash, &m_sha);
    init();
  }

  size_t getDigestSize() const {return SHA256_HASH_SIZE; }
};


static void my_crypt(uint8_t *to, const uint8_t *s1, const uint8_t *s2, size_t len)
{
  const uint8_t *s1_end = s1 + len;

  while (s1 < s1_end)
    *to++= *s1++ ^ *s2++;
}



template<class SHA_Crypt>
static std::string scramble(const std::string &scramble_data, const std::string &password)
{
  SHA_Crypt sha;

  if (scramble_data.length() != SCRAMBLE_LENGTH)
    throw std::invalid_argument("Password scramble data is invalid");

  byte hash_stage1[SHA_Crypt::DIGEST_SIZE];
  byte hash_stage2[SHA_Crypt::DIGEST_SIZE];
  byte result_buf[SHA_Crypt::DIGEST_SIZE+1];

  memset(result_buf, 0, sizeof(result_buf));

  /* Two stage SHA1 hash of the pwd */
  /* Stage 1: hash pwd */
  sha.Update((byte*)password.data(), (length_t)password.length());
  sha.Final(hash_stage1);

  /* Stage 2 : hash first stage's output. */
  sha.Update(hash_stage1, sha.getDigestSize());
  sha.Final(hash_stage2);

  /* create crypt string as sha1(message, hash_stage2) */;
  /* MYSQL41 and SHA256_PASSWORD have different behaviors here! Bug? */
  if (sha.getDigestSize() == SHA1_HASH_SIZE)
  {
    sha.Update((byte*)scramble_data.data(), (length_t)scramble_data.length());
    sha.Update(hash_stage2, sha.getDigestSize());
  }
  else
  {
    sha.Update(hash_stage2, sha.getDigestSize());
    sha.Update((byte*)scramble_data.data(), (length_t)scramble_data.length());
  }
  sha.Final(result_buf);
  result_buf[sha.getDigestSize()] = '\0';

  my_crypt(result_buf, result_buf, hash_stage1, sha.getDigestSize());

  return std::string((char*)result_buf, sha.getDigestSize());
}

static char *octet2hex(char *to, const char *str, size_t len)
{
  const char *_dig_vec_upper = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  const char *str_end= str + len;
  for (; str != str_end; ++str)
  {
    *to++= _dig_vec_upper[((uint8_t) *str) >> 4];
    *to++= _dig_vec_upper[((uint8_t) *str) & 0x0F];
  }
  *to= '\0';
  return to;
}

// MYSQL41 specific

static std::string get_password_from_salt_mysql41(const std::string &hash_stage2)
{
  std::string result(2*SHA1_HASH_SIZE + 1, '\0');

  if (hash_stage2.length() != SHA1_HASH_SIZE)
    throw std::invalid_argument("Wrong size of binary hash password");

  result[0] = PVERSION41_CHAR;
  octet2hex(&result[1], &hash_stage2[0], SHA1_HASH_SIZE);

  return result;
}


std::string mysqlx::build_mysql41_authentication_response(
    const std::string &salt_data,
    const std::string &user,
    const std::string &password,
    const std::string &schema)
{
  std::string data;
  std::string password_hash;

  if (password.length())
  {
    password_hash = scramble<class SHA>(salt_data, password);
    password_hash = get_password_from_salt_mysql41(password_hash);
  }

  data.append(schema).push_back('\0'); // authz
  data.append(user).push_back('\0'); // authc
  data.append(password_hash); // pass

  return data;
}

// SHA256_MEMORY specific

static std::string get_password_from_salt_sha256(const std::string &hash_stage2)
{
  std::string result(2*SHA256_HASH_SIZE +1 , '\0');

  if (hash_stage2.length() != SHA256_HASH_SIZE)
    throw std::invalid_argument("Wrong size of binary hash password");

  octet2hex(&result[0], &hash_stage2[0], SHA256_HASH_SIZE);

  return result;
}

std::string mysqlx::build_sha256_authentication_response(
    const std::string &salt_data,
    const std::string &user,
    const std::string &password,
    const std::string &schema)
{
  std::string password_hash;
  std::string data;

  password_hash = scramble<class SHA256>(salt_data, password);
  password_hash = get_password_from_salt_sha256(password_hash);
  //REMOVE ending \0
  password_hash.pop_back();

  data.append(schema).push_back('\0'); // authz
  data.append(user).push_back('\0'); // authc
  data.append(password_hash); // pass

  return data;
}

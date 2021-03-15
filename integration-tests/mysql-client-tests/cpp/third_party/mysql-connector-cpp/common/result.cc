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

#include <mysql/cdk.h>

#include "result.h"
#include "session.h"

#include <vector>
#include <sstream>
#include <iomanip>
#include <cctype>


/*
  Implementation of result and row objects and conversion of raw bytes
  into values.
*/

using namespace ::mysqlx::impl::common;


/*
  Decoding raw bytes into values
  ==============================

  Overloads of convert() defined below handle conversion of raw representation
  of values of different CDK types into Value object. A format descriptor is
  used to learn about the raw encoding format and perform conversion to the
  correct type using a corresponding codec object.
*/


Value
mysqlx::impl::common::
convert(cdk::bytes data, Format_descr<cdk::TYPE_STRING> &fd)
{
  /*
    String encoding has artificial 0x00 byte appended at the end to
    distinguish the empty string from the null value. We skip
    the trailing 0x00 byte to get just the raw bytes that encode the string.
  */

  cdk::bytes raw(data.begin(), data.end() - 1);

  // If this string value is in fact a SET, then return it as raw bytes.

  if (fd.m_format.is_set())
    return { raw.begin(), raw.size() };

  auto &codec = fd.m_codec;
  cdk::string str;
  codec.from_bytes(raw, str);
  return (std::u16string)str;
}


Value
mysqlx::impl::common::
convert(cdk::bytes data, Format_descr<cdk::TYPE_INTEGER> &fd)
{
  auto &codec = fd.m_codec;
  auto &fmt = fd.m_format;

  if (fmt.is_unsigned())
  {
    uint64_t val;
    codec.from_bytes(data, val);
    return Value(val);
  }
  else
  {
    int64_t val;
    codec.from_bytes(data, val);
    return Value(val);
  }
}


Value
mysqlx::impl::common::
convert(cdk::bytes data, Format_descr<cdk::TYPE_FLOAT> &fd)
{
  auto &fmt = fd.m_format;

  if (fmt.FLOAT == fmt.type())
  {
    float val;
    fd.m_codec.from_bytes(data, val);
    return Value(val);
  }

  // For other formats (DOUBLE, DECIMAL), try storing in double
  // TODO: exact representation for DECIMAL values?
  {
    double val;
    fd.m_codec.from_bytes(data, val);
    return Value(val);
  }
}


Value
mysqlx::impl::common::
convert(cdk::bytes data, Format_descr<cdk::TYPE_DOCUMENT>&)
{
  if (0 == data.size())
    return Value();

  /*
    Note: Here we do not look into format description and blindly assume
    that document is represented as a JSON string.

    Otherwise, implementation that would not assume what underlying
    representation is used for documnets should use a Codec to decode
    the raw bytes and build a representation of the documnent to be
    stored in the Value instance.
  */

  // trim initial space

  unsigned i;
  for (i = 0; i < data.size() && std::isspace(*(data.begin() + i)); ++i);

  std::string json(data.begin() + i, data.end()-1);

  return Value::Access::mk_json(json);
}


Value
mysqlx::impl::common::
convert(cdk::foundation::bytes data, Format_descr<cdk::TYPE_DATETIME> &)
{
  return{ data.begin(), data.size()};
}



/*
  Result implementation
  =====================
*/


Result_impl::Result_impl(Result_init &init)
  : m_sess(init.get_session()), m_reply(init.get_reply())
{
  // Note: init.get_reply() can be NULL in the case of ignored server error
  m_sess->register_result(this);
  init.init_result(*this);
}


Result_impl::~Result_impl()
{
  try {
    if (m_sess)
      m_sess->deregister_result(this);
  }
  catch (...)
  {}

  // Note: Cursor must be deleted before reply.
  delete m_cursor;
  delete m_reply;
}


bool Result_impl::next_result()
{
  pop_row_cache();
  if(!m_result_cache.empty())
    return true;

  // Nothing in cache... jump to next resultset and read it
  return read_next_result();
}

bool Result_impl::read_next_result()
{
  /*
    Note: closing cursor discards previous rset. Only then
    we can move to the next rset (if any).
  */

  if (m_pending_rows)
  {
    assert(m_cursor);
    m_cursor->close();
  }

  // Prepare for reading (next) result

  delete m_cursor;
  m_cursor = nullptr;
  m_pending_rows = false;
  m_inited = true;

  if (!m_reply)
    return false;

  if (!m_reply->has_results())
  {
    if (0 < m_reply->entry_count())
      m_reply->get_error().rethrow();
    m_sess->deregister_result(this);
    return false;
  }

  // Result has row data - create cursor to access it

  m_cursor = new cdk::Cursor(*m_reply);

  // Wait for cursor to fetch result meta-data and copy it to local storage.

  m_cursor->wait();

  m_pending_rows = true;
  //Push new row cache
  push_row_cache();

  return true;
}

void Result_impl::push_row_cache()
{
  m_result_mdata.push(Shared_meta_data(new Meta_data(*m_cursor)));
  m_result_cache.push(Row_cache());
  m_result_cache_size.push(0);
}


const Row_data* Result_impl::get_row()
{
  // TODO: Session parameter for cache prefetch size

  load_cache(16);

  if (m_result_cache.empty() || m_result_cache.front().empty())
  {
    if (m_reply->entry_count() > 0)
      m_reply->get_error().rethrow();
    return nullptr;
  }

  m_row = m_result_cache.front().front();
  m_result_cache.front().pop_front();
  m_result_cache_size.front()--;
  return &m_row;
}


/*
  Returns true if there are some rows in the cache after returning from
  the call. If cache is empty when this method is called, it loads
  prefetch_size rows into the cache. If prefetch_size is 0, it loads
  all remaining rows into the cache (even if cache currently contains some
  rows).
  It caches elements to the last queue element, since more resultsets could have
  been cached before.
*/

bool Result_impl::load_cache(row_count_t prefetch_size)
{
  if (!m_inited)
    next_result();

  if(m_result_cache.empty())
    return false;

  if (!m_result_cache.back().empty() && 0 != prefetch_size)
    return true;

  if (!m_pending_rows)
    return false;

  /*
    Note: if cache is not empty then m_cache_it correctly points at the last
    element in the cache.
  */

  if (m_result_cache.back().empty())
    m_cache_it = m_result_cache.back().before_begin();

  // Initiate row reading operation

  if (0 < prefetch_size)
    m_cursor->get_rows(*this, prefetch_size);
  else
    m_cursor->get_rows(*this);  // this reads all remaining rows

  // Wait for it to complete

  m_cursor->wait();

  /*
    Cleanup after reading all rows: close the cursor if whole rset has
    been consumed (or error happend, in which case server won't sent more
    data).
  */

  if (!m_pending_rows || m_reply->entry_count() > 0)
  {
    m_cursor->close();
    m_pending_rows = false;

    /*
      If there are no more rsets in the reply, deregister the result so that
      session is unlocked for the next command.
    */

    if (m_reply->end_of_reply())
      m_sess->deregister_result(this);
  }

  return !m_result_cache.back().empty();
}


//  Row_processor interface implementation


size_t Result_impl::field_begin(col_count_t pos, size_t size)
{
  //m_row.insert(std::pair<col_count_t, Buffer>(pos, Buffer()));
  m_row.emplace(pos, Buffer());
  // FIX
  return size;
}

size_t Result_impl::field_data(col_count_t pos, bytes data)
{
  m_row[(unsigned)pos].append(data);
  // FIX
  return data.size();
}

void Result_impl::row_end(row_count_t)
{
  if (!m_row_filter(m_row))
    return;

  m_cache_it = m_result_cache.back().emplace_after(m_cache_it, std::move(m_row));
  m_result_cache_size.back()++;
}

void Result_impl::end_of_data()
{
  m_pending_rows = false;
}

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

#include <mysql/cdk/foundation/stream.h>
#include <mysql/cdk/foundation/error.h>
#include <mysql/cdk/foundation/opaque_impl.i>
#include <memory.h> // for memcpy

using namespace cdk::foundation;


/*
  Implementation of in-memory input/output stream.
*/

class Mem_stream_impl
  : public api::Connection
  , nocopy
{
  const size_t m_size;
  byte  *m_buf;
  byte  *m_in_pos;
  byte  *m_out_pos;

public:

  enum State { OPEN, CLOSED } m_state;

  Mem_stream_impl(byte *buf, size_t size)
    : m_size(size), m_buf(buf)
    , m_in_pos(m_buf), m_out_pos(m_buf)
    , m_state(OPEN)
  {}

  // Connection

  void connect() {}
  void close() { m_state= CLOSED; }
  bool is_closed() const { return m_state == CLOSED; }

  void reset()
  {
    m_in_pos= m_out_pos= m_buf;
    m_state= OPEN;
  }

  // Input_stream

  bool   eos() const { return m_in_pos >= m_out_pos; }
  bool   has_bytes() const { return !eos(); }

  // Output_stream

  bool   is_ended() const { return is_closed() || m_out_pos >= m_buf + m_size; }
  bool   has_space() const { return !is_ended(); }
  void   flush();

private:

  size_t read_buf(const bytes&);
  size_t write_buf(const bytes&);

  friend class cdk::foundation::test::Mem_stream_base::Read_op;
  friend class cdk::foundation::test::Mem_stream_base::Write_op;
};


IMPL_TYPE(cdk::foundation::test::Mem_stream_base, Mem_stream_impl);
IMPL_PLAIN(cdk::foundation::test::Mem_stream_base);



size_t Mem_stream_impl::read_buf(const bytes &buf)
{
  size_t howmuch= 0;

  if (m_in_pos < m_out_pos)
  {
    howmuch = static_cast<size_t>(m_out_pos - m_in_pos);
    if (howmuch > buf.size())
      howmuch= buf.size();
    memcpy(buf.begin(), m_in_pos, howmuch);
    m_in_pos += howmuch;
  }

  return howmuch;
}


size_t Mem_stream_impl::write_buf(const bytes &buf)
{
  size_t howmuch= 0;

  if (m_out_pos < m_buf + m_size)
  {
    howmuch = static_cast<size_t>(m_buf + m_size - m_out_pos);
    if (howmuch > buf.size())
      howmuch= buf.size();
    memcpy(m_out_pos, buf.begin(), howmuch);
    m_out_pos += howmuch;
  }

  return howmuch;
}


void Mem_stream_impl::flush()
{
  if (is_closed())
    throw_error("output_stream: flush: closed!");

  if (m_in_pos < m_out_pos)
    return;
  m_in_pos= m_buf;
  m_out_pos= m_buf;
}


namespace cdk {
namespace foundation {
namespace test {

Mem_stream_base::Read_op::Read_op(Mem_stream_base &str,
                                  const buffers &bufs,
                                  time_t deadline)
    : IO_op(str, bufs, deadline)
{
  Mem_stream_impl &impl = m_conn.get_impl();

  if (impl.eos())
    throw_error("mem_stream: attempt to read after eos");

  unsigned pos= 0;

  while (impl.m_in_pos < impl.m_out_pos && pos < bufs.buf_count())
  {
    m_howmuch += impl.read_buf(bufs.get_buffer(pos));
    pos++;
  }
}


Mem_stream_base::Write_op::Write_op(Mem_stream_base &str,
                                    const buffers &bufs,
                                    time_t deadline)
    : IO_op(str, bufs, deadline)
{
  Mem_stream_impl &impl = m_conn.get_impl();

  if (impl.is_ended())
    throw_error("mem_stream: attempt to write to ended stream");

  unsigned pos= 0;

  while (impl.m_out_pos < impl.m_buf + impl.m_size && pos < bufs.buf_count())
  {
    m_howmuch += impl.write_buf(bufs.get_buffer(pos));
    pos++;
  }
}

}}}



namespace cdk {
namespace foundation {

/*
  Implement public interface of test::Mem_stream_base
  using internal implementation.
*/

test::Mem_stream_base::Mem_stream_base(byte *buf, size_t size)
  : opaque_impl<Mem_stream_base>(NULL, buf, size)
{}

// Connection

void test::Mem_stream_base::connect()
{ get_impl().connect(); }

void test::Mem_stream_base::close()
{ get_impl().close(); }

bool test::Mem_stream_base::is_closed() const
{ return get_impl().is_closed(); }

void test::Mem_stream_base::reset()
{ get_impl().reset(); }

// Input stream

bool test::Mem_stream_base::eos() const
{ return get_impl().eos(); }

bool test::Mem_stream_base::has_bytes() const
{ return get_impl().has_bytes(); }

// Output stream

bool test::Mem_stream_base::is_ended() const
{ return get_impl().is_ended(); }

bool test::Mem_stream_base::has_space() const
{ return get_impl().has_space(); }

void test::Mem_stream_base::flush()
{ get_impl().flush(); }

}}  // cdk::foundation


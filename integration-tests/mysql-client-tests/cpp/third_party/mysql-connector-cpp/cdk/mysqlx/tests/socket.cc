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

#include "socket.h"


using namespace ::boost::asio;
using namespace ::boost::asio::ip;

using namespace cdk::foundation;

template<class boost_buffer_type>
class buffers_wrapper
{
  const buffers& m_bufs;

public:
  buffers_wrapper(const buffers& bufs)
    : m_bufs(bufs)
  {}

  // MutableBufferSequence requirements
  // http://www.boost.org/doc/libs/1_57_0/doc/html/boost_asio/reference/MutableBufferSequence.html
  class const_iterator
  {
    const buffers *m_bufs;
    size_t m_pos;

  public:
    const_iterator(const buffers *bufs= NULL, size_t pos= 0) : m_bufs(bufs), m_pos(pos)
    {}
    const_iterator(const const_iterator &src)
      : m_bufs(src.m_bufs), m_pos(src.m_pos)
    {}


    boost_buffer_type operator*()
    {
      bytes tmp= m_bufs->get_buffer(m_pos);
      return boost_buffer_type(tmp.begin(), tmp.size());
    }

    const_iterator& operator++()
    {
      ++m_pos;
      return *this;
    }

    const_iterator operator++(int)
    {
      const_iterator tmp(*this);
      ++*this;
      return tmp;
    }

    // Inlined friends are discussed here:
    // http://web.mst.edu/~nmjxv3/articles/templates.html
    // http://stackoverflow.com/questions/8207633/whats-the-scope-of-inline-friend-functions
    friend bool operator==(const const_iterator& x, const const_iterator& y)
    {
      return x.m_bufs == y.m_bufs && (x.m_bufs == NULL || x.m_pos == y.m_pos);
    }
  };

  const_iterator begin()
  {
    return const_iterator(&m_bufs);
  }

  const_iterator end()const
  {
    return const_iterator(&m_bufs, m_bufs.buf_count());
  }
};



cdk::api::Input_stream::Op& Socket::Connection::read(const buffers &bufs)
{
  m_howmuch= boost::asio::read(m_sock, buffers_wrapper<boost::asio::mutable_buffer>(bufs));
  return *this;
}

cdk::api::Output_stream::Op& Socket::Connection::write(const buffers &bufs)
{
  m_howmuch= boost::asio::write(m_sock, buffers_wrapper<boost::asio::const_buffer>(bufs));
  return *this;
}

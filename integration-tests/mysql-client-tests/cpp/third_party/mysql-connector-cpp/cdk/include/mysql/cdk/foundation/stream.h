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

#ifndef SDK_FOUNDATION_STREAM_H
#define SDK_FOUNDATION_STREAM_H

#include "types.h"
#include "async.h"
#include "opaque_impl.h"


namespace cdk {
namespace foundation {
namespace api {

class Stream_base
{
public:
  typedef Async_op<size_t> Op;
};


class Input_stream : public Stream_base
{
public:

  //class Error;

  virtual bool  eos() const =0;
  virtual bool  has_bytes() const =0;
};


class Output_stream : public Stream_base
{
public:

  //class Error;

  virtual bool  is_ended() const =0;
  virtual bool  has_space() const =0;
  virtual void  flush() =0;
};


class Connection
  : public Input_stream
  , public Output_stream
{
public:
  virtual ~Connection() {}
  virtual void connect() =0;
  virtual void close() =0;
  virtual bool is_closed() const =0;
};

}  // cdk::foundation::api

}}  // cdk::foundation


namespace cdk {
namespace foundation {


template<class X>
class Connection_class : public api::Connection
{
protected:

  typedef Connection_class<X> Base;

  class IO_op : public api::Async_op<size_t>
  {
  protected:

    X &m_conn;
    buffers m_bufs;
    const  time_t    m_deadline;
    size_t m_howmuch;
    bool m_completed;

    IO_op(X &conn, const buffers &bufs, time_t deadline =0)
    :  m_conn(conn), m_bufs(bufs), m_deadline(deadline)
    , m_howmuch(0), m_completed(false)
    {}

    size_t do_get_result() { return m_howmuch; }
    bool is_completed() const { return m_completed; }
    void set_completed(size_t howmuch)
    {
      m_howmuch= howmuch;
      m_completed= true;
    }
  };
};

}}  // cdk::foundation


namespace cdk {
namespace foundation {
namespace test {

/*
  In-memory stream for testing purposes
  =====================================

  Class Mem_stream<size> implements input/output stream that uses in-memory
  buffer of given size. Bytes written to output stream are stored in the buffer
  and can be read via input stream using CDK stream interfaces defined above.

  Implementation details of the stream are hidden in this public header using
  opaque implementation infrastructure (see opaque_impl.h for details). Since
  this infrastructure does not work with templates, we declare Mem_stream_base
  with opaque implementation and then define Mem_stream<size> template
  using that base.
*/


class Mem_stream_base
  : public Connection_class<Mem_stream_base>
  , opaque_impl<Mem_stream_base>
  , nocopy
{
protected:

  Mem_stream_base(byte*, size_t);
  class IO_op;

public:

  class Read_op;
  class Write_op;
  typedef Read_op  Read_some_op;
  typedef Write_op Write_some_op;

  void connect();
  void close();
  bool is_closed() const;
  bool eos() const;
  bool has_bytes() const;
  bool is_ended() const;
  bool has_space() const;
  void flush();
  void reset();
};


template <size_t size>
class Mem_stream
  : public Mem_stream_base
{
  byte m_buf[size];

public:

  Mem_stream() : Mem_stream_base(m_buf, size)
  {}
};


class Mem_stream_base::IO_op : public Base::IO_op
{
protected:

  IO_op(Mem_stream_base &str, const buffers &bufs, time_t deadline =0)
    :  Base::IO_op(str, bufs, deadline)
 {}

  // Async_op interface (trivial implementation)

  // LCOV_EXCL_START
  bool is_completed() const { return true; }
  bool do_cont() { return true; }
  void do_cancel() { THROW("not implemented"); }
  void do_wait() {}
  // LCOV_EXCL_STOP

  const api::Event_info* get_event_info() const { return  NULL; }
};


class Mem_stream_base::Read_op : public IO_op
{
public:

  Read_op(Mem_stream_base &str, const buffers &bufs, time_t deadline =0);
};

class Mem_stream_base::Write_op : public IO_op
{
public:

  Write_op(Mem_stream_base &str, const buffers &bufs, time_t deadline =0);
};

}}} // cdk::foundation::test

#endif

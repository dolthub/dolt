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

#ifndef SDK_FOUNDATION_ASYNC_H
#define SDK_FOUNDATION_ASYNC_H

#include "types.h"   // for nocopy
#include <stddef.h>  // for NULL

namespace cdk {
namespace foundation {
namespace api {

class Event_info
{
public:
  enum event_type {OTHER, SOCKET_RD, SOCKET_WR, ASYNC_OP };
  virtual event_type type() const { return OTHER; }
};


class Async_op_base : nocopy
{
public:

  virtual ~Async_op_base() {}

  virtual bool is_completed() const =0;

  virtual const Event_info* waits_for() const
  {
    if (is_completed()) return NULL;
    return get_event_info();
  }

  virtual void cancel()
  {
    if (is_completed()) return;
    do_cancel();
  }

  bool cont()
  {
    if (is_completed()) return true;
    return do_cont();
  }

  void wait()
  {
    if (is_completed()) return;
    do_wait();
  }

private:

  virtual bool do_cont() =0;
  virtual void do_wait() =0;
  virtual void do_cancel() =0;
  virtual const Event_info* get_event_info() const =0;
};



template  <typename T>
class Async_op
  : public Async_op_base
{
public:
  typedef T result_type;

  T get_result()
  {
    wait();
    return do_get_result();
  }

private:

  virtual T do_get_result() =0;
};


template<>
class Async_op<void>
  : public Async_op_base
{
};


}}} // cdk::foundation::api

#endif

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

#ifndef CDK_REPLY_H
#define CDK_REPLY_H

#include "common.h"
#include "api/reply.h"
#include "mysqlx.h"


namespace cdk {

class Session;
class Cursor;

class Reply
  : public api::Reply<Traits>
{
protected:

  // Note: Implementation might be shared with a Cursor instance.
  std::shared_ptr<mysqlx::Stmt_op> m_impl;
  typedef mysqlx::Stmt_op* Initializer;

public:

  Reply()
  {}

  Reply(Initializer _init)
    : m_impl(_init)
  {}

  Reply& operator=(Initializer _init)
  {
    m_impl.reset(_init);
    return *this;
  }

  // Reply interface

  bool end_of_reply() override
  {
    assert(m_impl);
    return !m_impl->check_results();
  }

  bool has_results() override
  {
    /*
      If we are in a state after consuming one result set and there is
      a next one available, next_result() will return true and proceed
      to reading it. Otherwise check_results() tells if we have a (not yet
      consumed) result set available.
    */

    return m_impl->next_result() || m_impl->check_results();
  }

  void skip_result() override
  {
    m_impl->discard_result();
    m_impl->wait();
    // Note: this moves to next result set, if present.
    m_impl->next_result();
  }

  void discard() override
  { m_impl->discard(); }

  row_count_t affected_rows() override
  { return m_impl->affected_rows(); }

  row_count_t last_insert_id()
  { return m_impl->last_insert_id(); }

  const std::vector<std::string>& generated_ids() const
  { return m_impl->generated_ids(); }

  // Diagnostics interface

  unsigned int entry_count(Severity::value level=Severity::ERROR) override
  { return m_impl->entry_count(level); }

  Diagnostic_iterator& get_entries(Severity::value level=Severity::ERROR) override
  { return m_impl->get_entries(level); }

  const Error& get_error() override
  { return m_impl->get_error(); }

  // Async_op interface

  bool is_completed() const override
  { return m_impl->is_completed(); }

private:

  // Async_op

  bool do_cont() override { return m_impl->cont(); }
  void do_wait() override { return m_impl->wait(); }
  void do_cancel() override { return m_impl->cancel(); }

  const cdk::api::Event_info* get_event_info() const override
  { return m_impl->get_event_info(); }


  friend class Session;
  friend class Cursor;
};

}

#endif

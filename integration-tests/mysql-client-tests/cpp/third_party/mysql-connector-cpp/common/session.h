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

#ifndef MYSQLX_COMMON_SESSION_INT_H
#define MYSQLX_COMMON_SESSION_INT_H

/*
  Internal implementations for public DevAPI classes.
*/

#include "common.h"

#include <mysql/cdk.h>

PUSH_SYS_WARNINGS
#include <list>
#include <mutex>
#include <condition_variable>
POP_SYS_WARNINGS

namespace mysqlx {

namespace impl {
namespace common {

using duration = std::chrono::milliseconds;
using system_clock = std::chrono::system_clock;
using time_point = std::chrono::time_point<system_clock>;


/*
   Session pooling
*/

/*
  Abstract interface used to clean up a session before it is closed.
*/

struct Session_cleanup
{
  virtual void cleanup() = 0;
};


/*
  Wraps a shared pointer to a CDK session that was created and is managed
  by a session pool.

  Pooled_session acts as an asynchronous operation. After construction one has
  to wait until it is completed -- only then the session is available.
*/

class Pooled_session
  : public cdk::foundation::api::Async_op<void>
  , public std::shared_ptr<cdk::Session>
{
  Session_pool_shared m_sess_pool;
  time_point m_deadline;
  Session_cleanup *m_cleanup = nullptr;

public:

  /*
    Get a session from the given pool, registering a cleanup handler to be
    called if the pool decides to close this session.
  */

  Pooled_session(Session_pool_shared &pool, Session_cleanup *cleanup = nullptr);

  Pooled_session(cdk::ds::Multi_source &ds);

  ~Pooled_session() override;

  bool is_completed() const override;
  bool do_cont() override;
  void do_wait() override;
  void do_cancel() override;
  const cdk::foundation::api::Event_info* get_event_info() const override;

  void release();

private:

  friend class Session_pool;

};


}  // common
}  // impl


MYSQLX_ABI_BEGIN(2,0)
namespace common {

using impl::common::duration;
using impl::common::time_point;
using impl::common::Pooled_session;
using impl::common::Session_cleanup;


/*
  Note: This class must be defined inside ABI namespace to preserve ABI
  compatibility (its name is used in public API)
*/

class Session_pool
{
public:

  Session_pool(cdk::ds::Multi_source &ds);

  ~Session_pool();

  void close();

  void set_pool_opts(Settings_impl &opts);

  void set_pooling(bool x)
  {
    m_pool_enable = x;
  }

  void set_size(size_t sz)
  {
    assert(sz > 0);
    m_max = sz;
  }

  void set_timeout(uint64_t ms)
  {
    if (!check_num_limits<int64_t>(ms))
      common::throw_error("Timeout value too big!");
    m_timeout = duration(static_cast<int64_t>(ms));
  }

  void set_time_to_live(uint64_t ms)
  {
    if (!check_num_limits<int64_t>(ms))
      common::throw_error("MaxIdleTime value too big!");
    m_time_to_live = duration(static_cast<int64_t>(ms));
  }


protected:

  void release_session(std::shared_ptr<cdk::Session> &sess);

  /*
    Returns Session if possible (available). Throws error if the pool is closed.
    If cleanup handler is given, it will be called in case this session needs
    to be closed while in use (for example, when pool is closed).
  */

  std::shared_ptr<cdk::Session> get_session(Session_cleanup* = nullptr);

  void time_to_live_cleanup();

  cdk::ds::Multi_source m_ds;
  bool m_pool_enable = true;
  bool m_pool_closed = false;
  size_t m_max = 25;
  duration m_timeout = duration::max();
  duration m_time_to_live = duration::max();

  struct Sess_data {
    time_point m_deadline;
    Session_cleanup *m_cleanup; 
  };

  std::map<cdk::shared_ptr<cdk::Session>, Sess_data> m_pool;
  std::recursive_mutex m_pool_mutex;
  std::mutex m_reelase_mutex;
  std::condition_variable m_release_cond;


  friend Pooled_session;
};



/*
  Internal implementation for Session objects.

  Note: This class must be defined inside ABI namespace to preserve ABI
  compatibility (its name is used in public API)

  TODO: Add transaction methods here?
*/

class Session_impl
  : public Session_cleanup
{
public:

  using string = cdk::string;

  Pooled_session      m_sess;
  string              m_default_db;
  std::set<uint32_t>  m_stmt_id;
  std::set<uint32_t>  m_stmt_id_cleanup;
  size_t              m_max_pstmt = std::numeric_limits<size_t>::max();

  Session_impl(Session_pool_shared &pool)
    : m_sess(pool, this)
  {
    m_sess.wait();
    if (m_sess->get_default_schema())
      m_default_db = *m_sess->get_default_schema();
    if (!m_sess->is_valid())
      m_sess->get_error().rethrow();
  }

  Session_impl(cdk::ds::Multi_source &ms)
    : m_sess(ms)
  {
    if (m_sess->get_default_schema())
      m_default_db = *m_sess->get_default_schema();
    if (!m_sess->is_valid())
      m_sess->get_error().rethrow();
  }

  Result_impl *m_current_result = nullptr;

  virtual ~Session_impl()
  {
    /*
      There should be no registered results when session implementation is
      deleted because:
      - each result has a shared pointer to session implementation,
      - session implementation is deleted only when the last result referring
        to it is deleted
      - results de-register themselves before being destroyed.
    */
    assert(!m_current_result);

    // TODO: rollback an on-going transaction, if any?
  }


  /*
    Result objects should register itself with the session and de-register
    when all result data is consumed (this is also the case when result object
    is deleted).
  */

  void register_result(Result_impl *result)
  {
    assert(!m_current_result);
    m_current_result = result;
  }

  void deregister_result(Result_impl *result)
  {
    if (result == m_current_result)
      m_current_result = nullptr;
  }

  /*
    Prepare session for sending new command. This caches the current result,
    if one is registered with session.
  */

  void prepare_for_cmd();

  unsigned long m_savepoint = 0;

  unsigned long next_savepoint()
  {
    return ++m_savepoint;
  }


  /*
    Return a non-used prepared statement id. If possible, re-uses previously
    allocated ids that are no longer in use.

    Returns 0 if prepared statements are not available at the moment.
  */
  uint32_t create_stmt_id()
  {
    /*
      If server doesn't support PS or or we reached server max PS (value set on
      m_max_pstmt when a error occur on prepare), it will return 0, so no PS
      possible.
    */
    if (!m_sess->has_prepared_statements() ||
      m_stmt_id.size() >= m_max_pstmt)
      return  0;

    uint32_t val = 1;
    if (!m_stmt_id_cleanup.empty())
    {
      //Use one that was freed
      val = *m_stmt_id_cleanup.begin();
      m_stmt_id.insert(val);
      m_stmt_id_cleanup.erase(m_stmt_id_cleanup.begin());

      //And clean up the others!
      clean_up_stmt_id();
    }
    else if (m_stmt_id.empty())
    {
      m_stmt_id.insert(val);
    }
    else
    {
      val = (*m_stmt_id.rbegin()) + 1;
      m_stmt_id.insert(val);
    }

    return val;
  }


  /*
    To be called when given PS id is no longer used.
  */
  void release_stmt_id(uint32_t id)
  {
    m_stmt_id.erase(id);
    m_stmt_id_cleanup.insert(id);
  }

  /*
    To be called when, while trying to use given PS, we have detected that the
    server can not handle more PS.
  */
  void error_stmt_id(uint32_t id)
  {
    m_stmt_id.erase(id);
    m_max_pstmt = m_stmt_id.size();
  }

  /*
    Send commands to server to deallocate PS ids that are no longer in use.
  */
  void clean_up_stmt_id()
  {
    if (m_stmt_id_cleanup.empty())
      return;

    m_sess->set_has_prepared_statements(true);

    for (auto id : m_stmt_id_cleanup)
    {
      cdk::Reply(m_sess->prepared_deallocate(id)).wait();
    }

    m_stmt_id_cleanup.clear();

  }


  void release();

  void cleanup() override
  {
    prepare_for_cmd();
  }
};


}  // common
MYSQLX_ABI_END(2,0)


}  // mysqlx namespace


#endif

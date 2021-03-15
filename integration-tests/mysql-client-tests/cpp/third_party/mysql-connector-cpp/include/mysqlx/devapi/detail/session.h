/*
 * Copyright (c) 2017, 2019, Oracle and/or its affiliates. All rights reserved.
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

#ifndef MYSQLX_DETAIL_SESSION_H
#define MYSQLX_DETAIL_SESSION_H

#include "../common.h"
#include "../crud.h"

#include <set>

namespace cdk {
  class Session;
}


namespace mysqlx {
MYSQLX_ABI_BEGIN(2,0)

class Value;
class Session;
class Schema;
class Table;
class Collection;
class CollectionOptions;

namespace common {
  class Session_impl;
  class Session_pool;
  using Shared_session_pool = std::shared_ptr<Session_pool>;
  class Result_init;
  class Result_impl;
}

namespace internal {

class Schema_detail;
using Client_impl = common::Session_pool;
using Shared_client_impl = std::shared_ptr<Client_impl>;
using Session_impl = common::Session_impl;
using Shared_session_impl = std::shared_ptr<common::Session_impl>;


/*
  Base class for database objects. Can't be used alone.
*/

class PUBLIC_API Db_obj_base
{
protected:

  DLL_WARNINGS_PUSH
  Shared_session_impl m_sess;
  string m_name;
  DLL_WARNINGS_POP

  Db_obj_base(const Shared_session_impl& sess, const string& name)
    : m_sess(sess), m_name(name)
  {}

  virtual ~Db_obj_base()
  {}
};


class PUBLIC_API Collection_detail
  : public Db_obj_base
{
protected:

  Collection_detail(const Shared_session_impl &sess, const string &name)
    : Db_obj_base(sess, name)
  {}

  virtual Schema_detail& get_schema() = 0;

  Result add_or_replace_one(const string &id, Value&&, bool);

  void index_drop(const string &name);
  void index_create(const string &name, Value &&spec);
};


// ---------------------------------------------------------------------------

/*
  Base class for classes to be used by common::List_source<> which get item
  from query results.

  It assumes that the first column in the results contains string
  data. An instance of this class iterates over the string data in the
  result until all rows are consumed.

  Derived class must send the query to the server and set m_res member to point
  at the result of this query.
*/

struct PUBLIC_API Query_src
{
  using Value = string;
  using Res_impl = common::Result_impl;

  Res_impl *m_res = nullptr;
  const void *m_row = nullptr;

public:

  Query_src()
  {}

  Query_src(Query_src &&other)
    : m_res(other.m_res)
  {
    // Note: only one instance of Query_src owns m_res.
    other.m_res = nullptr;
  }

  Query_src(const Query_src&) = delete;

  virtual ~Query_src();

  virtual void  iterator_start()
  {
    assert(m_res);
  }

  bool   iterator_next();
  string iterator_get();
};


// ---------------------------------------------------------------------------


class PUBLIC_API Schema_detail
  : public Db_obj_base
{
protected:

  enum Obj_type { COLLECTION, TABLE };

  /*
    Sources for lists of schema objects and their names.

    When constructing a source, a SQL style patter on object names is
    given as ctor parameter -- only object matching the pattern are listed.
    Name_src accepts a parameter which tells whether names of tables or
    collections should be listed.
  */

  struct PUBLIC_API Name_src
    : public Query_src
  {
    const Schema &m_schema;
    Name_src(const Schema&, Obj_type, const string &pattern);
  };

  struct PUBLIC_API Collection_src
    : Name_src
  {
    using Value = Collection;

    Collection_src(const Schema &sch, const string &pattern)
      : Name_src(sch, COLLECTION, pattern)
    {}

    using Name_src::iterator_start;
    using Name_src::iterator_next;
    Collection iterator_get();
  };

  struct PUBLIC_API Table_src
    : Name_src
  {
    using Value = Table;

    Table_src(const Schema &sch, const string &pattern)
      : Name_src(sch, TABLE, pattern)
    {}

    using Name_src::iterator_start;
    using Name_src::iterator_next;
    Table iterator_get();
  };

  Schema_detail(const Shared_session_impl &sess, const string &name)
    : Db_obj_base(sess, name)
  {}

public:

  using CollectionList = List_initializer<List_source<Collection_src>>;
  using TableList      = List_initializer<List_source<Table_src>>;
  using StringList     = List_initializer<List_source<Name_src>>;

protected:


  void  create_collection(const mysqlx::string &name,
                          CollectionOptions options);
  void  modify_collection(const mysqlx::string &name,
                         CollectionOptions options);
  void  drop_collection(const string &name);

  friend Collection_detail;

  struct Access;
  friend Access;
};


/*
  Class representing an SQL statement that can be executed on the server.
*/

struct SQL_statement;

using SQL_statement_cmd = Executable<SqlResult, SQL_statement>;

struct SQL_statement
  : public Bind_placeholders< SQL_statement_cmd >
{
  SQL_statement(Session *sess, const string &query)
  {
    assert(sess);
    try {
      reset(internal::Crud_factory::mk_sql(*sess, query));
    }
    CATCH_AND_WRAP
  }

  SQL_statement(SQL_statement_cmd &other)
  {
    SQL_statement_cmd::operator=(other);
  }

  SQL_statement(SQL_statement_cmd &&other)
  {
    SQL_statement_cmd::operator=(std::move(other));
  }
};


struct Session_detail;

struct PUBLIC_API Client_detail
{

  // Disable copy semantics for client class.

  Client_detail(const Client_detail&) = delete;
  Client_detail& operator=(const Client_detail&) = delete;



  Client_detail(common::Settings_impl &settings);
  //Client_detail(common::Settings_impl &&settings);

  void close();

protected:

  Client_detail(Client_detail && other)
  {
    m_impl = other.m_impl;
    other.m_impl.reset();
  }

  common::Shared_session_pool& get_session_pool();


  struct INTERNAL Impl;

  DLL_WARNINGS_PUSH
  Shared_client_impl  m_impl = NULL;
  DLL_WARNINGS_POP

  friend Session;
};


struct PUBLIC_API Session_detail
{
  // Disable copy semantics for session class.

  Session_detail(const Session_detail&) = delete;
  Session_detail& operator=(const Session_detail&) = delete;

  /*
    Sources for lists of schemata and schema names. Only schemata matching
    the given SQL-style pattern are listed.
  */

  struct PUBLIC_API Name_src
    : public Query_src
  {
    const Session &m_sess;
    Name_src(const Session&, const string &pattern);
  };

  struct PUBLIC_API Schema_src
    : Name_src
  {
    using Value = Schema;

    Schema_src(Session &sess, const string &pattern)
      : Name_src(sess, pattern)
    {}

    Schema_src(Session &sess)
      : Schema_src(sess, "%")
    {}

    using Name_src::iterator_start;
    using Name_src::iterator_next;
    Schema iterator_get();
  };

public:

  using SchemaList = List_initializer<List_source<Schema_src>>;

protected:

  Session_detail(Session_detail && other)
  {
    m_impl = other.m_impl;
    other.m_impl.reset();
  }


  struct INTERNAL Impl;

  /*
    Note: Session implementation is shared with result objects because it
    must exists as long as result implementation exists. This means that
    even when session object is deleted, its implementation can still hang
    around.
  */

  DLL_WARNINGS_PUSH
  Shared_session_impl  m_impl = NULL;
  DLL_WARNINGS_POP

  Session_detail(common::Settings_impl&);
  Session_detail(common::Shared_session_pool&);

  virtual ~Session_detail()
  {
    try {
      if (m_impl)
        close();
    }
    catch (...) {}
  }

  void create_schema(const string &name, bool reuse);
  void drop_schema(const string &name);
  string get_default_schema_name();

  void start_transaction();
  void commit();
  void rollback(const string &sp = string());
  string savepoint_set(const string &sp = string());
  void savepoint_remove(const string&);


  common::Session_impl& get_impl()
  {
    if (!m_impl)
      THROW("Invalid session");
    return *m_impl;
  }

  INTERNAL cdk::Session& get_cdk_session();

  void close();

  /*
    Do necessary cleanups before sending new command to the server.
  */
  void prepare_for_cmd();

public:

  /// @cond IGNORED
  friend Result_detail::Impl;
  friend internal::Crud_factory;
  /// @endcond
};


}  // internal namespace

MYSQLX_ABI_END(2,0)
}  // mysqlx namespace


#endif

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

#include <mysqlx/xdevapi.h>

PUSH_SYS_WARNINGS
#include <time.h>
#include <forward_list>
#include <list>
POP_SYS_WARNINGS

#include "impl.h"

using namespace ::mysqlx::impl::common;
using namespace ::mysqlx::internal;
using namespace ::mysqlx;

/*
  Code in this file defines implementations for various CRUD operations used
  by X DevAPI. We use common implementations of these operations.

*/


auto Crud_factory::mk_sql(Session &sess, const mysqlx::string &query)
-> Impl*
{
  return new Op_sql(sess.m_impl, query);
}



// --------------------------------------------------------------------

/*
  Collection CRUD operations
  ==========================
*/


auto Crud_factory::mk_add(Collection &coll) -> Impl*
{
  return new Op_collection_add(
    coll.get_session(), Object_ref(coll)
  );
}


auto Crud_factory::mk_remove(
  Collection &coll, const mysqlx::string &expr
) -> Impl*
{
  return new Op_collection_remove(
    coll.get_session(), Object_ref(coll), expr
  );
}


auto Crud_factory::mk_find(Collection &coll) -> Impl*
{
  return new Op_collection_find(
    coll.get_session(), Object_ref(coll)
  );
}


auto Crud_factory::mk_find(
  Collection &coll, const mysqlx::string &expr
) -> Impl*
{
  return new Op_collection_find(
    coll.get_session(), Object_ref(coll), expr
  );
}


auto Crud_factory::mk_modify(
  Collection &coll, const mysqlx::string &expr
) -> Impl*
{
  return new Op_collection_modify(
    coll.get_session(), Object_ref(coll), expr
  );
}


struct Replace_cmd
  : public Executable<Result, Replace_cmd>
{
  Replace_cmd(
    internal::Shared_session_impl sess,
    const cdk::api::Object_ref &coll,
    const std::string &id,
    const cdk::Expression &doc
  )
  {
    reset(new Op_collection_replace(
      sess, coll, id, doc
    ));
  }
};


struct Upsert_cmd : public Executable<Result, Upsert_cmd>
{
  Upsert_cmd(
    internal::Shared_session_impl sess,
    const cdk::api::Object_ref &coll,
    const std::string &id,
    cdk::Expression &doc
  )
  {
    reset(new Op_collection_upsert(
      sess, coll, id, doc
    ));
  }
};


/*
  A helper class used by Collection_detail::add_or_replace_one().

  It is a wrapper around CDK expression m_expr that describes a document.
  The wrapper forwards this description to a processor, but at the same
  time checks if the value of the (top-level) "_id" field equals the value
  given in the constructor.
*/

struct Value_expr_check_id
  : cdk::Expression
    , cdk::Expression::Processor
    , cdk::Expression::Processor::Doc_prc
{
  mysqlx::Value_expr &m_expr;
  bool m_is_expr;

  Processor *m_prc;
  Doc_prc *m_doc_prc;

  /*
    This class defines m_any_prc member which is used below
    to check the value of "_id" field as reported by the
    source expression. Before using this class, m_id_prc must be
    set to point at the sub-processor that was given for processing
    the value of the "_id" field (this is done by key_val() callback in
    the main class).

    Then all callbacks are forwarded to this sub-processor (or its
    sub-processors) and in case of calling scalar str() callback that
    gives string value of the "_id" field, the check is done first.
  */

  struct Any_processor_check
      : cdk::Expression::Processor::Doc_prc::Any_prc
      , cdk::Expression::Processor::Doc_prc::Any_prc::Scalar_prc
      , cdk::Expression::Processor::Doc_prc::Any_prc::Scalar_prc::Value_prc
  {
    Any_prc *m_id_prc;
    Scalar_prc *m_scalar_prc;
    Value_prc *m_value_prc;
    const std::string &m_id;

    Any_processor_check(const std::string& id)
      : m_id(id)
    {}

    // Any processor implementation

    Scalar_prc* scalar() override
    {
      m_scalar_prc = m_id_prc->scalar();
      return m_scalar_prc ? this : nullptr;
    }

    List_prc* arr() override
    {
      return m_id_prc->arr();
    }

    Doc_prc* doc() override
    {
      return m_id_prc->doc();
    }

    //Scalar processor implementation

    Value_prc* val() override
    {
      m_value_prc = m_scalar_prc->val();
      return m_value_prc ? this : nullptr;
    }

    Args_prc* op(const char *name) override
    {
      return m_scalar_prc->op(name);
    }
    Args_prc* call(const Object_ref&obj) override
    {
      return m_scalar_prc->call(obj);
    }

    void ref(const Column_ref &col, const Doc_path *path) override
    {
      return m_scalar_prc->ref(col, path);
    }
    void ref(const Doc_path &path) override
    {
      return m_scalar_prc->ref(path);
    }

    void param(const string &val) override
    {
      return m_scalar_prc->param(val);
    }

    void param(uint16_t val) override
    {
      return m_scalar_prc->param(val);
    }

    void var(const string &name) override
    {
      m_scalar_prc->var(name);
    }

    // Value processor implementation

    void null() override { m_value_prc->null();}

    void value(cdk::Type_info type,
                       const cdk::Format_info &format,
                       cdk::foundation::bytes val) override
    {
      m_value_prc->value(type, format, val);
    }

    void str(const string &val) override
    {
      if (m_id != val)
        throw mysqlx::Error(R"(Document "_id" and replace id are different!)");
      m_value_prc->str(val);
    }

    void num(int64_t  val) override { m_value_prc->num(val); }
    void num(uint64_t val) override { m_value_prc->num(val); }
    void num(float    val) override { m_value_prc->num(val); }
    void num(double   val) override { m_value_prc->num(val); }
    void yesno(bool   val) override { m_value_prc->yesno(val); }

  };

  Any_processor_check m_any_prc;

  Value_expr_check_id(mysqlx::Value_expr &expr, bool is_expr, const std::string& id)
    : m_expr(expr)
    , m_is_expr(is_expr)
    , m_any_prc(id)
  {}

  // Expression implementation

  void process(Processor& prc) const override
  {
    auto self = const_cast<Value_expr_check_id*>(this);
    self->m_prc = &prc;
    m_expr.process(*self);
  }

  // Expression processor implementation

  Scalar_prc* scalar() override
  {
    return m_prc->scalar();
  }

  List_prc* arr() override
  {
    return m_prc->arr();
  }

  Doc_prc* doc() override
  {
    m_doc_prc = m_prc->doc();
    return m_doc_prc ? this : nullptr;
  }

  // Doc_prc implementation

  void doc_begin() override
  {
    m_doc_prc->doc_begin();
  }

  void doc_end() override
  {
    m_doc_prc->doc_end();
  }

  Any_prc* key_val(const string &key) override
  {
    if (string("_id") == key )
    {
      if (m_is_expr)
        mysqlx::throw_error(
          R"(Document "_id" will be replaced by expression "_id")"
        );

      m_any_prc.m_id_prc = m_doc_prc->key_val(key);
      return m_any_prc.m_id_prc ? &m_any_prc : nullptr;
    }
    return m_doc_prc->key_val(key);
  }

};


Result
Collection_detail::add_or_replace_one(
  const mysqlx::string &id, mysqlx::Value &&doc, bool replace
)
{
  /*
    This is implemented by executing Replace_cmd or Upsert_command
    which internally use Op_collection_replace or Op_collection_upsert
    to perform relevant operation on the server.
  */

  Object_ref coll(get_schema().m_name, m_name);
  std::string id_str(id);


  if (!Value::Access::is_expr(doc) &&
      doc.getType() == Value::STRING)
  {
    doc = DbDoc(doc.get<string>());
  }

  /*
    expr is a CDK expression object which describes the document
    to be added.
  */

  Value_expr expr(doc, parser::Parser_mode::DOCUMENT);

  if (replace)
  {
    /*
      Replace_cmd executes Op_collection_replace which picks a document
      with the given id and replaes it with the document given as the last
      argument.

      The document expression is wrapped in Value_expr_check_id to check
      if the "_id" field (if present) stores the correct document id
      and throws error if it is not the case (otherwise Replace_cmd
      would modify the "_id" field to match the given id).
    */

    Value_expr_check_id check_id(expr, Value::Access::is_expr(doc), id_str);

    Replace_cmd cmd(m_sess, coll, id_str, check_id);
    return cmd.execute();
  }
  else
  {
    Upsert_cmd cmd(m_sess, coll, std::string(id), expr);
    return cmd.execute();
  }
}


void Collection_detail::index_drop(const mysqlx::string &name)
{
  Object_ref coll(get_schema().m_name, m_name);
  Op_idx_drop cmd(m_sess, coll, name);
  cmd.execute();
}

void
Collection_detail::index_create(
  const mysqlx::string &name, mysqlx::Value &&spec
)
{
  switch (spec.getType())
  {
  case Value::STRING:
    break;
  default:
    // TODO: support other forms: DbDoc, expr("{...}")?
    throw_error("Index specification must be a string.");
  }

  Object_ref coll(get_schema().m_name, m_name);
  Op_idx_create cmd(m_sess, coll, name, (std::string)spec);
  cmd.execute();

}


// --------------------------------------------------------------------

/*
  Table CRUD operations
  =====================
*/


auto Crud_factory::mk_insert(Table &table) -> Impl*
{
  return new Op_table_insert<Value>(
    table.get_session(), Object_ref(table)
  );
}


auto Crud_factory::mk_select(Table &table) -> Impl*
{
  return new Op_table_select(
    table.get_session(), Object_ref(table)
  );
}


auto Crud_factory::mk_update(Table &table) -> Impl*
{
  return new Op_table_update(
    table.get_session(), Object_ref(table)
  );
}


auto Crud_factory::mk_remove(Table &table) -> Impl*
{
  return new Op_table_remove(
    table.get_session(), Object_ref(table)
  );
}

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

/*
  Test mysqlx::Protocol class against xplugin, CRUD requests.
*/


#include "test.h"
#include "expr.h"
#include <list>


namespace cdk {
namespace test {
namespace proto {

using namespace ::std;
using cdk::protocol::mysqlx::Limit;


class Row_handler_crud
  : public Row_handler
  , Format_info
{
  public:
  std::vector<int> row_ids;

  Row_handler_crud()
  {}

  virtual void col_end(col_count_t col, size_t len)
  {
    if (col > 0 )
      return Row_handler::col_end(col, len);

    sint64_t id_val = 0;
    cdk::Codec<cdk::TYPE_INTEGER> codec(*this);
    codec.from_bytes(bytes(buf,buf+pos), id_val);
    cout << "INTEGER: " << id_val << endl;

    if ( row_ids.size() > m_row_num)
      EXPECT_EQ(row_ids[(unsigned)m_row_num], id_val);
  }

private:
  virtual bool for_type(Type_info type) const { return TYPE_INTEGER == type; }

  using Format_info::get_info;
  virtual void get_info(Format<TYPE_INTEGER>& fmt) const
  {
    Format<TYPE_INTEGER>::Access::set_fmt(fmt, Format<TYPE_INTEGER>::SINT);
  }

};


/*
  A row source class which sends single row with one numeric
  column. The value of the colum is given by named parameter ":value".

  For that reason, when using this row source one has to provide
  an Args_map object which defines value of the parameter. An instance
  of this class can act as such Args_map object in which case it
  defines ":value" to be the number set with set_val() method.
*/

class Row_source_args_crud
  : public protocol::mysqlx::Row_source
  , public protocol::mysqlx::api::Args_map
{
  sint64_t    m_val;
  row_count_t m_row_count;

public:

  Row_source_args_crud() : m_row_count(1)
  {}

  bool next()
  {
    if (m_row_count == 0)
      return false;
    --m_row_count;
    return true;
  }

  // Describe the row to a processor.

  void process(Row_source::Expr_list::Processor &ep) const
  {
    ep.list_begin();
    Expr_list::Processor::Element_prc *el =ep.list_el();
    if (el)
    {
      el->scalar()->placeholder("value");
    }

    ep.list_end();
  }

  // Describe parameter values to a processor.

  void process(protocol::mysqlx::api::Args_map::Processor &ep) const
  {
    ep.doc_begin();
    ep.key_val("value")->scalar()->num(m_val);
    ep.doc_end();
  }

  // Set value of the parameter.

  void set_val(sint64_t val) { m_val = val; }
};


class Update
    : public protocol::mysqlx::Update_spec
{
public:

  Update()
    : m_done(false)
  {

  }

  virtual void process(Processor& prc) const
  {
    prc.target_name(m_name);
    prc.update_op(update_op::SET)->scalar()->val()->num(m_value);
  }

  bool next()
  {
    if (m_done)
      return false;
    m_done = true;
    return true;
  }

  const string* get_name() { return &m_name; }
  const Doc_path* get_path() { return NULL; }
  const Db_obj* get_table() { return NULL; }

  void set_name(const string& name) { m_name = name; }
  void set_value(sint64_t val) { m_value = val; }

protected:
  bool m_done;
  sint64_t m_value;
  string m_name;
};


/*
  Class implementing protocol Find_spec interface, as required by
  Protocol methods which send CRUD commands.
*/

class Find
  : public protocol::mysqlx::Find_spec
{
  protocol::mysqlx::Db_obj m_obj;
  const Expression *m_expr;
  const Projection *m_proj;
  cdk::test::proto::Limit  m_lim;
  bool   m_has_lim;

public:

  Lock_mode_value m_lock_mode = Lock_mode_value::NONE;

  Find(const Db_obj &obj,
    const Expression *criteria, row_count_t limit, row_count_t skip = 0)
    : m_obj(obj)
    , m_expr(criteria), m_proj(NULL)
    , m_lim(limit, skip), m_has_lim(true)
  {}

  Find(const Db_obj &obj,
       const Expression *criteria = NULL,
       const Projection *proj = NULL)
    : m_obj(obj)
    , m_expr(criteria)
    , m_proj(proj)
    , m_has_lim(false)
  {}

private:

  const Db_obj& obj() const
  {
    return m_obj;
  }

  const Expression* select() const
  {
    return m_expr;
  }

  const Order_by* order() const
  {
    return NULL;
  }

  const Limit* limit() const
  {
    return m_has_lim ? &m_lim : NULL;
  }

  const Projection* project() const
  {
    return m_proj;
  }

  const Expr_list*  group_by() const
  {
    return NULL;
  }

  const Expression* having() const
  {
    return NULL;
  }

  const Lock_mode_value locking() const
  {
    return m_lock_mode;
  }
};


TEST_F(Protocol_mysqlx_xplugin, crud_basic)
{
  SKIP_IF_NO_XPLUGIN;

  authenticate();

  do_query(L"CREATE DATABASE IF NOT EXISTS crud_test_db");
  do_query(L"USE crud_test_db");
  do_query(L"DROP TABLE IF EXISTS crud_basic");
  do_query(L"CREATE TABLE crud_basic(id int primary key)");
  do_query(L"INSERT INTO crud_basic(id) VALUES (1),(2)");

  Protocol &proto= get_proto();

  Db_obj db_obj("crud_basic", "crud_test_db");
  Find   find1(db_obj, NULL, 1, 1);

  cout <<"Find" <<endl;
  proto.snd_Find(TABLE, find1).wait();

  cout <<"Metadata" <<endl;
  Mdata_handler mdh;
  proto.rcv_MetaData(mdh).wait();

  cout <<"Fetch rows" <<endl;
  Row_handler_crud rhc;

  rhc.row_ids.push_back(2);

  proto.rcv_Rows(rhc).wait();

  Stmt_handler sh;
  proto.rcv_StmtReply(sh).wait();  // Expect OK

  Row_source_args_crud rsc;
  rsc.set_val(3);

  Columns columns;
  cdk::string col_name = "id";
  columns.add_columns(&col_name, NULL);

  cout <<"Insert" <<endl;
  proto.snd_Insert(TABLE, db_obj, &columns, rsc, &rsc).wait();

  proto.rcv_MetaData(mdh).wait();

  sh.set_rows_check_num(1);        // Expect 1 row to be inserted
  proto.rcv_StmtReply(sh).wait();  // Expect Insert OK;

  Find find2(db_obj, NULL, 1, 2);
  cout <<"Checking inserted rows. Find." <<endl;
  proto.snd_Find(TABLE, find2).wait();

  cout <<"Metadata" <<endl;
  proto.rcv_MetaData(mdh).wait();


//  commented out because of data representation in the result
//  was encoded using protobuf::internal::WireFormatLite

  rhc.row_ids.clear();
  rhc.row_ids.push_back(3);

  cout <<"Fetch rows" <<endl;
  proto.rcv_Rows(rhc).wait();

  proto.rcv_StmtReply(sh).wait();  // Expect OK

  cout <<"Update rows"<<endl;

  Update upd;

  upd.set_name("id");
  upd.set_value(8);

  Find find3(db_obj, NULL, 1);

  proto.snd_Update(TABLE, find3, upd).wait();

  proto.rcv_MetaData(mdh).wait();

  sh.set_rows_check_num(1);        // Expect 1 row to be updated
  proto.rcv_StmtReply(sh).wait();  // Expect Update OK;

  cout <<"Checking updated rows. Find." <<endl;

  proto.snd_Find(TABLE, Find(db_obj)).wait();

  cout <<"Metadata" <<endl;
  proto.rcv_MetaData(mdh).wait();

  cout <<"Fetch rows" <<endl;
  rhc.row_ids.clear();
  rhc.row_ids.push_back(2);
  rhc.row_ids.push_back(3);
  rhc.row_ids.push_back(8);
  proto.rcv_Rows(rhc).wait();

  proto.rcv_StmtReply(sh).wait();  // Expect OK

  cout <<"Delete" <<endl;
  proto.snd_Delete(TABLE, Find(db_obj)).wait();

  proto.rcv_MetaData(mdh).wait();

  sh.set_rows_check_num(3);       // Expect 3 rows to be deleted
  proto.rcv_StmtReply(sh).wait(); // Expect Delete OK;
  sh.set_rows_check_num(-1);      // Reset row check counter

  // do the clean-up
  do_query(L"DROP TABLE IF EXISTS crud_basic");
}

using namespace expr;


/*
  Expression processor which prints expression representation
  to the given output stream.
*/

class Expr_printer
    : public api::Expression::Processor
    , public api::Expression::Processor::Scalar_prc
    , public api::Expression::Processor::List_prc
    , public api::Expression::Processor::Doc_prc
    , public api::Expression::Processor::Scalar_prc::Value_prc
{
  ostream &m_out;
  unsigned m_indent;

public:

  Expr_printer(ostream &out, unsigned ind =0)
    : m_out(out), m_indent(ind)
  {}

  ostream& out_ind()
  {
    for (unsigned i=0; i < 2*m_indent; ++i)
      m_out.put(' ');
    return m_out;
  }

  virtual Scalar_prc* scalar() { return this; }

  virtual List_prc*   arr() { return this; }

  virtual Doc_prc*    doc() { return this; }


  // Scalar_prc
  virtual Value_prc* val() { return this; }

  virtual void null()
  {
    out_ind() << "(NULL)" <<endl;
  }

  virtual void str(bytes val)
  {
    out_ind() <<"\"" <<std::string(val.begin(), val.end()) <<"\"" <<endl;
  }

  virtual void str(collation_id_t /*charset*/, bytes val)
  {
    out_ind() <<"\"" <<std::string(val.begin(), val.end()) <<"\"" <<endl;
  }

  virtual void num(int64_t val)
  {
    out_ind() << val <<endl;
  }

  virtual void num(uint64_t val)
  {
    out_ind() << val <<endl;
  }
  virtual void num(float val)
  {
    out_ind() << val <<endl;
  }

  virtual void num(double val)
  {
    out_ind() << val <<endl;
  }

  virtual void yesno(bool val)
  {
    out_ind() << val <<endl;
  }

  virtual void octets(bytes val, Octets_content_type)
  {
    out_ind() <<"\"" <<std::string(val.begin(), val.end()) <<"\"" <<endl;
  }


  virtual void constant(int fmt, bytes val)
  {
    out_ind();
    switch (fmt)
    {
    case val_fmt::VNULL: m_out <<"NULL"; break;

    case val_fmt::SINT:
      {
        Number_codec codec;
        int x;
        codec.from_bytes(val, x);
        m_out <<x;
      };
      break;

    case val_fmt::UINT:
      {
        Number_codec codec;
        unsigned x;
        codec.from_bytes(val, x);
        m_out <<x;
      };
      break;

    case val_fmt::STRING:
      m_out <<'"' <<std::string(val.begin(), val.end()) <<'"';
      break;

    default: m_out <<"literal (" <<val.size() <<" bytes, fmt " <<fmt <<")";
    }
    m_out <<endl;
  }

  virtual void var(const cdk::string &name)
  {
    out_ind() <<"@" <<name <<endl;
  }

  virtual void id(const cdk::string &name, const api::Db_obj *coll)
  {
    out_ind();
    if (coll)
    {
      m_out <<"`" <<(coll->get_name()) <<"`.";
    }
    m_out <<"`" <<name <<"`" <<endl;
  }

  void out_path(const api::Doc_path &path)
  {
    for (unsigned i = 0; i < path.length(); ++i)
    {
      if (i == 0)
        m_out <<"`";
      else
        m_out <<".`";
      m_out << path.get_name(i) <<"`";
    }
  }

  virtual void id(const cdk::string &name, const api::Db_obj *coll,
                  const api::Doc_path &path)
  {
    out_ind();
    if (coll)
    {
      m_out <<"`" <<(coll->get_name()) <<"`.";
    }
    out_path(path);
    m_out <<endl;
  }

  virtual void id(const Doc_path &path)
  {
    out_ind();
    out_path(path);
    m_out <<endl;
  }

  virtual Args_prc* call(const api::Db_obj &db_obj)
  {
    out_ind() << db_obj.get_name() << endl;
    return this;
  }

  virtual Args_prc* op(const char *name)
  {
    std::string op_call("operator \"");
    op_call.append(name);
    op_call.append("\"");
    return call(protocol::mysqlx::Db_obj(op_call));
  }

  virtual void placeholder()
  {
    out_ind() <<"?" <<endl;
  }

  virtual void placeholder(const cdk::string &name)
  {
    out_ind() <<":" <<name <<endl;
  }

  virtual void placeholder(unsigned pos)
  {
    out_ind() <<":" <<pos <<endl;
  }


  virtual Any_prc* key_val(const string &key)
  {
    out_ind() << key << " : ";
    return this;
  }

  //List Processor
  void list_begin()
  {
    out_ind() << "{" << endl;
    ++m_indent;
  }

  Element_prc* list_el()
  {
    return this;
  }

  void list_end()
  {
    --m_indent;
    out_ind() << "}" << endl;
  }
};


TEST(Protocol_mysqlx, crud_expr)
{
  Expr_printer ep(cout);

  cout <<endl <<"== expr 1 ==" <<endl<<endl;
  Op expr1(">", Field(L"foo"), Number((int64_t)7));
  expr1.process(ep);

  cout <<endl <<"== expr 2 ==" <<endl<<endl;
  Call call("append");
  call.add_arg(String("prefix_"));
  call.add_arg(Field(L"foo"));
  Op expr2("like", call, Parameter());
  expr2.process(ep);

  cout <<endl <<"== expr 3 ==" <<endl<<endl;
  Op expr3("&&", expr1, expr2);
  expr3.process(ep);

  cout <<endl;
}


TEST_F(Protocol_mysqlx_xplugin, crud_expr_query)
{
  SKIP_IF_NO_XPLUGIN;

  authenticate();

  do_query(L"CREATE DATABASE IF NOT EXISTS crud_test_db");
  do_query(L"USE crud_test_db");
  do_query(L"DROP TABLE IF EXISTS crud_expr");
  do_query(L"CREATE TABLE crud_expr(id int primary key, n int)");
  do_query(L"INSERT INTO crud_expr(id, n) VALUES (1, 3),(2, 3),(3, 5)");

  Protocol &proto= get_proto();

  Db_obj db_obj("crud_expr", "crud_test_db");
  cout <<"Find" <<endl;

  List my_expr_list;
  /*
    We are going to build an expression like
    (id > 1) && (n < (15 div 3))
  */

  Op op_div("/");
  op_div.add_arg(Number((int64_t)15));
  op_div.add_arg(Number((int64_t)3));

  Op less("<");
  less.add_arg(Field("n"));
  less.add_arg(op_div);

  Op greater(">");
  greater.add_arg(Field("id"));
  greater.add_arg(Number((int64_t)1));

  Op and_op("&&");
  and_op.add_arg(greater);
  and_op.add_arg(less);

  proto.snd_Find(TABLE, Find(db_obj, &and_op)).wait();

  cout <<"Metadata" <<endl;
  Mdata_handler mdh;
  proto.rcv_MetaData(mdh).wait();

  cout <<"Fetch rows" <<endl;
  Row_handler_crud rhc;
  // add a new id=2 to check the criteria
  rhc.row_ids.push_back(2);

  proto.rcv_Rows(rhc).wait();

  Stmt_handler sh;
  proto.rcv_StmtReply(sh).wait(); // Expect Find OK;

  /*
    Build the expression id=2, which should pick
    the same rows as in the previous expression for Find
  */
  Op op_equal("==");
  op_equal.add_arg(Field("id"));
  op_equal.add_arg(Number((uint64_t)2));

  Find find1(db_obj, &op_equal);

  cout <<"Delete" <<endl;
  proto.snd_Delete(TABLE, find1).wait();

  proto.rcv_MetaData(mdh).wait();


  sh.set_rows_check_num(1);       // Expect 3 rows to be deleted
  proto.rcv_StmtReply(sh).wait(); // Expect Delete OK;

  sh.set_rows_check_num(-1);      // Reset row check counter

  // now try again with the same criteria, the result should be empty
  proto.snd_Find(TABLE, find1).wait();

  cout <<"Metadata" <<endl;
  proto.rcv_MetaData(mdh).wait();

  cout <<"Fetch rows" <<endl;
  /*
    add a non-existing id to check the criteria
    if any result is returned it will trigger the error
  */
  rhc.row_ids.push_back(5000);

  proto.rcv_Rows(rhc).wait();

  proto.rcv_StmtReply(sh).wait(); // Expect Find OK;

  do_query(L"DROP TABLE IF EXISTS crud_expr");
}


TEST_F(Protocol_mysqlx_xplugin, crud_expr_args)
{
  SKIP_IF_NO_XPLUGIN;

  authenticate();

  do_query(L"CREATE DATABASE IF NOT EXISTS crud_test_db");
  do_query(L"USE crud_test_db");
  do_query(L"DROP TABLE IF EXISTS crud_expr");
  do_query(L"CREATE TABLE crud_expr(id int primary key, n int)");
  do_query(L"INSERT INTO crud_expr(id, n) VALUES (1, 3),(2, 3),(3, 5)");


  Protocol &proto= get_proto();

  Db_obj db_obj("crud_expr", "crud_test_db");
  cout <<"Find" <<endl;

  List my_expr_list;
  /*
    We are going to build an expression like
    (id > Param0) && (n < (15 div Param1))
    where Param0 = 1 and Param1 = 3
  */

  Op op_div("/");
  op_div.add_arg(Number((int64_t)15));
  op_div.add_arg(Parameter("Param1"));

  Op less("<");
  less.add_arg(Field("n"));
  less.add_arg(op_div);

  Op greater(">");
  greater.add_arg(Field("id"));
  greater.add_arg(Parameter("Param0"));

  Op and_op("&&");
  and_op.add_arg(greater);
  and_op.add_arg(less);

  Find find1(db_obj, &and_op);

  Args_map params;
  params.add("Param0", Param_Number((int64_t)1));
  params.add("Param1", Param_Number((int64_t)3));

  proto.snd_Find(TABLE, find1, &params).wait();

  cout <<"Metadata" <<endl;
  Mdata_handler mdh;
  proto.rcv_MetaData(mdh).wait();

  cout <<"Fetch rows" <<endl;
  Row_handler_crud rhc;
  // add a new id=2 to check the criteria
  rhc.row_ids.push_back(2);

  proto.rcv_Rows(rhc).wait();

  Stmt_handler sh;
  proto.rcv_StmtReply(sh).wait();  // Expect Update OK;

  Update upd;

  upd.set_name("id");
  upd.set_value(8);

  proto.snd_Update(TABLE, find1, upd, &params).wait();

  proto.rcv_MetaData(mdh).wait();

  sh.set_rows_check_num(1);        // Expect 1 row to be updated
  proto.rcv_StmtReply(sh).wait();  // Expect Update OK;

  proto.snd_Find(TABLE, find1, &params).wait();

  cout <<"Metadata" <<endl;
  proto.rcv_MetaData(mdh).wait();

  cout <<"Fetch rows" <<endl;

  rhc.row_ids.clear();
  // add a new id=8 to check the criteria
  rhc.row_ids.push_back(8);

  proto.rcv_Rows(rhc).wait();

  proto.rcv_StmtReply(sh).wait();  // Expect Update OK;

  proto.snd_Delete(TABLE, find1, &params).wait();

  proto.rcv_MetaData(mdh).wait();

  sh.set_rows_check_num(1);        // Expect 1 row to be deleted
  proto.rcv_StmtReply(sh).wait();  // Expect Update OK;

  proto.snd_Find(TABLE, Find(db_obj)).wait();

  cout <<"Metadata" <<endl;
  proto.rcv_MetaData(mdh).wait();

  rhc.row_ids.clear();
  // add a new id=1,3 to check the criteria
  rhc.row_ids.push_back(1);
  rhc.row_ids.push_back(3);

  proto.rcv_Rows(rhc).wait();
  proto.rcv_StmtReply(sh).wait();  // Expect Update OK;
}


TEST_F(Protocol_mysqlx_xplugin, crud_projections)
{
  SKIP_IF_NO_XPLUGIN;

  authenticate();

  do_query(L"CREATE DATABASE IF NOT EXISTS crud_test_db");
  do_query(L"USE crud_test_db");
  do_query(L"DROP TABLE IF EXISTS crud_expr");
  do_query(L"CREATE TABLE crud_expr(id int primary key, n int)");
  do_query(L"INSERT INTO crud_expr(id, n) VALUES (1, 3),(2, 3),(3, 5)");


  Protocol &proto= get_proto();

  Db_obj db_obj("crud_expr", "crud_test_db");

  cout <<"Find with projection" <<endl;

  struct : public api::Projection
  {
    void process(Processor &prc) const
    {
      prc.list_begin();

      Processor::Element_prc *ep;

      ep = prc.list_el();
      if (ep)
      {
        Op sum("+");
        sum.add_arg(Field("id"));
        sum.add_arg(Field("n"));

        sum.process_if(ep->expr());
        ep->alias("sum");
      }

      ep = prc.list_el();
      if (ep)
        safe_prc(ep->expr())->scalar()->val()->num((int64_t)127);

      prc.list_end();
    }
  }
  projection;

  Find find1(db_obj, NULL, &projection);

  proto.snd_Find(TABLE, find1).wait();

  cout <<"Metadata" <<endl;
  Mdata_handler mdh;
  proto.rcv_MetaData(mdh).wait();

  cout <<"Fetch rows" <<endl;
  Row_handler_crud rhc;
  rhc.row_ids.push_back(4);
  rhc.row_ids.push_back(5);
  rhc.row_ids.push_back(8);
  proto.rcv_Rows(rhc).wait();

  Stmt_handler sh;
  proto.rcv_StmtReply(sh).wait();  // Expect Update OK;

}

TEST_F(Protocol_mysqlx_xplugin, row_locking)
{
  SKIP_IF_NO_XPLUGIN;

  authenticate();
  SKIP_IF_SERVER_VERSION_LESS(8, 0, 3)

  do_query(L"DROP DATABASE IF EXISTS crud_test_db");
  do_query(L"CREATE DATABASE crud_test_db");
  do_query(L"USE crud_test_db");
  do_query(L"DROP TABLE IF EXISTS row_locking");
  do_query(L"CREATE TABLE row_locking(id int primary key)");
  do_query(L"INSERT INTO row_locking(id) VALUES (1),(2),(3)");

  do_query(L"BEGIN");

  Protocol &proto = get_proto();

  Db_obj db_obj("row_locking", "crud_test_db");
  Find   find1(db_obj, NULL, 10, 0);
  find1.m_lock_mode = Lock_mode_value::EXCLUSIVE;

  cout << "Find" << endl;
  proto.snd_Find(TABLE, find1).wait();

  cout << "Metadata" << endl;
  Mdata_handler mdh;
  proto.rcv_MetaData(mdh).wait();

  cout << "Fetch rows" << endl;
  Row_handler_crud rhc;

  rhc.row_ids.push_back(1);
  rhc.row_ids.push_back(2);
  rhc.row_ids.push_back(3);

  proto.rcv_Rows(rhc).wait();

  Stmt_handler sh;
  proto.rcv_StmtReply(sh).wait(); // Expect OK

  proto.snd_StmtExecute("sql",
    L"select IF(trx_rows_locked > 0, 1, 0) rows_locked from information_schema.innodb_trx " \
    "where trx_mysql_thread_id = connection_id()", NULL).wait();

  cout << "Metadata for locked rows number" << endl;
  proto.rcv_MetaData(mdh).wait();

  cout << "Fetch number of locked rows" << endl;

  // The number of rows locked will be the number of rows in the table + 1
  rhc.row_ids.clear();
  rhc.row_ids.push_back(1);
  proto.rcv_Rows(rhc).wait();
  proto.rcv_StmtReply(sh).wait(); // Expect OK

  do_query(L"COMMIT");
  do_query(L"DROP TABLE IF EXISTS row_locking");
}


}}}  // cdk::test::proto


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


#include "test.h"
//#include "expr.h"
#include <list>


#include "json_parser.h"
#include "../../../mysqlx/converters.h"  // for mysqlx::JSON_converter

namespace cdk {
namespace test {


using namespace ::std;
using namespace protocol::mysqlx::api;
using mysqlx::JSON_converter;
using mysqlx::Doc_converter;
using protocol::mysqlx::Db_obj;


/*
  Class Doc is a protocol document expression describing given JSON
  document.
*/

struct Doc : public Doc_converter
{
  parser::JSON_parser m_parser;
  JSON_converter      m_conv;

  Doc(const string &json)
    : m_parser(json)
  {
    m_conv.reset(m_parser);
    reset(m_conv);
  }

  using Doc_converter::process;
};


// -------------------------------------------------------------------------


/*
  Document and Scalar message checkers
  ====================================
  These classes act as processors for protocol documents and values. They check
  that given document or value is correctly described by a protobuf Expr
  message.
*/

struct Scalar_checker;
struct Array_checker;
struct Doc_checker;
struct Expr_checker;


struct Scalar_checker
  : public protocol::mysqlx::api::Scalar_processor
{
  typedef Mysqlx::Datatypes::Scalar Scalar;

  const Scalar &m_msg;

  Scalar_checker(const Scalar &msg) : m_msg(msg)
  {}

  virtual ~Scalar_checker() {}

  // value callbacks

  void null() { FAIL() <<"NULL value inside document"; }

  void str(bytes val)
  {
    EXPECT_EQ(Scalar::V_STRING, m_msg.type());
    EXPECT_TRUE(m_msg.has_v_string());
    EXPECT_EQ(std::string(val.begin(), val.end()), m_msg.v_string().value());
  }

  void str(collation_id_t, bytes val)
  {
    // TODO: check that cs is utf8
    str(val);
  }

  void num(uint64_t val)
  {
    EXPECT_EQ(Scalar::V_UINT, m_msg.type());
    EXPECT_TRUE(m_msg.has_v_unsigned_int());
    EXPECT_EQ(val, m_msg.v_unsigned_int());
  }

  void num(int64_t val)
  {
    EXPECT_EQ(Scalar::V_SINT, m_msg.type());
    EXPECT_TRUE(m_msg.has_v_signed_int());
    EXPECT_EQ(val, m_msg.v_signed_int());
  }

  void num(float val)
  {
    EXPECT_EQ(Scalar::V_FLOAT, m_msg.type());
    EXPECT_TRUE(m_msg.has_v_float());
    EXPECT_EQ(val, m_msg.v_float());
  }

  void num(double val)
  {
    EXPECT_EQ(Scalar::V_DOUBLE, m_msg.type());
    EXPECT_TRUE(m_msg.has_v_double());
    EXPECT_EQ(val, m_msg.v_double());
  }

  void yesno(bool val)
  {
    EXPECT_EQ(Scalar::V_BOOL, m_msg.type());
    EXPECT_TRUE(m_msg.has_v_bool());
    EXPECT_EQ(val, m_msg.v_bool());
  }

  // in documents obtained from parsing JSON strings, only scalar
  // values should be present, no expressions.

  void octets(bytes, Octets_content_type) { FAIL(); }
};


struct Expr_checker_base
  : public protocol::mysqlx::api::Expr_processor
{
  typedef Mysqlx::Expr::Expr   Msg;

  const Msg &m_msg;
  Expr_checker_base(const Msg &msg) : m_msg(msg)
  {}

  virtual ~Expr_checker_base() {}

  scoped_ptr<Scalar_checker> m_scalar_checker;

  Value_prc* val()
  {
    EXPECT_EQ(Msg::LITERAL, m_msg.type());
    EXPECT_TRUE(m_msg.has_literal());
    m_scalar_checker.reset(new Scalar_checker(m_msg.literal()));
    return m_scalar_checker.get();
  }

  // TODO: Implement full expression message structure checks

  Args_prc* op(const char *) { throw "Operator in expression"; }
  Args_prc* call(const Db_obj&) { throw "Function call in expression"; }

  void octets(bytes) { FAIL(); }
  void var(const string &) { FAIL(); }
  void id(const string &, const Db_obj *) { FAIL(); }
  void id(const string &, const Db_obj *, const Doc_path &)
  {
    FAIL();
  }
  void id(const Doc_path &) { FAIL(); }

  void placeholder() { FAIL(); }
  void placeholder(const string &) { FAIL(); }
  void placeholder(unsigned) { FAIL(); }
};


struct Array_checker
  : public protocol::mysqlx::api::Expr_list::Processor
{
  typedef Mysqlx::Expr::Expr   Expr_msg;
  typedef Mysqlx::Expr::Array  Msg;

  const Msg &m_msg;

  Array_checker(const Expr_msg &expr)
    : m_msg(expr.array())
  {
    EXPECT_EQ(Expr_msg::ARRAY, expr.type());
  }

  scoped_ptr<Expr_checker> m_expr_checker;

  Element_prc* list_el();

  uint64_t el_count;

  void list_end()
  {
    EXPECT_EQ(el_count, m_msg.value_size());
  }
};


// TODO: Trace path in document for failure reporting

struct Doc_checker
  : public protocol::mysqlx::api::Expression::Document::Processor
{
  typedef Mysqlx::Expr::Expr   Expr_msg;
  typedef Mysqlx::Expr::Array  Array_msg;
  typedef Mysqlx::Expr::Object Object_msg;

  const Object_msg *m_obj;

  Doc_checker(const Expr_msg &expr)
  {
    EXPECT_EQ(Expr_msg::OBJECT, expr.type());
    EXPECT_TRUE(expr.has_object());
    m_obj= &expr.object();
  }

  void doc_begin() {}
  void doc_end() {}  // TODO: check that there are no extra keys in m_obj

  scoped_ptr<Expr_checker> m_expr_checker;

  Any_prc* key_val(const string &key);

  const Expr_msg* find_key(const string &key)
  {
    for (int pos=0; pos < m_obj->fld_size(); ++pos)
    {
      if (key == string(m_obj->fld(pos).key()))
        return &m_obj->fld(pos).value();
    }
    return NULL;
  }

};


struct Expr_checker
  : public protocol::mysqlx::api::Expression::Processor
{
  typedef Mysqlx::Expr::Expr   Msg;
  const Msg &m_msg;

  Expr_checker(const Msg &msg) : m_msg(msg)
  {}

  scoped_ptr<Expr_checker_base> m_scalar_checker;

  Scalar_prc* scalar()
  {
    m_scalar_checker.reset(new Expr_checker_base(m_msg));
    return m_scalar_checker.get();
  }

  scoped_ptr<Array_checker> m_arr_checker;

  List_prc*  arr()
  {
    m_arr_checker.reset(new Array_checker(m_msg));
    return m_arr_checker.get();
  }

  scoped_ptr<Doc_checker> m_doc_checker;

  Doc_prc* doc()
  {
    m_doc_checker.reset(new Doc_checker(m_msg));
    return m_doc_checker.get();
  }
};


Array_checker::Element_prc*
Array_checker::list_el()
{
  Expr_msg msg = m_msg.value().Get(el_count++);
  //EXPECT_NE(NULL,msg);
  m_expr_checker.reset(new Expr_checker(msg));
  return m_expr_checker.get();
}


Doc_checker::Any_prc*
Doc_checker::key_val(const string &key)
{
  const Expr_msg *expr= find_key(key);
  EXPECT_TRUE(NULL != expr) <<"key " <<key <<" could not be found";
  m_expr_checker.reset(new Expr_checker(*expr));
  return m_expr_checker.get();
}


// -------------------------------------------------------------------------

/*
  Document list which acts as list of protocol expressions, each expression
  describing single document from the list.
*/


struct Doc_list_base
  : public protocol::mysqlx::api::Expression
  , public foundation::Iterator
{
  typedef protocol::mysqlx::api::Expression Expression;
};


template <uint32_t N>
struct Doc_list
  : public Doc_list_base
{

  const wchar_t **m_list;
  uint32_t m_pos;

  Doc_list(const wchar_t *list[N])
    : m_list(list), m_pos(0)
  {}

  bool next()
  {
    if (m_pos == N)
      return false;
    m_pos++;
    return true;
  }

  void process(Expression::Processor &prc) const
  {
    Doc doc(m_list[m_pos-1]);
    doc.process_if(prc.doc());
  }
};


/*
  A row source which sends rows with 2 columns. First column is a consecutive
  number (starting from 0), second column in N-th row is the N-th expression
  from given expression list.
*/

struct Expr_source
  : public protocol::mysqlx::Row_source
{
  Doc_list_base &m_docs;
  uint64_t m_cnt;

  Expr_source(Doc_list_base &docs)
    : m_docs(docs), m_cnt(0)
  {}

  void process(Processor &prc) const
  {
    prc.list_begin();
    safe_prc(prc)->list_el()->scalar()->val()->num(m_cnt);
    m_docs.process_if(prc.list_el());
    prc.list_end();
  }

  bool next()
  {
    return m_docs.next();
  }
};


/*
  Message processor which checks that Insert message generated from above
  row source has the expected structure. Each row should have two columns
  and second column should contain protocol message describing the
  corresponding document from the list.
*/

struct Insert_checker : public Msg_processor
{
  Doc_list_base &m_docs;

  Insert_checker(Doc_list_base &docs)
    : m_docs(docs)
  {}

  void process_msg(msg_type_t type, Message &msg)
  {
#if PRINT_MSG
    cout <<"==== msg of type: " <<type <<endl;
    cout <<msg.DebugString();
    cout <<"================" <<endl;
#endif
    switch (type)
    {
    case msg_type::cli_CrudInsert:
      {
        Mysqlx::Crud::Insert &ins= static_cast<Mysqlx::Crud::Insert&>(msg);

        for (int r=0; m_docs.next() && r < ins.row_size(); ++r)
        {
          cout <<"checking row#" <<r;
          const Mysqlx::Crud::Insert::TypedRow &row= ins.row(r);
          EXPECT_LE(2, row.field_size());
          // check that 2-nd field describes corresponding document from the list
          Expr_checker checker(row.field(1));
          m_docs.process(checker);
          cout <<" OK" <<endl;
        }
      };
      break;

    default: throw "Unexpected msg type";
    }
  }
};


TEST(Protocol_mysqlx_msg, insert)
{
  static const wchar_t *docs[] =
  {
    L"{\"str\": \"foo\", \"num\": 123, \"bool\": true}",
    L"{\"str\": \"bar\", \"doc\": {\"str\": \"foo\", \"num\": 123, \"bool\": true}}",
   // TODO: L"{\"str\": \"bar\", \"arr\": [ 1, \"two\", { \"three\": true }, false ]}",
  };

  TRY_TEST_GENERIC
  {
    Test_server<1024> srv;
    Protocol proto(srv.get_connection());

    Db_obj obj("schema", "name");
    Doc_list<sizeof(docs)/sizeof(wchar_t*)> dlist(docs);

    cout <<"== Sending Insert message" <<endl;

    Expr_source src(dlist);
    proto.snd_Insert(TABLE, obj, NULL, src);

    cout <<"== Checking received message" <<endl;

    Insert_checker checker(dlist);
    srv.rcv_msg(checker);

    cout <<"== Done!" <<endl;
  }
  CATCH_TEST_GENERIC;
}

}}  // cdk::test


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
 * which is part of <MySQL Product>, is also subject to the
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
#include "session_test.h"

#include <iostream>
#include <mysql/cdk.h>
#include <json_parser.h>
#include <expr_parser.h>
#include "../../mysqlx/converters.h"

#include <vector>
#include <map>
#include <sstream>


using ::std::cout;
using ::std::endl;
using ::std::vector;
using ::std::pair;
using ::std::map;

using namespace ::cdk;
using cdk::test::Schema_ref;
using cdk::test::Table_ref;
using parser::JSON_parser;
using parser::Expr_parser;
using parser::Parser_mode;
using cdk::mysqlx::JSON_converter;
using cdk::Any_prc_converter;


/*
  Value class can store integer or string value and act
  as CDK document value.
*/

struct Value
  : public Expression::Scalar
{
  enum { VNULL, STR, INT } m_type;
  string  m_str;
  int64_t m_int;

  Value() : m_type(VNULL)
  {}

  Value(const string &val) : m_type(STR), m_str(val)
  {}

  Value(int64_t val) : m_type(INT), m_int(val)
  {}

  void process(Processor &prc) const
  {
    switch (m_type)
    {
    case VNULL: safe_prc(prc)->val()->null(); return;
    case STR: safe_prc(prc)->val()->str(m_str); return;
    case INT: safe_prc(prc)->val()->num(m_int); return;
    }
  }
};



/*
  Document class used for testing.

  Each instance represents document with fields "_id", "name" and "age".

  Values of fields "_id" and "name" are given by members m_id and m_name,
  respectively. Value of field "age" is given by parameter ":NN_age" (the
  name of the parameter is returned by param_name() method).

  Document can contain extra fields taken from a JSON string given to
  set_extra() method.
*/

struct Doc
  : public Expression::Document
{
  typedef Expression::Document Document;
  typedef Param_source Param;
  typedef Document::string string;
  unsigned  m_id;
  string    m_name;
  string    m_extra;
  bool      m_has_extra;

  Doc() : m_id(0), m_has_extra(false)
  {}

  void set_extra(const string &extra)
  {
    m_extra = extra;
    m_has_extra = true;
  }

  unsigned id() const { return m_id; }
  const string& name() const { return m_name; }
  const string* extra() const { return m_has_extra ? &m_extra : NULL; }

  string param_name() const {
    std::stringstream ss;
    ss << m_id << "_age";
    return ss.str();
  }


  void process(Expression::Document::Processor &prc) const
  {
    Safe_prc<Expression::Document::Processor> sprc(prc);
    sprc->doc_begin();

    sprc->key_val("_id")->scalar()->val()->num((uint64_t)m_id);
    sprc->key_val("name")->scalar()->val()->str(m_name);

    sprc->key_val("age")->scalar()->param(param_name());

    /*
      If we have extra fields given by m_extra JSON string,
      create JSON_doc out of the string and process it using
      this instance as a processor. In the Document::Processor
      callbacks this forwards key-value pairs to the prc processor.
    */

    if (m_has_extra)
    {
      struct : public Expression::Document::Processor
      {
        Expression::Document::Processor *m_prc;
        Any_prc* key_val(const string &key)
        { return m_prc->key_val(key); }
      }
      doc_prc;
      doc_prc.m_prc = &prc;

      JSON_parser    extra(m_extra);
      JSON_converter conv(extra);
      conv.process(doc_prc);
    }

    sprc->doc_end();
  }

};


/*
  Source of docuemnts containing name/age data. Documents are added
  to the source using:

    Doc_list list;
    list.add(id, name, age);

  The add method adds Doc instance to the list, with given docuemnt
  data. Created document can have extra fields defined
  by JSON string passed as additional argument of add():

    list.add(id, name, age, "{ \"foo\": 7 }");

  Note: The age values are given via named parameters. When using
  this document source, one has to pass a Param_source object
  which defines values of the parameters. Method params() returns
  a reference to such parameter source.

  The same document data can be accessed as a sequence of rows
  with id, name, age and extra JSON document as fields in each row.
  Method rows() returns the corresponding Row_source object. Method
  create_table() creates a table into which such rows can be inserted.
  Definitions of table columns are given by m_col_defs[] table.
  Method columns() describes a projection into the columns used for
  this document data (column names as defined by m_col_defs[]).
*/

class Doc_list
  : public cdk::Doc_source
{
  typedef map<unsigned,Doc> doc_map;
  map<unsigned,Doc> m_docs;
  map<unsigned,Doc>::const_iterator m_it;
  bool m_at_begin;

  /*
    Object which defines values for parameters used to
    define "age" field of each document.
  */

  class Params
      : public Param_source
  {
    map<string, unsigned> m_map;

  public:

    void add(const string &name, unsigned age)
    {
      m_map[name] = age;
    }

    unsigned age(const string &name)
    {
      return m_map[name];
    }

    // report parameter values

    void process(Processor &prc) const
    {
      prc.doc_begin();

      for (map<string, unsigned>::const_iterator it = m_map.begin();
           it != m_map.end();
           ++it)
      {
        prc.key_val(it->first)->scalar()->num((uint64_t)it->second);
      }

      prc.doc_end();
    }

  } m_params;


public:

  Doc_list() : m_at_begin(true)
  {}

  Param_source & params()
  {
    return m_params;
  }

  void reset()
  {
    m_at_begin = true;
  }

  Doc& add(unsigned id, const string &name, unsigned age)
  {
    Doc &doc= m_docs[id];
    doc.m_id = id;
    doc.m_name = name;

    /*
      Add age as the value of the parameter used to define
      "age" field in the document.
    */
    m_params.add(doc.param_name(), age);

    return doc;
  }

  Doc& add(unsigned id, const string &name, unsigned age, const string &extra)
  {
    Doc &doc= add(id, name, age);
    doc.set_extra(extra);
    return doc;
  }

  unsigned count() const
  {
    return m_docs.size();
  }

  // get data corresponding to given id.

  const string& get_name(int id)
  {
    return m_docs[id].m_name;
  }

  unsigned get_age(int id)
  {
    return m_params.age(m_docs[id].param_name());
  }

  // Doc_source

  bool next()
  {
    if (m_at_begin)
    {
      m_at_begin = false;
      m_it = m_docs.begin();
    }
    else
      m_it++;
    return m_it != m_docs.end();
  }

  void process(Processor &prc) const
  {
    m_it->second.process_if(prc.doc());
  }

  // Row_source

  /*
    Row_source object which serves the same document
    data as a sequence of rows, with 4 fields in each row:
    - id    (number)
    - name  (string)
    - age   (named parameter)
    - extra (JSON string)
  */

  struct Rows : public cdk::Row_source
  {
    doc_map::const_iterator m_it;
    doc_map::const_iterator m_end;
    bool m_started;

    void reset(const doc_map &docs)
    {
      m_it = docs.begin();
      m_end = docs.end();
      m_started = false;
    }

    bool next()
    {
      if (m_it == m_end)
        return false;

      if (m_started)
        ++m_it;
      else
        m_started = true;

      return m_it != m_end;
    }

    void process(Processor &prc) const
    {
      Safe_prc<Processor> sprc(prc);
      const Doc &doc = m_it->second;

      prc.list_begin();

      // Report the 4 fields in the row

      sprc->list_el()->scalar()->val()->num((uint64_t)doc.id());
      sprc->list_el()->scalar()->val()->str(doc.name());
      sprc->list_el()->scalar()->param(doc.param_name());

      if (doc.extra())
        sprc->list_el()->scalar()->val()->str(*doc.extra());
      else
        sprc->list_el()->scalar()->val()->null();

      prc.list_end();
    }

  }
  m_rows;

  static struct col_def {
    const char *name;
    const char *type;
  }
  m_col_defs[];


  /*
    Class implementing api::Columns interface which can be used to restrict
    table_insert() operation to columns given by a col_def array.
  */

  struct Columns : public api::Columns
  {
    struct col_def *m_col;

    Columns(struct col_def *ptr) : m_col(ptr)
    {}

    void process(Processor &prc) const
    {
      Safe_prc<Processor> sprc(prc);

      prc.list_begin();

      for (struct col_def *ptr = m_col; NULL != ptr->name; ++ptr)
        sprc->list_el()->name(ptr->name);

      prc.list_end();
    }
  };


  Row_source& rows()
  {
    m_rows.reset(m_docs);
    return m_rows;
  }

  const api::Columns& columns()
  {
    static Columns columns(m_col_defs);
    return columns;
  }


  /*
    Create table for storing document data with columns as
    defined by m_col_defs[] array.
  */

  void create_table(Session&, const Table_ref &tbl);

};


Doc_list::col_def
Doc_list::m_col_defs[]
= {
    { "id",    "INT" },
    { "name",  "CHAR(32)" },
    { "age" ,  "INT" },
    { "extra", "JSON" },
    { NULL, NULL }
  };


void Doc_list::create_table(Session &sess, const Table_ref &tbl)
{
  std::stringstream query;

  query << "CREATE TABLE " <<tbl <<" (";
  for (col_def *ptr = m_col_defs; NULL != ptr->name; ++ptr)
  {
    if (ptr != m_col_defs)
      query <<", ";
    query <<ptr->name <<" " <<ptr->type;
  }
  query << ")" <<std::ends;

  Reply create(sess.sql(query.str()));
  create.wait();
  if (0 < create.entry_count())
    create.get_error().rethrow();
}

// ------------------------------------------------------------


typedef cdk::test::Row_processor<cdk::Meta_data> Row_processor;


/*
  Test fixture which creates collection named "coll" in schema
  "test" and populates it with data.

  It also acts as a Row_processor and stores data retrieved from
  the collection in `data` member.

  TODO: make it more generic so that it can process data from
  other collections or tables.
*/

class Session_crud
    : public cdk::test::Core_test
    , public ::Row_processor // to read rows
    , public cdk::JSON::Processor // to process document fields in rows
      // to process key values in document
    , public cdk::JSON::Processor::Any_prc
      // to process scalar key values
    , public cdk::JSON::Processor::Any_prc::Scalar_prc
{

public:

  // Object which represents the collection.
  static Table_ref coll;
  static Table_ref tbl;

  // List of documents to be inserted into collection. It is
  // populated in the constructor.

  Doc_list docs;

  void load_docs(cdk::Session&, const Table_ref &c =coll);
  void create_coll(cdk::Session&, const Table_ref &c =coll);
  void drop_coll(cdk::Session&, const Table_ref &c =coll);
  void drop_table(cdk::Session&, const Table_ref &t = tbl);

  // These members are set when processing rows.

  row_count_t row_count;
  typedef map<unsigned,::Value> Row;
  vector<Row> rows;

  // Prepare for processing rows using given result meta-data.

  void set_meta_data(cdk::Meta_data &md)
  {
    row_count= 0;
    rows.clear();
    Row_processor::set_meta_data(md);
  }

protected:

  Session_crud();

  void SetUp()
  {
    try {
      Core_test::SetUp();

      if (!has_xplugin())
        return;

      Session sess(this);
      create_coll(sess);
      drop_table(sess, tbl);
      docs.create_table(sess, tbl);
      load_docs(sess);
    }
    CATCH_TEST_GENERIC;
  }

  void TearDown()
  {
    try {
      if (has_xplugin())
      {
        Session sess(this);
        drop_coll(sess);
        drop_table(sess);
      }
      Core_test::TearDown();
    }
    CATCH_TEST_GENERIC;
  }

  // Row_processor

  bool row_begin(row_count_t row)
  {
    row_count++;
    return Row_processor::row_begin(row);
  }

  /*
    If row contains a document field, process this document to
    store data in a new Row in m_rows.
  */

  void process_field_doc(col_count_t pos, bytes data)
  {
    cdk::Codec<cdk::TYPE_DOCUMENT> codec;

    codec.from_bytes(data, *this);
    Row_processor::process_field_doc(pos, data);
  }

  // Scalar processor

  /*
    Expecting documents having `name` and `age` fields. These are stored in
    row as follows:

    m_row[0] - _id
    m_row[1] - name
    m_row[2] - age
  */

  Row m_row;
  string m_key;

  void num(uint64_t val)
  {
    if (m_key == L"_id")
      m_row[0] = ::Value(val);
    else
      // age
      m_row[2] = ::Value(val);
  }

  void num(int64_t val)
  {
    num((uint64_t)val);
  }

  void str(const string &val)
  {
    // name
    m_row[1] = ::Value(val);
  }

  // We do not expect fields of these types.

  void null()      { assert(false); }
  void num(float)  { assert(false); }
  void num(double) { assert(false); }
  void yesno(bool) { assert(false); }

  // Any processor

  Scalar_prc* scalar() { return this; }
  List_prc*   arr()    { return NULL; }
  Doc_prc*    doc()    { return NULL; }

  // JSON::Processor

  void doc_begin()
  {
    m_row.clear();
  }

  void doc_end()
  {
    rows.push_back(m_row);
  }

  Any_prc* key_val(const string &key)
  {
    m_key = key;
    if (key == L"_id" || key == L"name" || key == L"age")
      return this;
    return NULL;
  }

};



Table_ref Session_crud::coll("coll", "test");
Table_ref Session_crud::tbl("tbl", "test");

Session_crud::Session_crud()
{
  docs.add(1, "foo", 1);
  docs.add(2, "bar", 2);
  docs.add(3, "baz", 3, "{\"date\": { \"day\": 20, \"month\": \"Apr\" }}");
  docs.add(4, "foo", 7);
  docs.add(5, "buz", 17);
}


void Session_crud::load_docs(cdk::Session &sess, const Table_ref &coll)
{
  Reply r;

  r = sess.coll_add(coll, docs, &docs.params());
  r.wait();

  if (0 < r.entry_count())
    r.get_error().rethrow();

  // Insert the same data into the table

  r = sess.table_insert(tbl, docs.rows(), &docs.columns(), &docs.params());
  r.wait();

  if (0 < r.entry_count())
    r.get_error().rethrow();
}


void Session_crud::drop_table(cdk::Session &sess, const Table_ref &tbl)
{
  std::ostringstream query;

  query <<"DROP TABLE IF EXISTS " <<tbl;

  Reply drop(sess.sql(query.str()));
  drop.wait();

  if (0 < drop.entry_count())
  {
    drop.get_error().rethrow();
  }
}

void Session_crud::drop_coll(cdk::Session &sess, const Table_ref &coll)
{
  Reply drop(sess.admin("drop_collection",
                        static_cast<const cdk::Any::Document&>(coll)));
  drop.wait();

  if (0 < drop.entry_count()
      && cdk::server_error(1051) != drop.get_error().code())
  {
    // 1051 = unknown table
    drop.get_error().rethrow();
  }
}

void Session_crud::create_coll(cdk::Session &sess, const Table_ref &coll)
{
  drop_coll(sess, coll);
  Reply create(sess.admin("create_collection",
                          static_cast<const cdk::Any::Document&>(coll)));
  create.wait();

  if (0 < create.entry_count()
      && cdk::server_error(1050) != create.get_error().code())
  {
    // 1050 = table already exists
    create.get_error().rethrow();
  }
}



#define CRUD_TEST_BEGIN(Name) \
TEST_F(Session_crud, Name) \
{ \
  SKIP_IF_NO_XPLUGIN; \
  try {

#define CRUD_TEST_END \
  } CATCH_TEST_GENERIC \
} \


// -----------------------------------------------------------------


struct Expr : public parser::Expression_parser
{
  Expr(const string &expr)
    : Expression_parser(Parser_mode::DOCUMENT, expr)
  {}
};

struct TExpr : public parser::Expression_parser
{
  TExpr(const string &expr)
    : Expression_parser(Parser_mode::TABLE, expr)
  {}
};


class Path
  : public cdk::Doc_path
{
protected:

  std::vector<string> m_path;

public:

  Path()
  {}

  Path(const string &member)
  {
    add(member);
  }

  Path(const string &first, const string &second)
  {
    add(first);
    add(second);
  }

  void add(const string &member)
  {
    m_path.push_back(member);
  }

private:

  void process(Processor &prc) const
  {
    prc.list_begin();

    for (size_t pos = 0; pos < m_path.size(); ++pos)
      safe_prc(prc)->list_el()->member(m_path[pos]);

    prc.list_end();
  }

};


// =====================================================================

CRUD_TEST_BEGIN(find)
{
  Session sess(this);

  if (!sess.is_valid())
    FAIL() << "Invalid Session created";

  {
    cout << endl
         << "Fetch all documents from collection"
         << endl <<endl;

    Reply find(sess.coll_find(coll, NULL, NULL));
    Cursor c(find);

    set_meta_data(c);

    c.get_rows(*this);
    c.wait();

    EXPECT_EQ(docs.count(), row_count);
  }

  using namespace cdk::test;

  {
    const char * criteria_str = " age > 2 and (name LIKE 'ba%')";

    cout << endl
         << "Find documents which satisfy selection criteria:" << endl
         <<  criteria_str << endl
         << endl;

    Expr criteria(criteria_str);

    Reply find(sess.coll_find(coll, NULL, &criteria, NULL));
    Cursor c(find);

    set_meta_data(c);
    c.get_rows(*this);
    c.wait();

    EXPECT_EQ(1, row_count);
    EXPECT_LT(2, rows[0][2].m_int);  // age
    EXPECT_EQ(string("ba"), rows[0][1].m_str.substr(0,2));  // name
  }


  {
    const char * criteria_str = " age > 2000 and (name LIKE 'Jes%')";

    cout << endl
         << "Don't find documents which satisfy selection criteria:" << endl
         <<  criteria_str << endl
         << endl;

    Expr criteria(criteria_str);

    Reply find(sess.coll_find(coll, NULL, &criteria, NULL));
    Cursor c(find);

    set_meta_data(c);
    c.get_rows(*this);
    c.wait();

    EXPECT_EQ(0, row_count);

  }

  cout <<"Done!" <<endl;
}
CRUD_TEST_END



CRUD_TEST_BEGIN(update)
{

  /*
    Helper class to define update specification with given number
    of elements. Derived class should define the process() method.
  */

  struct Update_spec : public cdk::Update_spec
  {
    // Iterator

    unsigned m_pos;

    Update_spec(unsigned size) : m_pos(size)
    {}

    bool next()
    {
      if (0 == m_pos)
        return false;
      m_pos--;
      return true;
    }

  };

  using namespace cdk::test;

  static Path age("age");
  static Path name("name");

  Session sess(this);

  cout << endl
       << "Set age of persons with name 'foo' to 10"
       << endl << endl;

  {
    struct Update : public Update_spec
    {
      Update() : Update_spec(1)
      {}

      void process(Update_processor &prc) const
      {
        safe_prc(prc)->set(&age)->scalar()->val()->num((uint64_t)10);
      }
    }
    update_spec;

    static Expr which("name = 'foo'");

    Reply update(sess.coll_update(coll, &which, update_spec));
    update.wait();

    // Check data after update.

    Reply find(sess.coll_find(coll, NULL));
    Cursor c(find);
    set_meta_data(c);
    c.get_rows(*this);
    c.wait();
    cout <<endl;

    for (unsigned r=0; r < row_count; ++r)
    {
      unsigned      id   = rows[r][0].m_int;
      const string &name = rows[r][1].m_str;
      unsigned      age  = rows[r][2].m_int;

      if (name == L"foo")
        EXPECT_EQ(10, age);
      else
      {
        EXPECT_EQ(docs.get_age(id), age);
      }
    }
  }

  cout << endl
       << "Double age for all persons"
       << endl << endl;

  {
    struct Update : public Update_spec
    {
      Update() : Update_spec(1)
      {}

      void process(Update_processor &prc) const
      {
        static Expr expr("2 * age");
        expr.process_if(prc.set(&age));
      }
    }
    update_spec;

    Reply update(sess.coll_update(coll, NULL, update_spec));
    update.wait();

    // Check data after update.

    Reply find(sess.coll_find(coll, NULL));
    Cursor c(find);
    set_meta_data(c);
    c.get_rows(*this);
    c.wait();
    cout << endl;

    for (unsigned r=0; r < row_count; ++r)
    {
      unsigned      id   = rows[r][0].m_int;
      const string &name = rows[r][1].m_str;
      unsigned      age  = rows[r][2].m_int;

      if (name == L"foo")
        EXPECT_EQ(20, age);
      else
      {
        EXPECT_EQ(2*docs.get_age(id), age);
      }
    }
  }

  cout << endl
       << "Preform two updates for person with name 'baz'" <<endl
       << " - set date.day to 23," <<endl
       << " - add field date.year with value 2015"
       << endl << endl;

  {
    struct Update : public Update_spec
    {
      Update() : Update_spec(2)
      {}

      void process(Update_processor &prc) const
      {
        switch (m_pos)
        {
        case 1:
          {
            static Path path("date", "day");
            safe_prc(prc)->set(&path)->scalar()->val()->num((uint64_t)23);
          }
          return;

        case 0:
          {
            static Path path("date", "year");
            safe_prc(prc)->set(&path)->scalar()->val()->num((uint64_t)2015);
          }
          return;

        default: return;
        }
      }
    }
    update_spec;

    static Expr which("name = 'baz'");

    Reply update(sess.coll_update(coll, &which, update_spec));
    update.wait();

    // Show data after update.

    Reply find(sess.coll_find(coll, NULL, &which));
    Cursor c(find);
    set_meta_data(c);
    c.get_rows(*this);
    c.wait();

    // TODO: Add data checks
  }

  cout << endl
       << "Remove field date.year from person with name 'baz'"
       << endl << endl;

  {
    struct Update : public Update_spec
    {
      Update() : Update_spec(1)
      {}

      void process(Update_processor &prc) const
      {
        static Path path("date", "year");
        prc.remove(&path);
      }
    }
    update_spec;

    static Expr which("name = 'baz'");

    Reply update(sess.coll_update(coll, &which, update_spec));
    update.wait();

    // Show data after update.

    Reply find(sess.coll_find(coll, NULL, &which));
    Cursor c(find);
    set_meta_data(c);
    c.get_rows(*this);
    c.wait();

    // TODO: Add data checks
  }

  cout << endl
    << "Do a merge and change the document layout" << endl
    << " - rename name to name2" << endl
    << " - flatten date {day: 23, month: \"April\"} to" << endl
    << "   day2: 23, month2: \"April\""
    << endl << endl;

  {
    struct Update : public Update_spec
    {
      Update() : Update_spec(2)
      {}

      void process(Update_processor &prc) const
      {
          Safe_prc<Expression::Document::Processor> sprc(prc.patch()->doc());
          Path name_path("name");
          Path day_path("date", "day");
          Path mon_path("date", "month");

          sprc->doc_begin();
          // rename "name" to "name2"
          sprc->key_val("name2")->scalar()->ref(name_path);
          sprc->key_val("name")->scalar()->val()->null();

          // Flatten "date: { day : 23, month : apr }" into
          // "day2 : 23, month2: apr"
          sprc->key_val("date")->scalar()->val()->null();
          sprc->key_val("day2")->scalar()->ref(day_path);
          sprc->key_val("month2")->scalar()->ref(mon_path);
          sprc->doc_end();
      }
    }
    update_spec;

    static Expr which_update("name = 'baz'");

    Reply update(sess.coll_update(coll, &which_update, update_spec));
    update.wait();

    // Show data after update.
    static Expr which_find("name2 = 'baz' AND day2 = 23");
    Reply find(sess.coll_find(coll, NULL, &which_find));
    Cursor c(find);
    set_meta_data(c);
    c.get_rows(*this);
    c.wait();
    EXPECT_EQ(1, rows.size());

    // TODO: Add data checks
  }


  // TODO: array update operations
  // TODO: Update rows in a table

}
CRUD_TEST_END


CRUD_TEST_BEGIN(parameters)
{
  Session sess(this);

  if (!sess.is_valid())
    FAIL() << "Invalid Session created";


  struct Update_spec : public cdk::Update_spec
  {
    // Iterator

    unsigned m_pos;

    Update_spec(unsigned size) : m_pos(size)
    {}

    bool next()
    {
      if (0 == m_pos)
        return false;
      m_pos--;
      return true;
    }

  };

  using namespace cdk::test;

  {
    /*
      Expression: "name LIKE :name AND age > :age"
    */

    Expr expr("name LIKE :name AND age > :age");

    /*
      Define values for parameters :name and :age.
    */

    static string name_pattern("ba%");
    static int age_limit = 2;
    static int new_age = 10;

    /*
      Param_source class reports values of parameters to
      a Param_source::Processor using key_val() callbacks.
      Second argument of the callback reports the value of the parameter
      using Param_souce::Value::Processor callbacks.

      This Param_values instance acts as both Param_source and
      the Value reporting class.
    */

    struct Param_values
      : public Param_source
    {
       // Report parameter values.

      void process(Param_source::Processor &prc) const
      {
        safe_prc(prc)->key_val("name")->scalar()->str(name_pattern);
        safe_prc(prc)->key_val("age")->scalar()->num((int64_t)age_limit);
        safe_prc(prc)->key_val("new_age")->scalar()->num((int64_t)new_age);
      }
    }
    param_values;

    {
      Reply find(sess.coll_find(coll, NULL, &expr, NULL, NULL, NULL, NULL, NULL, &param_values));
      Cursor c(find);

      set_meta_data(c);
      c.get_rows(*this);
      c.wait();

      EXPECT_EQ(1, row_count);
      EXPECT_LT(age_limit, rows[0][2].m_int);
      EXPECT_EQ(name_pattern.substr(0,2), rows[0][1].m_str.substr(0,2));
    }

    {

      struct Update : public Update_spec
      {
        Update() : Update_spec(1)
        {}

        void process(Update_processor &prc) const
        {
          static Path age("age");
          safe_prc(prc)->set(&age)->scalar()->param("new_age");
        }
      }
      update_spec;

      //Update age to 10
      Reply update(sess.coll_update(coll, &expr, update_spec, NULL, NULL, &param_values));
    }

    {
      Reply find(sess.coll_find(coll, NULL, &expr, NULL, NULL, NULL, NULL, NULL, &param_values));
      Cursor c(find);

      set_meta_data(c);
      c.get_rows(*this);
      c.wait();

      EXPECT_EQ(1, row_count);
      EXPECT_EQ(new_age, rows[0][2].m_int);
      EXPECT_EQ(name_pattern.substr(0,2), rows[0][1].m_str.substr(0,2));
    }


    {
      Reply remove(sess.coll_remove(coll, &expr, NULL, NULL, &param_values));

      //TODO: Not Working!
//      EXPECT_EQ(1, remove.affected_rows());
    }

    {
      Reply find(sess.coll_find(coll, NULL, &expr, NULL, NULL, NULL, NULL, NULL, &param_values));
      Cursor c(find);

      set_meta_data(c);
      c.get_rows(*this);
      c.wait();

      EXPECT_EQ(0, row_count);
    }

  }

  cout <<"Done!" <<endl;
}
CRUD_TEST_END


CRUD_TEST_BEGIN(projections)
{
  Session sess(this);

  if (!sess.is_valid())
    FAIL() << "Invalid Session created";

  {
    cout << endl
         << "Fetch documents with projection"
         << endl <<endl;

    struct : public Expression::Document
    {
      void process(Processor &prc) const
      {
        Path name_path("name");
        Path age_path("age");
        Expr double_age("2*age");
        Safe_prc<Processor> sprc(prc);

        prc.doc_begin();

        sprc->key_val("name_proj")->scalar()->ref(name_path);
        double_age.process_if(sprc->key_val("age_proj"));
        sprc->key_val("extra.param")->scalar()->param("foo");
        sprc->key_val("extra.val")->scalar()->val()->str("bar");

        prc.doc_end();
      }
    }
    projection;

    struct : public Param_source
    {
      void process(Processor &prc) const
      {
        safe_prc(prc)->key_val("foo")->scalar()->str("foo");
      }
    }
    parameters;

    Reply find(
      sess.coll_find(coll,
                     NULL,        // view spec
                     NULL,        // where
                     &projection,
                     NULL,        // sort
                     NULL,        // group by
                     NULL,        // having
                     NULL,        // limit
                     &parameters)
    );

    Cursor c(find);

    set_meta_data(c);

    c.get_rows(*this);
    c.wait();

    EXPECT_EQ(docs.count(), row_count);
  }

  {
    // Using table mode.

    const char * criteria_str = " doc->$.age > 2 and (doc->$.name LIKE 'ba%')";

    cout << endl
         << "Project documents which satisfy selection criteria:" << endl
         <<  criteria_str << endl
         << endl;

    TExpr criteria(criteria_str);

    struct : public Projection
    {
      void process(Processor &prc) const
      {
        Processor::Element_prc *ep;

        prc.list_begin();

        /*
          Note: Without casts, result of extracting value from a document
          path is reported as value of type JSON. This confuses our result
          handling code which asumes that JSON values are full JSON documents.

          TODO: Fix this (MYC-176).
        */

        if ((ep = prc.list_el()))
        {
          TExpr proj(L"CAST(doc->$._id AS CHAR)");
          proj.process_if(ep->expr());
          // no alias
        }

        if ((ep = prc.list_el()))
        {
          TExpr proj(L"2 * doc->$.age");
          proj.process_if(ep->expr());
          ep->alias(L"double age");
        }

        if ((ep = prc.list_el()))
        {
          TExpr proj(L"CAST(doc->$.date.day AS UNSIGNED)");
          proj.process_if(ep->expr());
          ep->alias(L"day");
        }

        prc.list_end();
      }
    }
    projection;

    Reply find(sess.table_select(coll, NULL, &criteria, &projection));
    Cursor c(find);

    set_meta_data(c);
    c.get_rows(*this);
    c.wait();

    EXPECT_EQ(1, row_count);
  }

  cout <<"Done!" <<endl;
}
CRUD_TEST_END


CRUD_TEST_BEGIN(insert)
{
  Session sess(this);


  /*
    Insert pair ("insert test",23) into columns
    "name", "age".
  */

  static const string   name("insert test");
  static const uint64_t age(23);

  struct : public Row_source
  {
    void process(Processor &prc) const
    {
      Safe_prc<Processor> sprc(prc);

      prc.list_begin();
      sprc->list_el()->scalar()->val()->str(name);
      sprc->list_el()->scalar()->val()->num(age);
      prc.list_end();
    }

    bool m_at_begin;

    bool next()
    {
      if (!m_at_begin)
        return false;
      m_at_begin = false;
      return true;
    }
  }
  data;
  data.m_at_begin = true;

  struct : public api::Columns
  {
    void process(Processor &prc) const
    {
      Safe_prc<Processor> sprc(prc);

      prc.list_begin();
      sprc->list_el()->name("name");
      sprc->list_el()->name("age");
      prc.list_end();
    }
  }
  columns;

  Reply insert(sess.table_insert(tbl, data, &columns, NULL));
  insert.wait();
  if (0 < insert.entry_count())
    insert.get_error().rethrow();

  // Show table contents after insertion.

  {
    Reply select(sess.table_select(tbl));
    Cursor c(select);

    set_meta_data(c);
    c.get_rows(*this);
    c.wait();
  }

  // Check that row was inserted with remaining columns set to NULL

  {
    TExpr  cond("id IS NULL AND extra IS NULL");
    Reply check(sess.table_select(tbl, NULL, &cond));
    Cursor c(check);
    set_meta_data(c);
    c.get_rows(*this);
    c.wait();
    EXPECT_EQ(1, row_count);
  }

  cout <<"Done!" <<endl;
}
CRUD_TEST_END


CRUD_TEST_BEGIN(group_by)
{
  Session sess(this);

  if (!sess.is_valid())
    FAIL() << "Invalid Session created";

  /*
    Note: By default sql_mode ONLY_FULL_GROUP_BY is enabled.
    This result in errors from queries generated by xplugin
    (select list refers to fields not listed in GROUP BY clause).
    See: https://dev.mysql.com/doc/refman/en/sql-mode.html#sqlmode_only_full_group_by
  */
  Reply set_mode(sess.sql("set sql_mode=''"));
  set_mode.wait();

  {
    cout << endl
         << "Fetch documents grouped by name"
         << endl <<endl;

    /*
      Group_by specification which is a list of expressions.

      In this case we have a single expression which is a reference
      to the "name" field inside each document.
    */

    struct : public Expr_list
    {
      void process(Processor &prc) const
      {
        static Path path("name");

        prc.list_begin();
        safe_prc(prc)->list_el()->scalar()->ref(path);
        prc.list_end();
      }
    }
    group_by;

    /*
      Project to documents of the form:

       { name: X, count: N }

      where N is the number of original documents with
      name X.
    */

    struct : public Expression::Document
    {
      void process(Processor &prc) const
      {
        static Path path("name");
        static Expr count("count(*)");
        Safe_prc<Processor> sprc(prc);

        prc.doc_begin();
        sprc->key_val("name")->scalar()->ref(path);
        count.process_if(sprc->key_val("count"));
        prc.doc_end();
      }
    }
    projection;

    /*
      TODO: There is problem with group_by + having on
      document collections - bug#23738896. Re-enable this when
      the bug is fixed.
    */

#if 0
    // Select only these entries for which count > 1.

    Expr having("count > 1");
#endif

    Reply find(
      sess.coll_find(coll,
                     NULL, // view spec
                     NULL, // where
                     &projection,
                     NULL, // sort
                     &group_by,
                     NULL, //&having,
                     NULL, // limit
                     NULL) // parameters
    );

    Cursor c(find);

    set_meta_data(c);

    c.get_rows(*this);
    c.wait();

    EXPECT_LE(1, row_count);
  }

  {
    cout << endl
         << "Fetch rows grouped by name"
         << endl <<endl;

    struct : public Expr_list
    {
      void process(Processor &prc) const
      {
        static TExpr path("`name`");

        prc.list_begin();
        path.process_if(safe_prc(prc)->list_el());
        prc.list_end();
      }
    }
    group_by;

    struct : public Projection
    {
      void process(Processor &prc) const
      {
        static TExpr name("`name`");
        static Expr count("count(*)");

        prc.list_begin();

        name.process_if(safe_prc(prc)->list_el()->expr());

        Processor::Element_prc *eprc = safe_prc(prc)->list_el();
        if (eprc)
        {
          count.process_if(eprc->expr());
          eprc->alias("count");
        }

        prc.list_end();
      }
    }
    projection;

    TExpr having("count > 1");

    Reply find(
      sess.table_select(tbl,
                        NULL, // view spec
                        NULL, // where
                        &projection,
                        NULL, // sort
                        &group_by,
                        &having,
                        NULL, // limit
                        NULL) // parameters
    );

    Cursor c(find);

    set_meta_data(c);

    c.get_rows(*this);
    c.wait();

    EXPECT_LE(1, row_count);
  }

  cout <<"Done!" <<endl;
}
CRUD_TEST_END


CRUD_TEST_BEGIN(views)
{
  Session sess(this);

  if (!sess.is_valid())
    FAIL() << "Invalid Session created";

  cout << "Session established" << endl;

  struct View_spec : cdk::View_spec
  {
    Table_ref v;
    String_list  *columns;
    cdk::View_spec::Options *opts;

    View_spec()
      : v("view", "test")
      , columns(NULL)
      , opts(NULL)
    {}

    void process(Processor &prc) const
    {
      prc.name(v);
      if (columns)
        columns->process_if(prc.columns());
      if (opts)
        opts->process_if(prc.options());
    }
  }
  view;

  cout << "Creating collection view..." << endl;

  // Drop the view first, if it already exists.

  {
    Reply drop(sess.view_drop(view.v));
    drop.wait();
    if (0 < drop.entry_count())
      drop.get_error().rethrow();
  }

  {
    struct : public View_spec::Options
    {
      void process(Processor &prc) const
      {
        prc.security(View_security::DEFINER);
        prc.check(View_check::LOCAL);
      }
    }
    view_opts;

    view.opts = &view_opts;

    struct : public Expression::Document
    {
      void process(Processor &prc) const
      {
        Path name_path("name");
        Path age_path("age");
        Expr double_age("2*age");
        Safe_prc<Processor> sprc(prc);

        prc.doc_begin();

        sprc->key_val("name_proj")->scalar()->ref(name_path);
        double_age.process_if(sprc->key_val("age_proj"));
        sprc->key_val("extra.orig_age")->scalar()->ref(age_path);
        sprc->key_val("extra.val")->scalar()->val()->str("bar");

        prc.doc_end();
      }
    }
    projection;

    Expr cond("name LIKE 'ba%'");

    Reply create(sess.coll_find(coll, &view, &cond, &projection));
    create.wait();
    if (0 < create.entry_count())
      create.get_error().rethrow();
  }

  cout << "View created, querying it..." << endl;

  {
    Reply select(sess.coll_find(view.v));
    select.wait();

    cout << "Got reply..." << endl;

    Cursor c(select);

    set_meta_data(c);
    c.get_rows(*this);
    c.wait();
  }


  cout << "Creating table view..." << endl;

  // Drop the view first.

  {
    Reply drop(sess.view_drop(view.v, false));
    drop.wait();
    if (0 < drop.entry_count())
      drop.get_error().rethrow();
  }

  {
    struct : public View_spec::Options
    {
      void process(Processor &prc) const
      {
        prc.security(View_security::INVOKER);
        prc.algorithm(View_algorithm::UNDEFINED);
      }
    }
    view_opts;

    view.opts = &view_opts;

    struct : public Projection
    {
      void process(Processor &prc) const
      {
        Processor::Element_prc *ep;

        prc.list_begin();

        if ((ep = prc.list_el()))
        {
          TExpr proj(L"name");
          proj.process_if(ep->expr());
          // no alias
        }

        if ((ep = prc.list_el()))
        {
          TExpr proj(L"2 * age");
          proj.process_if(ep->expr());
          ep->alias(L"double age");
        }

        prc.list_end();
      }
    }
    projection;

    struct : public cdk::String_list
    {
      void process(Processor &prc) const
      {
        prc.list_begin();
        safe_prc(prc)->list_el()->val("view_name");
        safe_prc(prc)->list_el()->val("view_age");
        prc.list_end();
      }
    }
    columns;

    view.columns = &columns;

    TExpr cond("name LIKE 'ba%'");

    Reply create(sess.table_select(tbl, &view, &cond, &projection));
    create.wait();
    if (0 < create.entry_count())
      create.get_error().rethrow();
  }

  cout << "View created, querying it..." << endl;

  {
    Reply select(sess.table_select(view.v));
    select.wait();

    cout << "Got reply..." << endl;

    Cursor c(select);

    EXPECT_EQ(L"view_name", c.col_info(0).name());
    EXPECT_EQ(L"view_age", c.col_info(1).name());

    set_meta_data(c);
    c.get_rows(*this);
    c.wait();
  }


  cout <<"Done!" <<endl;
}
CRUD_TEST_END


CRUD_TEST_BEGIN(upsert)
{
  Session sess(this);
  SKIP_IF_SERVER_VERSION_LESS(sess, 8, 0, 3)

  Doc_list doc_list;
  doc_list.add(1, "coo", 10);
  doc_list.add(2, "roo", 20);
  doc_list.add(3, "moo", 30);

  Reply r = sess.coll_add(coll, doc_list, &doc_list.params());
  r.wait();

  Doc_list upsert_list;
  upsert_list.add(1, "zoo", 40);
  r = sess.coll_add(coll, upsert_list,
                   &upsert_list.params(), true);
  r.wait();
  EXPECT_EQ(0, r.entry_count());

  Doc_list no_upsert_list;
  no_upsert_list.add(1, "noo", 50);
  r = sess.coll_add(coll, no_upsert_list,
                    &no_upsert_list.params(), false);
  r.wait();
  EXPECT_EQ(1, r.entry_count());

  cout << "Done!" << endl;

}
CRUD_TEST_END

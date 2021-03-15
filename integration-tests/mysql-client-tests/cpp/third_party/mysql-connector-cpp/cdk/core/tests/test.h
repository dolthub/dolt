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

#ifndef MYSQL_CDK_CORE_TESTS_TEST_H
#define MYSQL_CDK_CORE_TESTS_TEST_H

#include "xplugin_test.h"
#include <mysql/cdk.h>

namespace cdk {
namespace test {


class Core_test
  : public cdk::test::Xplugin
{
  scoped_ptr<ds::TCPIP> m_ds;

public:

  ds::TCPIP& get_ds()
  {
    if (!m_ds)
      m_ds.reset(new ds::TCPIP(m_host, m_port));
    return *m_ds;
  }

  ds::TCPIP::Options& get_opts()
  {
    // TODO: make it configurable through env. variables
    static ds::TCPIP::Options opts("root", NULL);
    opts.set_database("test");
    return opts;
  }

  class Session : public cdk::Session
  {
  public:

    Session(Core_test *fixture)
      : cdk::Session(fixture->get_ds(), fixture->get_opts())
    {}

    bool is_server_version_less(int test_upper_version,
                                int test_lower_version,
                                int test_release_version)
    {
      Reply reply(sql("SELECT VERSION()"));
      reply.wait();
      Cursor cursor(reply);

      struct : cdk::Row_processor
      {
        cdk::string version;

        virtual bool row_begin(row_count_t row)
        { return true; }

        virtual void row_end(row_count_t row)
        {}

        virtual void field_null(col_count_t pos)
        {}

        virtual size_t field_begin(col_count_t pos, size_t)
        { return  SIZE_MAX; }

        size_t field_data(col_count_t pos, bytes data)
        {
          cdk::foundation::Codec<cdk::foundation::Type::STRING> codec;
          // Trim trailing \0
          bytes d1(data.begin(), data.end() - 1);
          codec.from_bytes(d1, version);
          return 0;
        }

        virtual void field_end(col_count_t /*pos*/)
        {}

        virtual void end_of_data()
        {}
      }
      prc;

      cursor.get_rows(prc);
      cursor.wait();

      std::stringstream version;
      version << std::string(prc.version);

      int upper_version, minor_version, release_version;
      char sep;
      version >> upper_version;
      version >> sep;
      version >> minor_version;
      version >> sep;
      version >> release_version;

      if ((upper_version < test_upper_version) ||
        (upper_version == test_upper_version &&
         minor_version << test_lower_version) ||
         (upper_version == test_upper_version &&
          minor_version == test_lower_version &&
          release_version < test_release_version))
      {
        return true;
      }
      return false;
    }

  };

};

#define SKIP_IF_SERVER_VERSION_LESS(CDK_SESS, x,y,z)\
  if (CDK_SESS.is_server_version_less(x, y, z)) \
  {\
    std::cerr <<"SKIPPED: " << \
    "Server version not supported (" \
    << x << "." << y <<"." << ")" << z <<std::endl; \
    return; \
  }


template <class X>
struct Helper
{
  unsigned m_pos;

  void set_pos(unsigned pos) const
  {
    const_cast<Helper*>(this)->m_pos = pos;
  }

  X& get_ref() const
  {
    return *const_cast<X*>(static_cast<const X*>(this));
  }
};


class Schema_ref : public cdk::api::Schema_ref
{
  const cdk::string m_name;
  bool  m_null;

public:

  const cdk::string name() const { return m_name; }
  bool is_null() const { return m_null; }

  Schema_ref(const cdk::string &name) : m_name(name), m_null(false) {}
  Schema_ref() : m_null(true) {}
};


class Table_ref
  : public cdk::api::Table_ref
  // to be able to pass schema.name as admin command arguments
  , public cdk::Any::Document
{
  Schema_ref m_schema;
  const cdk::string m_name;

public:

  const cdk::string name() const { return m_name; }
  const cdk::api::Schema_ref* schema() const { return m_schema.is_null() ? NULL : &m_schema; }

  Table_ref(const cdk::string &name)
    : m_name(name)
  {}

  Table_ref(const cdk::string &name, const cdk::string &schema)
    : m_schema(schema), m_name(name)
  {}

  friend std::ostream& operator<<(std::ostream &out, const Table_ref &tbl);

private:

  /*
    Document: pass the schema name as a key-val pair
    "schema" = schema_name,
    followed by table name as
    "name" = table_name
    */

  void process(Processor &prc) const
  {
    prc.doc_begin();
    prc.key_val("name")->scalar()->str(m_name);
    if (!m_schema.is_null())
      prc.key_val("schema")->scalar()->str(m_schema.name());
    prc.doc_end();
  }

};


inline
std::ostream& operator<<(std::ostream &out, const Table_ref &tbl)
{
  if (tbl.schema())
    out <<"`" <<tbl.schema()->name() <<"`.";
  out <<"`" <<tbl.name() <<"`";
  return out;
}


}} // cdk::test

#endif

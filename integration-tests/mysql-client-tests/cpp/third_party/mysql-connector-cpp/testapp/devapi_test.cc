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

#include <iostream>
#include <mysqlx/xdevapi.h>


using ::std::cout;
using ::std::endl;
using namespace ::mysqlx;


int main(int argc, const char* argv[])
try {

  const char   *url = (argc > 1 ? argv[1] : "mysqlx://root@127.0.0.1");

  cout << "Creating session on " << url
       << " ..." << endl;

  Session sess(url);

  {
  /*
    TODO: Only working with server version 8
  */

    RowResult res = sess.sql("show variables like 'version'").execute();
    std::stringstream version;

    version << res.fetchOne().get(1).get<string>();
    int major_version;
    version >> major_version;

    if (major_version < 8)
    {
      cout <<"Done!" <<endl;
      return 0;
    }
  }


  cout <<"Session accepted, creating collection..." <<endl;

  Schema sch= sess.getSchema("test");
  Collection coll= sch.createCollection("c1", true);

  cout <<"Inserting documents..." <<endl;

  coll.remove("true").execute();

  {
    Result add;

    add= coll.add(R"({ "name": "foo", "age": 1 })").execute();
    std::vector<string> ids = add.getGeneratedIds();
    cout <<"- added doc with id: " << ids[0] <<endl;

    add= coll.add(R"({ "name": "bar", "age": 2, "toys": [ "car", "ball" ] })")
             .execute();
    if (ids.size() != 0)
      cout <<"- added doc with id: " << ids[0] <<endl;
    else
      cout <<"- added doc" <<endl;

    add= coll.add(R"({
       "name": "baz",
        "age": 3,
       "date": { "day": 20, "month": "Apr" }
    })").execute();
    if (ids.size() != 0)
      cout <<"- added doc with id: " << ids[0] <<endl;
    else
      cout <<"- added doc" <<endl;

    add= coll.add(R"({ "_id": "myuuid-1", "name": "foo", "age": 7 })")
             .execute();
    ids = add.getGeneratedIds();
    if (ids.size() != 0)
      cout <<"- added doc with id: " << ids[0] <<endl;
    else
      cout <<"- added doc" <<endl;
  }

  cout <<"Fetching documents..." <<endl;

  DocResult docs = coll.find("age > 1 and name like 'ba%'").execute();

  DbDoc doc = docs.fetchOne();

  for (int i = 0; doc; ++i, doc = docs.fetchOne())
  {
    cout <<"doc#" <<i <<": " <<doc <<endl;

    for (Field fld : doc)
    {
      cout << " field `" << fld << "`: " <<doc[fld] << endl;
    }

    string name = doc["name"];
    cout << " name: " << name << endl;

    if (doc.hasField("date") && Value::DOCUMENT == doc.fieldType("date"))
    {
      cout << "- date field" << endl;
      DbDoc date = doc["date"];
      for (Field fld : date)
      {
        cout << "  date `" << fld << "`: " << date[fld] << endl;
      }
      string month = doc["date"]["month"];
      int day = date["day"];
      cout << "  month: " << month << endl;
      cout << "  day: " << day << endl;
    }

    if (doc.hasField("toys") && Value::ARRAY == doc.fieldType("toys"))
    {
      cout << "- toys:" << endl;
      for (auto toy : doc["toys"])
      {
        cout << "  " << toy << endl;
      }
    }

    cout << endl;
  }

  cout <<"Done!" <<endl;
}
catch (const mysqlx::Error &err)
{
  cout <<"ERROR: " <<err <<endl;
  return 1;
}
catch (std::exception &ex)
{
  cout <<"STD EXCEPTION: " <<ex.what() <<endl;
  return 1;
}
catch (const char *ex)
{
  cout <<"EXCEPTION: " <<ex <<endl;
  return 1;
}

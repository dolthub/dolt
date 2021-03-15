# MySQL Connector/C++

This is a release of MySQL Connector/C++, [the C++ interface](https://dev.mysql.com/doc/dev/connector-cpp/8.0/) for communicating with MySQL servers.

For detailed information please visit the official [MySQL Connector/C++ documentation](https://dev.mysql.com/doc/dev/connector-cpp/8.0/).

## Licensing

Please refer to files README and LICENSE, available in this repository, and [Legal Notices in documentation](https://dev.mysql.com/doc/connector-cpp/8.0/en/preface.html) for further details. 

## Download & Install

MySQL Connector/C++ can be installed from pre-compiled packages that can be downloaded from the [MySQL downloads page](https://dev.mysql.com/downloads/connector/cpp/).
The process of installing of Connector/C++ from a binary distribution is described in [MySQL online manuals](https://dev.mysql.com/doc/connector-cpp/8.0/en/connector-cpp-installation-binary.html)

### Building from sources

MySQL Connector/C++ can be installed from the source. Please check [MySQL online manuals](https://dev.mysql.com/doc/connector-cpp/8.0/en/connector-cpp-installation-source.html)

### GitHub Repository

This repository contains the MySQL Connector/C++ source code as per latest released version. You should expect to see the same contents here and within the latest released Connector/C++ package.

## Sample Code

```
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

  cout <<"Session accepted, creating collection..." <<endl;

  Schema sch= sess.getSchema("test");
  Collection coll= sch.createCollection("c1", true);

  cout <<"Inserting documents..." <<endl;

  coll.remove("true").execute();

  {
    DbDoc doc(R"({ "name": "foo", "age": 1 })");

    Result add =
      coll.add(doc)
          .add(R"({ "name": "bar", "age": 2, "toys": [ "car", "ball" ] })")
          .add(R"({ "name": "bar", "age": 2, "toys": [ "car", "ball" ] })")
          .add(R"({
                 "name": "baz",
                  "age": 3,
                 "date": { "day": 20, "month": "Apr" }
              })")
          .add(R"({ "_id": "myuuid-1", "name": "foo", "age": 7 })")
          .execute();

    std::list<string> ids = add.getGeneratedIds();
    for (string id : ids)
      cout <<"- added doc with id: " << id <<endl;
  }

  cout <<"Fetching documents..." <<endl;

  DocResult docs = coll.find("age > 1 and name like 'ba%'").execute();

  int i = 0;
  for (DbDoc doc : docs)
  {
    cout <<"doc#" <<i++ <<": " <<doc <<endl;

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

```

## Documentation

* [MySQL](http://www.mysql.com/)
* [Connector/C++ API Reference](https://dev.mysql.com/doc/dev/connector-cpp/8.0/)

## Questions/Bug Reports

* [Discussion Forum](https://forums.mysql.com/list.php?167)
* [Slack](https://mysqlcommunity.slack.com)
* [Bugs](https://bugs.mysql.com)


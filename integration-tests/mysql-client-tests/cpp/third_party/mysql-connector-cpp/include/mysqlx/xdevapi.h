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

#ifndef MYSQL_DEVAPI_H
#define MYSQL_DEVAPI_H

#ifndef __cplusplus
#error This header can be only used with C++ code
#endif

/**
  @defgroup devapi  X DevAPI Classes

  X DevAPI Classes and types. See @ref devapi_ref for introduction.

  @defgroup devapi_op     Database operations
  @ingroup devapi

  Classes representing yet-to-be-executed database operations.

  Such operations are created by various methods of
  @link mysqlx::abi2::r0::Collection `Collection`@endlink or
  @link mysqlx::abi2::r0::Table `Table`@endlink classes. Database operation
  classes define methods that specify additional operation characteristics
  before it gets executed with `execute()` method. The latter
  returns a @link mysqlx::abi2::r0::Result `Result`@endlink,
  @link mysqlx::abi2::r0::DocResult `DocResult`@endlink or
  @link mysqlx::abi2::r0::RowResult `RowResult`@endlink object,
  depending on the type of the operation.

  @defgroup devapi_res    Classes for result processing
  @ingroup devapi

  Classes used to examine results of a statement and documents or
  rows contained in a result.

  @defgroup devapi_aux    Auxiliary types
  @ingroup devapi
*/


/**
  @file
  The main header for MySQL Connector/C++ DevAPI.

  This header should be included by C++ code which uses the DevAPI implemented
  by MySQL Connector/C++.

  @sa result.h, document.h

  @ingroup devapi
*/

/*
  X DevAPI public classes are declared in this and other headers included from
  devapi/ folder. The main public API classes, such as Session below, contain
  declarations of public interface methods. Any obscure details of the public
  API, which must be defined in the public header, are factored out
  to Session_detail class from which the main Session class inherits.
  Among other things, Session_detail declares the implementation class for
  Session. This implementation class is opaque and its details are not defined
  in the public headers - only in the implementation part. Definitions of
  XXX_detail classes can be found in devapi/detail/ sub-folder.
*/

#include "devapi/common.h"
#include "devapi/result.h"
#include "devapi/collection_crud.h"
#include "devapi/table_crud.h"
#include "devapi/settings.h"
#include "devapi/detail/session.h"

namespace mysqlx {
MYSQLX_ABI_BEGIN(2,0)

class Session;

namespace internal {

template <class Base> class Sch_object;

}  // internal


/// Collection create/modify Validation options

class CollectionOptions;

/**
  The CollectionValidation class defines collection schema and level of
  validation.
 */

class CollectionValidation
{
public:

#define COLLECTION_VALIDATION_ENUM(x,y) x=y,


  /**
    Collection validation level options

    \anchor CollectionValidation_Level
   */

  enum Level
  {
    COLLECTION_VALIDATION_LEVEL(COLLECTION_VALIDATION_ENUM)
  };


  /**
    \anchor CollectionValidation_Option
    Collection validation options
   */

  enum Option
  {
    COLLECTION_VALIDATION_OPTION(COLLECTION_VALIDATION_ENUM)
    LAST
  };

private:

  struct Data
  {
    std::string validation_level;
    DbDoc validation_schema;
    std::bitset<LAST> used;
  };

public:

  CollectionValidation()
  {}

  CollectionValidation(const char* json_doc)
    : CollectionValidation(DbDoc(json_doc))
  {}

  /**
     Constructor using a document.

     Document example:
     ~~~~~~
     {
       "level": "Strict",
       "schema":
       {
         "id": "http://json-schema.org/geo",
         "$schema": "http://json-schema.org/draft-06/schema#",
         "description": "A geographical coordinate",
         "type": "object",
         "properties":
         {
           "latitude":
           {
             "type": "number"
           },
           "longitude":
           {
             "type": "number"
           }
         },
         "required": ["latitude", "longitude"]
         }
       }
     }
     ~~~~~~

     Document keys:
       - `level`: See CollectionValidation::LEVEL;
       - `schema`: See CollectionValidation::SCHEMA;

   */

  CollectionValidation(DbDoc doc)
  {
    for(auto el : doc)
    {
      if(el == "level")
      {
        try {
          _set(LEVEL, doc[el].get<std::string>());
        } catch (const Error& e)
        {
          std::string err("Unexpected level type: ");
          err+=e.what();
          throw Error(err.c_str());
        }
      }
      else if(el == "schema")
      {
        _set(SCHEMA,doc[el].get<DbDoc>());
      }
      else {
        std::string err("Unexpected schema validation field ");
        err+=el;
        throw Error(err.c_str());
      }
    }
  }

  /**
    Construct CollectionValidation from list of Option and value pairs.
    See @ref CollectionValidation_Option "CollectionValidation::Option"
    for possible options.
   */
  template<typename... Rest>
  CollectionValidation(Option opt, Rest&&... rest)
  {
    set(opt, std::forward<Rest>(rest)...);
  }

  /**
    Set list of Option and value pairs.
    @see CollectionValidation::CollectionValidation
   */
  template<typename... Rest>
  void set(Rest&&... options)
  {
    Data tmp_data(m_data);
    try {
      _set(std::forward<Rest>(options)...);
    } catch (...) {
      m_data = tmp_data;
      throw;
    }
  }

protected:

  /// @cond DISABLED
  // Note: Doxygen gets confused here and renders docs incorrectly.
  template<typename T,typename... Rest>
  void _set(Option opt, T&& v, Rest&&... options)
  {
#define SCHEMA_VALIDATION_SET(x,y) case CollectionValidation::x:\
    do_set<CollectionValidation::x>(std::forward<T>(v)); break;

    switch (opt)
    {
      COLLECTION_VALIDATION_OPTION(SCHEMA_VALIDATION_SET)
      case CollectionValidation::LAST: throw_error("Invalid option."); ; break;
    }

    _set(std::forward<Rest>(options)...);
  }
  /// @endcond

  void _set() {}

  template<CollectionValidation::Option, typename T>
  void do_set(T)
  {
    throw_error("Invalid option value type.");
  }

  Data m_data;

  friend CollectionOptions;
  friend Schema;
  friend mysqlx::internal::Schema_detail;
};


/**
   The CollectionOptions class defines collection create/modify options.
 */

class CollectionOptions
{
  public:

#define COLLECTION_OPTIONS_ENUM(x,y) x=y,

  /**
    \anchor CollectionOptions_Option
    Collection options
   */

  enum Option
  {
    COLLECTION_OPTIONS_OPTION(COLLECTION_OPTIONS_ENUM)
    LAST
  };

private:

  struct Data{
    CollectionValidation validation;
    std::bitset<LAST> used;
    bool reuse = false;
  };

  public:


  CollectionOptions()
  {}

  CollectionOptions(const char* options)
    : CollectionOptions(DbDoc(options))
  {}

  CollectionOptions(const std::string& options)
    : CollectionOptions(DbDoc(options))
  {}

  /**
     Constructor using a document.

     Document example:
     ~~~~~~
      {
        "reuseExisting": true,
        "validation":
        {
          "level": "Strict",
          "schema":
          {
            "id": "http://json-schema.org/geo",
            "$schema": "http://json-schema.org/draft-06/schema#",
            "description": "A geographical coordinate",
            "type": "object",
            "properties":
            {
              "latitude":
              {
                "type": "number"
              },
              "longitude":
              {
                "type": "number"
              }
            },
            "required": ["latitude", "longitude"]
            }
          }
        }
      }
      ~~~~~~

      Document keys:
      - `reuseExisting` : Same as CollectionOptions::REUSE;
      - `validation` : Same as CollectionOptions::VALIDATION;


   */
  CollectionOptions(DbDoc options)
  {
    for(auto el : options)
    {
      if(el == "reuseExisting")
      {
        try {
          _set(REUSE, options["reuseExisting"].get<bool>());
        } catch (const Error& e)
        {
          std::string err("Wrong value for reuseExisting option: ");
          err+=e.what();
          throw Error(err.c_str());
        }
      }
      else if(el == "validation")
      {
        _set(VALIDATION, CollectionValidation(options["validation"].get<DbDoc>()));
      }
      else {
        std::string err("Unexpected collection option ");
        err+=el;
        throw Error(err.c_str());
      }
    }
  }

  CollectionOptions(CollectionValidation validation)
  {
    set(VALIDATION, validation);
  }

  /**
    Construct CollectionOptions from list of Option and value pairs.
    @ref CollectionOptions_Option "CollectionOptions::Option" and
    @ref CollectionValidation_Option "CollectionValidation::Option" can both be
    used.

    Example:
    ~~~~
    schema.createCollection(
                     "collection_test",
                     CollectionValidation::LEVEL, CollectionValidation::STRICT,
                     CollectionOptions::REUSE, true,
                     CollectionValidation::SCHEMA,
                     R"(
                     {
                     "id": "http://json-schema.org/geo",
                     "$schema": "http://json-schema.org/draft-06/schema#",
                     "description": "A geographical coordinate",
                     "type": "object",
                     "properties":
                     {
                       "latitude": {
                         "type": "number"
                       },
                       "longitude": {
                         "type": "number"
                       }
                     },
                     "required": ["latitude", "longitude"]
                     })"
                     );

    ~~~~

   */

  template<typename... Rest>
  CollectionOptions(Option opt, Rest&&... rest)
  {
   set(opt, std::forward<Rest>(rest)...);
  }


  template<typename... Rest>
  CollectionOptions(CollectionValidation::Option opt, Rest&&... rest)
  {
   set(opt, std::forward<Rest>(rest)...);
  }


  /**
    Set list of option and value pairs.
    @see CollectionOptions::CollectionOptions
   */
  template<typename... Rest>
  void set(Rest&&... rest)
  {
    Data tmp_data(m_data);
    try {
      _set(std::forward<Rest>(rest)...);
    } catch (...) {
      m_data = std::move(tmp_data);
      throw;
    }
  }



protected:

  /// @cond DISABLED
  // Note: Doxygen gets confused here and renders docs incorrectly.
  template<typename T,typename... Rest>
  void _set(Option opt, T&& v, Rest&&... rest)
  {
#define COLLECTION_OPTIONS_SET(x,y) case x:\
    do_set<x>(std::forward<T>(v)); break;

    switch (opt)
    {
      COLLECTION_OPTIONS_OPTION(COLLECTION_OPTIONS_SET)
      case LAST: throw_error("Invalid option."); ; break;
    }

    _set(std::forward<Rest>(rest)...);
  }
  /// @endcond

  template<typename T,typename... Rest>
  void _set(CollectionValidation::Option opt, T&& v, Rest&&... rest)
  {
    m_data.validation._set(opt, std::forward<T>(v));
    _set(std::forward<Rest>(rest)...);
  }

  void _set(){}

  template<CollectionOptions::Option O,typename T>
  void do_set(T)
  {
    throw_error("Invalid option value type.");
  }

  Data m_data;

  friend mysqlx::internal::Schema_detail;
};


/**
  Represents a database schema.

  A `Schema` instance can be obtained from `Session::getSchema()`
  method:

  ~~~~~~
  Session session;
  Schema   mySchema;

  mySchema= session.getSchema("My Schema");
  ~~~~~~

  or it can be directly constructed as follows:

  ~~~~~~
  Session   session;
  Schema     mySchema(session, "My Schema");
  ~~~~~~

  Each `Schema` instance is tied to a particular session and all
  the operations on the schema and its objects are performed using
  that session. If the session is destroyed, an attempt to use a schema of
  that session yields an error.

  When creating a `Schema` object, by default no checks are made that
  it actually exists in the database. An operation that is executed
  on the server and involves such a non-existent schema throws
  an error.

  @ingroup devapi
*/

class Schema
  : protected internal::Schema_detail
{
  Session *m_sess;

public:

  /**
    Construct an object representing the named schema.
  */

  Schema(Session &sess, const string &name);

  /**
    Construct an object representing the default schema of the session.

    The default schema is the one specified by session creation options.
  */

  Schema(Session&);


  /**
     Get schema name
  */

  const string& getName() const
  {
    return m_name;
  }

  /**
    Get Session object
  */

  Session& getSession()
  {
    return *m_sess;
  }

  const Session& getSession() const
  {
    return const_cast<Schema*>(this)->getSession();
  }

  /**
    Check if this schema exists in the database.

    @note Involves communication with the server.
  */

  bool existsInDatabase() const
  {
    try {
      /*
        Note: We get from server a list of schemata filtered by the name of
        this schema - if the schema exists the list should not be empty.
      */
      internal::Session_detail::Name_src list(*m_sess, m_name);
      list.iterator_start();
      return list.iterator_next();
    }
    CATCH_AND_WRAP
  }


  /**
    Create a new collection in the schema.

    Returns the created collection. Set `reuse` flag to true to return
    an already existing collection with the same name. Otherwise, an attempt
    to create a collection which already exists throws an error.
  */


  Collection createCollection(const string &name);
  Collection createCollection(const string &name, bool);

  /**
    Create a new collection in the schema, optionally specifying creation
    options. Arguments following `name`, if any, are used to construct
    CollectionOptions object. See CollectionOptions for possible ways of
    specifying the options.

    Returns the created collection.
  */
  template<typename... Rest>
  Collection createCollection(const string &name,
                              Rest&&... rest);

  /**
    Modify a collection in the schema specifying modify options. Arguments
   following `name` are used to construct CollectionOptions object. See
   CollectionOptions for possible ways of specifying the options.

    @note CollectionOptions::REUSE is not allowed and, if used, will throw
          error.
  */
  template<typename... Rest>
  void modifyCollection(const string &name, Rest&&... options);


  /**
    Return an object representing a collection with the given name.

    To check if the collection actually exists in the database set
    `check_existence` flag to true. Otherwise the returned object can refer to
    a non-existing collection. An attempt to use such a non-existing collection
    in a database operation throws an error.

    @note Checking the existence of a collection involves communication with
    the server. If `check_exists` is false, on the other hand, no I/O is
    involved when creating a `Collection` object.
  */

  Collection getCollection(const string &name, bool check_exists = false);

  /**
    Return an object representing a table or a view with the given name.

    To check if the table actually exists in the database set
    `check_existence` flag to true. Otherwise the returned object can refer to
    a non-existing table. An attempt to use such a non-existing table
    in a database operation throws an error.

    @note The returned `Table` object can represent a plain table or
    a view. See `Table` class description.

    @note Checking the existence of a table involves communication with
    the server. If `check_exists` is false, on the other hand, no I/O is
    involved when creating a `Table` object.
  */

  Table getTable(const string &name, bool check_exists = false);

  /**
    Get a list of all collections in the schema.

    The returned value can be stored in a container that holds `Collection`
    objects, such as `std::vector<Collection>`.
  */

  CollectionList getCollections()
  {
    try {
      return Collection_src(*this, "%");
    }
    CATCH_AND_WRAP
  }

  /**
    Get a list of names of all collections in the schema.

    The returned value can be stored in a container that holds strings, such as
    `std::vector<string>`.
  */

  StringList getCollectionNames()
  {
    try {
      return Name_src(*this, COLLECTION, "%");
    }
    CATCH_AND_WRAP
  }

  /**
    Get a list of all tables and views in the schema.

    The returned value can be stored in a container that holds `Table`
    objects, such as `std::vector<Table>`.

    @note The list also contains views which are represented by `Table` objects
    - see `Table` class description.
  */

  TableList getTables()
  {
    try {
      return Table_src(*this, "%");
    }
    CATCH_AND_WRAP
  }

  /**
    Get a list of names of all tables and views in the schema.

    The returned value can be stored in a container that holds strings, such as
    `std::vector<string>`.

    @note The list also contains names of views as views are represented
    by `Table` objects - see `Table` class description.
  */

  StringList getTableNames()
  {
    try {
      return Name_src(*this, TABLE, "%");
    }
    CATCH_AND_WRAP
  }

  /**
    Return a table corresponding to the given collection.

    The table has two columns: `_id` and `doc`. For each document in
    the collection there is one row in the table with `doc` filed holding
    the document as a JSON value and `_id` field holding document's identifier.

    To check if the collection actually exists in the database set
    `check_existence` flag to true. Otherwise the returned table can refer to
    a non-existing collection. An attempt to use such a non-existing collection
    table throws an error.

    @note Checking the existence of a collection involves communication with
    the server. If `check_exists` is false, on the other hand, no I/O is
    involved when creating the `Table` object.
  */

  Table getCollectionAsTable(const string &name, bool check_exists = true);


  /**
    Drop the given collection from the schema.

    This method silently succeeds if a collection with the given name does
    not exist.

    @note If a table name is passed to the method, it behaves like
    dropTable().
  */

  void dropCollection(const mysqlx::string& name)
  {
    try {
      Schema_detail::drop_collection(name);
    }
    CATCH_AND_WRAP
  }


  friend Collection;
  friend Table;

  ///@cond IGNORE
  friend internal::Schema_detail::Name_src;
  template <class Base> friend class internal::Sch_object;
  ///@endcond
};


/*
  Database objects that belong to some schema
  ===========================================
*/


namespace internal {

// Common base for schema objects defining the common API methods.

template <class Base = Db_obj_base>
class Sch_object
  : public Base
{
protected:

  Schema m_schema;

  Sch_object(const Schema &sch, const string &name);

public:

  using string = mysqlx::string;

  /**
    Get database object name
  */

  const string& getName() const
  {
    return Base::m_name;
  }

  /**
    Get Session object
  */

  Session& getSession()
  {
    return m_schema.getSession();
  }

  /**
    Get schema object
  */

  const Schema& getSchema() const
  {
    return m_schema;
  }

protected:

  std::shared_ptr<common::Session_impl> get_session();

  Schema_detail& get_schema()
  {
    return m_schema;
  }
};

}  // internal







/**
  Represents a collection of documents in a schema.

  A collection object can be obtained from `Schema::getCollection()`
  method:

  ~~~~~~
  Schema db;
  Collection myColl;

  myColl= db.getCollection("My Collection");
  ~~~~~~

  or directly constructed as follows:

  ~~~~~~
  Schema db;
  Collection myColl(db, "My Collection");
  ~~~~~~

  When creating a `Collection` object, by default no checks are made that
  it actually exists in the database. An operation that is executed
  on the server and involves such a non-existent collection throws
  an error. Call `existsInDatabase()` to check the existence of the collection.

  @ingroup devapi
*/

class Collection
  : protected internal::Sch_object<internal::Collection_detail>
{
public:

  Collection(const Schema &sch, const string &name)
  try
    : Sch_object(sch, name)
  {}
  CATCH_AND_WRAP


  using Sch_object::getName;
  using Sch_object::getSession;
  using Sch_object::getSchema;

  bool existsInDatabase() const
  {
    try {
      Schema::StringList list(m_schema, Schema::COLLECTION, m_name);
      return list.begin() != list.end();
    }
    CATCH_AND_WRAP
  }

  /**
    Get the number of documents in the collection.
  */

  uint64_t count();

  /*
    CRUD operations on a collection
    -------------------------------
  */

  /**
    Return an operation which fetches all documents from the collection.

    Call `execute()` on the returned operation object to execute it and get
    a `DocResult` object that gives access to the documents. Specify additional
    query parameters, such as ordering of the documents, using chained methods
    of `CollectionFind` class before making the final call to `execute()`.

    @note Any errors related to the operation are reported when the operation
    is executed, not when it is created.

    @see `CollectionFind`
  */

  CollectionFind find()
  {
    try {
      return CollectionFind(*this);
    }
    CATCH_AND_WRAP;
  }

  /**
    Return an operation which finds documents that satisfy given criteria.

    The criteria are specified as a Boolean expression string.
    Call `execute()` on the returned operation object to execute it and get
    a `DocResult` object that gives access to the documents. Specify additional
    query parameters, such as ordering of the documents, using chained methods
    of `CollectionFind` class before making the final call to `execute()`.

    @note Any errors related to the operation are reported when the operation
    is executed, not when it is created.

    @see `CollectionFind`
  */

  CollectionFind find(const string &cond)
  {
    try {
      return CollectionFind(*this, cond);
    }
    CATCH_AND_WRAP;
  }

  /**
    Return an operation which adds documents to the collection.

    Specify documents to be added in the same way as when calling
    `CollectionAdd::add()` method. Make additional calls to `add()` method on
    the returned operation object to add more documents. Call `execute()`
    to execute the operation and add all specified documents to the collection.

    @note Any errors related to the operation are reported when the operation
    is executed, not when it is created.

    @see `CollectionAdd`
  */

  template <typename... Types>
  CollectionAdd add(Types... args)
  {
    try {

      CollectionAdd add(*this);
      return add.add(args...);
    }
    CATCH_AND_WRAP;
  }

  /**
    Return an operation which removes documents satisfying given criteria.

    The criteria are specified as a Boolean expression string.
    Call `execute()` on the returned operation object to execute it and remove
    the matching documents. Use chained methods of `CollectionRemove` class
    before the final call to `execute()` to further limit the set of documents
    that are removed.

    @note To remove all documents in the collection, pass "true" as selection
    criteria.

    @note Any errors related to the operation are reported when the operation
    is executed, not when it is created.

    @see `CollectionRemove`
  */

  CollectionRemove remove(const string &cond)
  {
    try {
      return CollectionRemove(*this, cond);
    }
    CATCH_AND_WRAP;
  }

  /**
    Return an operation which modifies documents that satisfy given criteria.

    The criteria are specified as a Boolean expression string.
    Specify modifications to be applied to each document using chained methods
    of `CollectionModify` class on the returned operation object. Call
    `execute()` to execute the operation and modify matching documents
    as specified.

    @note To modify all documents in the collection, pass "true" as selection
    criteria.

    @note Any errors related to the operation are reported when the operation
    is executed, not when it is created.

    @see `CollectionModify`
  */

  CollectionModify modify(const string &expr)
  {
    try {
      return CollectionModify(*this, expr);
    }
    CATCH_AND_WRAP;
  }

  /**
    Return the document with the given id.

    Returns null document if a document with the given id does not exist in
    the collection.
  */

  DbDoc getOne(const string &id)
  {
    return find("_id = :id").bind("id", id).execute().fetchOne();
  }

  /**
    Remove the document with the given id.

    Does nothing if a document with the given id does not exist in
    the collection.
   */

  Result removeOne(const string &id)
  {
    return remove("_id = :id").bind("id", id).execute();
  }

  /**
    Replace the document with the given id by a new one.

    Specify the new document as either a `DbDoc` object, a JSON string or
    an `expr(docexpr)` argument, where `docexpr` is like a JSON string but
    field values are given by expressions to be evaluated on the server.

    If a document with the given id does not exist in the collection, nothing
    is done and the returned `Result` object indicates that no documents were
    modified.

    @note If expressions are used, they can not use named parameters because
    it is not possible to bind values prior to execution of `replaceOne()`
    operation.
  */

  Result replaceOne(const string &id, Value &&document)
  {
    try {
      return
        Collection_detail::add_or_replace_one(id, std::move(document), true);
    }
    CATCH_AND_WRAP
  }

  /**
    Add a new document or replace an existing document with the given id.

    Specify the new document as either a `DbDoc` object, a JSON string or
    an `expr(docexpr)` argument, where `docexpr` is like a JSON string but
    field values are given by expressions to be evaluated on the server.

    If a document with the given id does not exist, the new document is added
    to the collection.

    @note If expressions are used, they can not use named parameters because
    it is not possible to bind values prior to execution of `addOrReplaceOne()`
    operation.
  */

  Result addOrReplaceOne(const string &id, Value &&document)
  {
    try {
      return
        Collection_detail::add_or_replace_one(id, std::move(document), false);
    }
    CATCH_AND_WRAP
  }

  /**
    Create index on the collection.

    This function creates a named index in the collection using a JSON index
    specification.

    @param name name for an index to be created
    @param idx_spec index specification as a JSON string

    @see @ref indexing for information on how to define document
    collection indexes.
  */

  void createIndex(const string &name, const string &idx_spec)
  {
    try {
      Collection_detail::index_create(name, idx_spec);
    }
    CATCH_AND_WRAP
  }

  /**
    Drop index on the collection.

    @param name name for an index to be dropped

    @ingroup devapi_ddl
  */

  void dropIndex(const string &name)
  {
    try {
      Collection_detail::index_drop(name);
    }
    CATCH_AND_WRAP
  }


  friend CollectionFind;
  friend CollectionAdd;
  friend CollectionRemove;
  friend CollectionModify;

  ///@cond IGNORE
  friend internal::Crud_factory;
  ///@endcond
};

//------------------------------------------------------------------------------


template<>
inline
void CollectionValidation::do_set<CollectionValidation::SCHEMA>(DbDoc schema)
{
  if(m_data.used.test(CollectionValidation::SCHEMA))
    throw_error("Validation schema already set.");
  m_data.used.set(CollectionValidation::SCHEMA);
  m_data.validation_schema = schema;
}

template<>
inline
void CollectionValidation::do_set<CollectionValidation::SCHEMA>(const char* schema)
{
  do_set<CollectionValidation::SCHEMA>(DbDoc(schema));
}

template<>
inline
void CollectionValidation::do_set<CollectionValidation::SCHEMA>(std::string schema)
{
  do_set<CollectionValidation::SCHEMA>(DbDoc(schema));
}


template<>
inline
void CollectionValidation::do_set<CollectionValidation::LEVEL>(Level level)
{
  if(m_data.used.test(CollectionValidation::LEVEL))
    throw_error("Validation level already set.");
  m_data.used.set(CollectionValidation::LEVEL);

#define SCHEMA_VALIDATION_CASE(x,y) case Level::x: m_data.validation_level = #x; break;

  switch (level) {
  COLLECTION_VALIDATION_LEVEL(SCHEMA_VALIDATION_CASE)
  }
}

template<>
inline
void CollectionValidation::do_set<CollectionValidation::LEVEL>(std::string level)
{
  if(m_data.used.test(CollectionValidation::LEVEL))
    throw_error("Validation level already set.");
  m_data.used.set(CollectionValidation::LEVEL);

  m_data.validation_level = level;

}

template<>
inline
void CollectionOptions::do_set<CollectionOptions::REUSE>(bool reuse)
{
  if(m_data.used[CollectionOptions::REUSE])
    throw_error("Option reuse already set.");
  m_data.used.set(CollectionOptions::REUSE);
  m_data.reuse = reuse;
}

template<>
inline
void CollectionOptions::do_set<CollectionOptions::VALIDATION>(CollectionValidation validation)
{
  if(m_data.used.test(CollectionOptions::VALIDATION) ||
     m_data.validation.m_data.used.test(CollectionValidation::LEVEL) ||
     m_data.validation.m_data.used.test(CollectionValidation::SCHEMA))
    throw_error("Validation already set.");

  m_data.used.set(CollectionOptions::VALIDATION);
  m_data.validation.m_data.used.set(CollectionValidation::LEVEL);
  m_data.validation.m_data.used.set(CollectionValidation::SCHEMA);

  m_data.validation = validation;
}


inline
Collection Schema::createCollection(const string &name)
{
  try {
    Schema_detail::create_collection(
          name,
          CollectionOptions() );

    return Collection(*this, name);
  }
  CATCH_AND_WRAP
}

inline
Collection Schema::createCollection(const string &name, bool reuse)
{
  try {
    Schema_detail::create_collection(
          name,
          CollectionOptions(
            CollectionOptions::REUSE, reuse)
          );

    return Collection(*this, name);
  }
  CATCH_AND_WRAP
}


template<typename... Rest>
inline
Collection Schema::createCollection(const string &name, Rest&&... rest)
{
  try {
    Schema_detail::create_collection(name, CollectionOptions(std::forward<Rest>(rest)...));
    return Collection(*this, name);
  }
  CATCH_AND_WRAP
}


template <typename... Opt>
inline
void Schema::modifyCollection(const string &name, Opt&&... options)
{
  try {
    Schema_detail::modify_collection(name, CollectionOptions(std::forward<Opt>(options)...));
  }
  CATCH_AND_WRAP
}



inline
Collection Schema::getCollection(const string &name, bool check_exists)
{
  Collection coll(*this, name);
  if (check_exists && !coll.existsInDatabase())
    throw_error("Collection does not exist");
  return coll;
}



/*
  Table object
  ============
*/

/**
  Represents a table in a schema.

  A `Table` object can be obtained from `Schema::getTable()`
  method:

  ~~~~~~
  Schema db;
  Table  myTable;

  myTable= db.getTable("My Table");
  ~~~~~~

  or directly constructed as follows:

  ~~~~~~
  Schema db;
  Table myTable(db, "My Table");
  ~~~~~~

  A `Table` object can refer to a plain table or to a view. In the latter case
  method `isView()` called on the object returns true.

  When creating a `Table` object, by default no checks are made that
  it actually exists in the database. An operation that is executed
  on the server and involves such a non-existent table throws
  an error. Call `existsInDatabase()` to check the existence of the table.

  @ingroup devapi
*/

class Table
  : protected internal::Sch_object<>
{
public:

  Table(const Schema &sch, const string &name)
    : Sch_object(sch, name)
  {}

  Table(const Schema &sch, const string &name, bool is_view)
    : Sch_object(sch, name)
  {
    m_type = is_view ? VIEW : TABLE;
  }


  using Sch_object::getName;
  using Sch_object::getSession;
  using Sch_object::getSchema;


  bool existsInDatabase() const;


  /**
    Check if this table object corresponds to a view.

    @note This check might involve communication with the server.
  */

  bool isView();


  /**
    Get the number of rows in the table.
  */

  uint64_t count()
  {
    try {
      RowResult cnt = select("count(*)").execute();
      return cnt.fetchOne()[0].get<uint64_t>();
    }
    CATCH_AND_WRAP
  }


  /*
    CRUD operations
    ---------------
  */

  /**
    Return an operation which inserts rows into the full table without
    restricting the columns.

    Specify rows to be inserted using methods of `TableInsert` class chained
    on the returned operation object. Call `execute()` to execute the operation
    and insert the specified rows.

    Each specified row must have the same number of values as the number
    of columns of the table. If the value count and the table column count do
    not match, server reports error when operation is executed.

    @note Any errors related to the operation are reported when the operation
    is executed, not when it is created.

    @see `TableInsert`
  */

  TableInsert insert()
  {
    try {
      return TableInsert(*this);
    }
    CATCH_AND_WRAP;
  }

  /**
    Return an operation which inserts rows into the table restricting
    the columns.

    Specify column names as method arguments. Values are inserted only into
    the specified columns, other columns are set to null or to column's default
    value (depending on the definition of the table). Specify rows to
    be inserted using methods of `TableInsert` class chained on the returned
    operation object. Call `execute()` to execute the operation and insert
    the specified values.

    Each specified row must have the same number of values as the number
    of columns listed here. If the value count and the column count do
    not match, server reports error when operation is executed.

    @note Any errors related to the operation are reported when the operation
    is executed, not when it is created.

    @see `TableInsert`
  */

  template <class... T>
  TableInsert insert(const T&... t)
  {
    try {
      return TableInsert(*this, t...);
    }
    CATCH_AND_WRAP;
  }

  /**
    Return an operation which selects rows from the table.

    Call `execute()` on the returned operation object to execute it and get
    a `RowResult` object that gives access to the rows. Specify query
    parameters, such as selection criteria and ordering of the rows, using
    chained methods of `TableSelect` class before making the final call to
    `execute()`. To project selected rows before returning them in the result,
    specify a list of expressions as arguments of this method. These expressions
    are evaluated to form a row in the result. An expression can be optionally
    followed by "AS <name>" suffix to give a name to the corresponding column
    in the result. If no expressions are given, rows are returned as is, without
    any projection.

    @note Any errors related to the operation are reported when the operation
    is executed, not when it is created.

    @see `TableSelect`
  */

  template<typename ...PROJ>
  TableSelect select(const PROJ&...proj)
  {
    try {
      return TableSelect(*this, proj...);
    }
    CATCH_AND_WRAP;
  }

  /**
    Return an operation which removes rows from the table.

    Use chained methods of `TableRemove` class on the returned operation object
    to specify the rows to be removed. Call `execute()` to execute the operation
    and remove the rows.

    @note Any errors related to the operation are reported when the operation
    is executed, not when it is created.

    @see `TableRemove`
  */

  TableRemove remove()
  {
    try {
      return TableRemove(*this);
    }
    CATCH_AND_WRAP;
  }

  /**
    Return an operation which updates rows in the table.

    Use chained methods of `TableUpdate` class on the returned operation object
    to specify which rows should be updated and what modifications to apply
    to each of them. Call `execute()` to execute the operation and remove
    the rows.

    @note Any errors related to the operation are reported when the operation
    is executed, not when it is created.

    @see `TableUpdate`
  */

  TableUpdate update()
  {
    try {
      return TableUpdate(*this);
    }
    CATCH_AND_WRAP;
  }

private:

  enum { TABLE, VIEW, UNKNOWN } m_type = UNKNOWN;

  friend TableSelect;
  friend TableInsert;
  friend TableRemove;
  friend TableUpdate;

  ///@cond IGNORE
  friend internal::Crud_factory;
  ///@endcond
};


inline
bool Table::existsInDatabase() const
{
  try {
    /*
      Note: When checking existence, we also determine if this is a view or
      a plain table because this information is fetched from the server when
      querying for a list of tables.
    */
    Schema::TableList list(m_schema, m_name);
    auto it = list.begin();

    if (!(it != list.end()))
      return false;

    const_cast<Table*>(this)->m_type = (*it).isView() ? VIEW : TABLE;
    return true;
  }
  CATCH_AND_WRAP
}


inline
bool Table::isView()
{
  try {
    /*
      If view status was not determined yet, do existence check which
      determines it as a side effect.
    */
    if (UNKNOWN == m_type)
      if (!existsInDatabase())
        throw_error("Table does not exist");
    return VIEW == m_type;
  }
  CATCH_AND_WRAP
}


inline
Table Schema::getTable(const string &name, bool check_exists)
{
  Table tbl(*this, name);
  if (check_exists && !tbl.existsInDatabase())
    throw_error("Table does not exist");
  return tbl;
}


inline
Table Schema::getCollectionAsTable(const string &name, bool check_exists)
{
  if (check_exists && !getCollection(name).existsInDatabase())
    throw_error("Collection does not exist");
  return { *this, name };
}


inline
uint64_t Collection::count()
{
  return m_schema.getCollectionAsTable(m_name).count();
}


using SqlStatement = internal::SQL_statement;


/**
  Represents a session which gives access to data stored in a data store.

  A `Session` object can be created from a connection string, from
  `SessionSettings` or by directly specifying a host name, TCP/IP port and user
  credentials. Once created, a session is ready to be used. Session destructor
  closes the session and cleans up after it.

  If it is not possible to create a valid session for some reason, errors
  are thrown from session constructor.

  Several hosts can be specified by session creation parameters. In that case
  a failed connection to one of the hosts triggers a fail-over attempt
  to connect to a different host in the list. Only if none of the hosts could
  be contacted, session creation fails.

  The fail-over logic tries hosts in the order in which they are specified in
  session settings unless explicit priorities are assigned to the hosts. In that
  case hosts are tried in the decreasing priority order and for hosts with
  the same priority the order in which they are tired is random.

  Once a valid session is created using one of the hosts, the session is bound
  to that host and never re-connected again. If the connection gets broken,
  the session fails without making any other fail-over attempts. The fail-over
  logic is executed only when establishing a new session.

  @ingroup devapi
*/

class Session
  : private internal::Session_detail
{
public:


  /**
    Create a session specified by a `SessionSettings` object.
  */

  Session(SessionSettings settings)
  try
    : Session_detail(settings)
  {}
  CATCH_AND_WRAP


  /**
    Create a session using given session settings.

    This constructor forwards arguments to a `SessionSettings` constructor.
    Thus all forms of specifying session options are also directly available
    in `Session` constructor.

    Examples:
    ~~~~~~

      Session from_uri("mysqlx://user:pwd@host:port/db?ssl-mode=disabled");


      Session from_options("host", port, "user", "pwd", "db");

      Session from_option_list(
        SessionOption::USER, "user",
        SessionOption::PWD,  "pwd",
        SessionOption::HOST, "host",
        SessionOption::PORT, port,
        SessionOption::DB,   "db",
        SessionOption::SSL_MODE, SSLMode::DISABLED
      );
    ~~~~~~

    @see `SessionSettings`
  */

  template<typename...T>
  Session(T...options)
  try
    : Session(SessionSettings(options...))
  {}CATCH_AND_WRAP


  Session(Session &&other)
  try
    : internal::Session_detail(std::move(other))
  {}CATCH_AND_WRAP


  Session(Client &client);

  /**
    Create a new schema.

    Returns the created schema. Set `reuse` flag to true to return an already
    existing schema with the same name. Otherwise, an attempt to create
    a schema which already exists throws an error.
  */

  Schema createSchema(const string &name, bool reuse = false)
  {
    try {
      Session_detail::create_schema(name, reuse);
      return Schema(*this, name);
    }
    CATCH_AND_WRAP
  }

  /**
    Return an object representing a schema with the given name.

    To check if the schema actually exists in the database set `check_existence`
    flag to true. Otherwise the returned object can refer to a non-existing
    schema. An attempt to use such a non-existing schema in a database operation
    throws an error.

    @note Checking the existence of a schema involves communication with
    the server. If `check_exists` is false, on the other hand, no I/O is
    involved when creating a `Schema` object.
  */

  Schema getSchema(const string &name, bool check_exists = false)
  {
    Schema sch(*this, name);
    if (check_exists && !sch.existsInDatabase())
      throw_error("Schema does not exist");
    return sch;
  }

  /**
    Get the default schema specified when the session was created.
  */

  Schema getDefaultSchema()
  {
    return Schema(*this, getDefaultSchemaName());
  }

  /**
    Get the name of the default schema specified when the session was created.
  */

  string getDefaultSchemaName()
  {
    try {
      return Session_detail::get_default_schema_name();
    }
    CATCH_AND_WRAP
  }

  /**
    Get a list of all database schemas.

    The returned value can be stored in a container that holds `Schema` objects,
    such as `std::vector<Schema>`.
  */

  SchemaList getSchemas()
  {
    try {
      return Schema_src(*this, "%");
    }
    CATCH_AND_WRAP
  }

  // TODO: Should we have getSchemaNames() too?

  /**
    Drop the named schema.

    Error is thrown if the schema doesn't exist,
  */

  void dropSchema(const string &name)
  {
    try {
      Session_detail::drop_schema(name);
    }
    CATCH_AND_WRAP
  }


  /**
    Return an operation which executes an arbitrary SQL statement.

    Call `execute()` on the returned operation object to execute the statement
    and get an `SqlResult` object representing its results. If the SQL statement
    contains `?` placeholders, call `bind()` to define their values
    prior to the execution.

    @note Errors related to SQL execution are reported when the statement
    is executed, not when it is created.
  */

  SqlStatement sql(const string &query)
  {
    try {
      return SqlStatement(this, query);
    }
    CATCH_AND_WRAP
  }

  /**
    Start a new transaction.

    Throws error if previously opened transaction is not closed.
  */

  void startTransaction()
  {
    try {
      Session_detail::start_transaction();
    }
    CATCH_AND_WRAP
  }

  /**
    Commit opened transaction, if any.

    Does nothing if no transaction was opened. After committing the
    transaction is closed.
  */

  void commit()
  {
    try {
      Session_detail::commit();
    }
    CATCH_AND_WRAP
  }

  /**
    Roll back opened transaction, if any.

    Does nothing if no transaction was opened. Transaction which was
    rolled back is closed. To start a new transaction a call to
    `startTransaction()` is needed.
  */

  void rollback()
  {
    try {
      Session_detail::rollback();
    }
    CATCH_AND_WRAP
  }

  /**
    Roll back opened transaction to specified savepoint.

    It rolls back to savepoint, but transaction remains active.
    Does nothing if no transaction was opened.

    @throws Error If savepoint doesn't exist or is empty.
  */

  void rollbackTo(const string &savepoint)
  {
    try {
      if (savepoint.empty())
        throw_error("Invalid empty save point name");
      Session_detail::rollback(savepoint);
    }
    CATCH_AND_WRAP
  }


  /**
    Sets a named transaction savepoint with a name as
    identifier.

    To use savepoints a transaction has to be started using startTransaction().

    @returns string with savepoint name.

    @note If the current transaction has a savepoint with the same name,
    the old savepoint is deleted and a new one is set.
  */

  string setSavepoint(const string &savepoint)
  {
    try {
      if (savepoint.empty())
        throw_error("Invalid empty save point name");
      return Session_detail::savepoint_set(savepoint);
    }
    CATCH_AND_WRAP
  }


  /**
    Creats a transaction savepoint with a generated name as
    identifier.

    To use savepoints a transaction has to be started using startTransaction().

    @returns string with generated savepoint name.

    @note If the current transaction has a savepoint with the same name,
    the old savepoint is deleted and a new one is set.
  */

  string setSavepoint()
  {
    try {
      return Session_detail::savepoint_set();
    }
    CATCH_AND_WRAP
  }


  /**
    Releases savepoint previously added by setSavepoint().

    @note Releasing savepoint doesn't affect data.

    @throws Error If savepoint doesn't exist.
  */

  void releaseSavepoint(const string &savepoint)
  {
    try {
      if (savepoint.empty())
        throw_error("Invalid empty save point name");
      Session_detail::savepoint_remove(savepoint);
    }
    CATCH_AND_WRAP
  }


  /**
    Close this session.

    After the session is closed, any call to other session's methods
    throws an error.
  */

  void close()
  {
    try {
      Session_detail::close();
    }
    CATCH_AND_WRAP
  }

protected:

  using internal::Session_detail::m_impl;

public:

  friend Schema;
  friend Collection;
  friend Table;
  friend Result;
  friend RowResult;

  ///@cond IGNORE
  friend internal::Session_detail;
  friend internal::Crud_factory;
  friend internal::Result_detail;
  template <class Base> friend class internal::Sch_object;
  ///@endcond
};


/**
  Create a client using given client settings.

  Client allows the creation of sessions from a session pool.

  This constructor forwards arguments to a ClientSettings constructor.
  Thus all forms of specifying client options are also directly available
  in Client constructor. ClientOptions and SessionOptions can be mixed
  when construction Client objects

  Examples:
  ~~~~~~

    Client from_uri("mysqlx://user:pwd\@host:port/db?ssl-mode=disabled");


    Client from_options("host", port, "user", "pwd", "db");

    Client from_option_list(
      SessionOption::USER, "user",
      SessionOption::PWD,  "pwd",
      SessionOption::HOST, "host",
      SessionOption::PORT, port,
      SessionOption::DB,   "db",
      SessionOption::SSL_MODE, SSLMode::DISABLED
      ClientOption::POOLING, true,
      ClientOption::POOL_MAX_SIZE, 10,
      ClientOption::POOL_QUEUE_TIMEOUT, 1000,
      ClientOption::POOL_MAX_IDLE_TIME, 500,
    );
  ~~~~~~

  @see ClientSettings
*/

class Client : public internal::Client_detail
{
public:

  Client(ClientSettings settings)
  try
    : Client_detail(settings)
  {}
  CATCH_AND_WRAP

  Client(SessionSettings &settings)
  try
    : Client_detail(settings)
  {}
  CATCH_AND_WRAP

  template<typename...T>
  Client(T...options)
    : Client(ClientSettings(options...))
  {}


  Session getSession()
  {
    return *this;
  }

};


/*
  Session
*/

inline
Session::Session(Client &client)
try
  : internal::Session_detail(client.get_session_pool())
{}CATCH_AND_WRAP

/**
  @ingroup devapi
  Function to get Session object.
  @param p same as needed by Session constructor.
 */

template<class ...P>
Session getSession(P...p)
{
  return Session(p...);
}

/**
  @ingroup devapi
  Function to get Client object.
  @param p same as needed by Client constructor.
 */

template<class ...P>
Client getClient(P...p)
{
  return Client(p...);
}


/*
   Schema class implementation
*/

inline
Schema::Schema(Session &sess, const string &name)
  : Schema_detail(sess.m_impl, name)
  , m_sess(&sess)
{}


template <class Base>
inline
internal::Sch_object<Base>::Sch_object(const Schema &sch, const string &name)
  : Base(sch.getSession().m_impl, name)
  , m_schema(sch)
{}


template <class Base>
inline
std::shared_ptr<common::Session_impl>
internal::Sch_object<Base>::get_session()
{
  assert(m_schema.m_sess);
  return m_schema.m_sess->m_impl;
}


MYSQLX_ABI_END(2,0)
}  // mysqlx

#endif

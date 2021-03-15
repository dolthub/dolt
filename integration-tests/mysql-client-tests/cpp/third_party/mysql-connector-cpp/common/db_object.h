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

#ifndef MYSQLX_COMMON_DB_OBJECT_H
#define MYSQLX_COMMON_DB_OBJECT_H

#include "common.h"

#include <mysql/cdk.h>


namespace mysqlx {
namespace impl {

// --------------------------------------------------------------------

namespace common {

  class Object_ref;

  class Schema_ref : public cdk::api::Schema_ref
  {
    cdk::string m_name;

  public:

    Schema_ref(const cdk::string &name) : m_name(name) {}
    Schema_ref() = default;

    const cdk::string name() const { return m_name; }

    friend Object_ref;
  };


  class Object_ref : public cdk::api::Object_ref
  {
    Schema_ref m_schema;
    const cdk::string m_name;

  public:

    const cdk::string name() const { return m_name; }
    const cdk::api::Schema_ref* schema() const { return &m_schema; }

    Object_ref(const cdk::string &schema, const cdk::string &name)
      : m_schema(schema), m_name(name)
    {}

    Object_ref(const cdk::string &name)
      : m_name(name)
    {}

    Object_ref(const cdk::api::Object_ref &other)
      : Object_ref(other.name())
    {
      if (other.schema())
        m_schema.m_name = other.schema()->name();
    }
  };

} // common

}} // mysqlx::impl

#endif

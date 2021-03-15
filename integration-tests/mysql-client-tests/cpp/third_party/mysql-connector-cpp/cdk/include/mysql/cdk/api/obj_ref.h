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

#ifndef CDK_API_OBJ_REF_H
#define CDK_API_OBJ_REF_H

#include "../foundation.h" // for string

namespace cdk {
namespace api {

/*
  Classes for describing database object references of the form:

   [[<catalog>.<schema>.]<table>.]<column>

*/

class Ref_base
{
public:

  virtual ~Ref_base() {}

  virtual const string name() const =0;
  virtual const string orig_name() const { return name(); }
};


class Schema_ref
    : public Ref_base
{
public:

  virtual const Ref_base* catalog() const { return NULL; }
};

class Object_ref
    : public Ref_base
{
public:
  virtual const Schema_ref* schema() const =0;
};


typedef Object_ref Table_ref;


class Column_ref
    : public Ref_base
{
public:
  virtual const Table_ref* table() const =0;
};


}}  // cdk::api


#endif

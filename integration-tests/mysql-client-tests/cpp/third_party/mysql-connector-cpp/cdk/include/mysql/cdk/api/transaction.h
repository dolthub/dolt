/*
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
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

#ifndef CDK_API_TRANSACTION_H
#define CDK_API_TRANSACTION_H

#include "mysql/cdk/foundation.h"


namespace cdk {
namespace api {

template <class Traits>
class Transaction
    : public Diagnostics
{

public:

  typedef typename Traits::transaction_id_t transaction_id_t;
  typedef typename Traits::savepoint_id_t   savepoint_id_t;

  virtual transaction_id_t  commit() = 0;

  /*
    Rollback transaction to the given savepoint. Default Savepoint id
    (savepoint_id_t()) means beginning of the transaction.
  */
  virtual void rollback(savepoint_id_t id) = 0;

  /*
    TODO:
    Returns true if there are any data modification requests collected in
    the transaction.
  */
  //virtual bool has_changes() = 0;

  /*
    Create a savepoint with given id. If a savepoint with the same id was
    created earlier in the same transaction, then it is replaced by the new one.
    It is an error to create savepoint with id 0, which is reserved for
    the beginning of the current transaction.
  */
  virtual void savepoint_set(savepoint_id_t id) = 0;

  /*
    Remove a savepoint with given id.
  */
  virtual void savepoint_remove(savepoint_id_t id) = 0;

};


}} // cdk::api

#endif

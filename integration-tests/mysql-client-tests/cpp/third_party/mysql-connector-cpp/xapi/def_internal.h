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

#ifndef XAPI_DEF_INTERNAL_H
#define XAPI_DEF_INTERNAL_H

#include <mysql/cdk.h>

typedef parser::Expression_parser Expression_parser;
using   cdk::row_count_t;
typedef cdk::api::Sort_direction Sort_direction;

typedef enum mysqlx_op_enum
{
/*
  Table operation codes
*/
  OP_SELECT = 1, OP_INSERT = 2, OP_UPDATE = 3, OP_DELETE = 4,
/*
  Document operation codes
*/
  OP_FIND = 5, OP_ADD = 6, OP_MODIFY = 7, OP_REMOVE = 8,
/*
  Plain SQL operation
*/
  OP_SQL = 9,

/*
  View operation codes
*/
  OP_VIEW_CREATE = 10, OP_VIEW_UPDATE = 11, OP_VIEW_REPLACE = 12,

/*
  Transactions
*/
  OP_TRX_BEGIN, OP_TRX_COMMIT, OP_TRX_ROLLBACK,
  OP_TRX_SAVEPOINT_SET, OP_TRX_SAVEPOINT_RM,

  OP_LIST_SCHEMAS,
  OP_LIST_COLLECTIONS,
  OP_LIST_TABLES,

  OP_SCHEMA_CREATE, OP_COLLECTION_CREATE,
  OP_SCHEMA_DROP, OP_COLLECTION_DROP,

  OP_IDX_CREATE, OP_IDX_DROP

} mysqlx_op_t;


typedef enum mysqlx_modify_op_enum
{
  MODIFY_SET = 1,
  MODIFY_UNSET = 2,
  MODIFY_ARRAY_INSERT = 3,
  MODIFY_ARRAY_APPEND = 4,
  MODIFY_ARRAY_DELETE = 5,
  MODIFY_MERGE_PATCH = 6
} mysqlx_modify_op;

#endif

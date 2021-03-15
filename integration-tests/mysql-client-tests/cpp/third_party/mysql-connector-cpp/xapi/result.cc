/*
 * Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
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

#include <mysqlx/xapi.h>
#include "mysqlx_cc_internal.h"

using namespace mysqlx::common;

/*
  Convert CDK type and encoding information into XAPI type constant.
*/

uint32_t get_type(const Format_info &fi)
{
  switch (fi.m_type)
  {
  case cdk::TYPE_INTEGER:
  {
    auto &format = fi.get<cdk::TYPE_INTEGER>().m_format;

    if (format.length() == 1)
      return MYSQLX_TYPE_BOOL;

    if (format.is_unsigned())
      return MYSQLX_TYPE_UINT;

    return MYSQLX_TYPE_SINT;
  }

  case cdk::TYPE_FLOAT:
  {
    auto &format = fi.get<cdk::TYPE_FLOAT>().m_format;

    if (format.type() == format.FLOAT)
      return MYSQLX_TYPE_FLOAT;
    else if (format.type() == format.DOUBLE)
      return MYSQLX_TYPE_DOUBLE;
    else
      return MYSQLX_TYPE_DECIMAL;
  }

  case cdk::TYPE_DATETIME:
  {
    auto &format = fi.get<cdk::TYPE_DATETIME>().m_format;

    switch(format.type())
    {
      case cdk::Format<cdk::TYPE_DATETIME>::TIME:
        return MYSQLX_TYPE_TIME;
      case cdk::Format<cdk::TYPE_DATETIME>::TIMESTAMP:
        return MYSQLX_TYPE_TIMESTAMP;
      default:
        return MYSQLX_TYPE_DATETIME;
    }
    break;
  }

  // TODO: differ the blob types by their length
  case cdk::TYPE_BYTES:
  {
    // TODO: use it when GEOMETRY type is supported by CDK
    // cdk::Format<cdk::TYPE_BYTES> format(m_cursor->format(pos));
    return MYSQLX_TYPE_BYTES;
  }

  case cdk::TYPE_STRING:
  {
    auto &format = fi.get<cdk::TYPE_STRING>().m_format;
    if (format.is_enum())
      return MYSQLX_TYPE_ENUM;
    else if (format.is_set())
      return MYSQLX_TYPE_SET;

    return MYSQLX_TYPE_STRING;
  }

  case cdk::TYPE_DOCUMENT:
    return MYSQLX_TYPE_JSON;

  case cdk::TYPE_GEOMETRY:
    return MYSQLX_TYPE_GEOMETRY;

  default:
    return fi.m_type;
  }
}


/*
  Read the next JSON string from the result and advance the cursor position
*/

const char * mysqlx_result_struct::read_json(size_t *json_byte_size)
{
  assert(!m_result_mdata.empty());
  assert(1 == m_result_mdata.front()->col_count());
  assert(cdk::TYPE_DOCUMENT == m_result_mdata.front()->get_type(0));

  mysqlx_row_struct *row = read_row();

  if (!row)
    return NULL;

  cdk::bytes data = row->get_bytes(0);

  /*
    Note: we return size in bytes, including the '\0' terminator.
  */

  if (json_byte_size)
    *json_byte_size = data.size();

  if (0 == data.size())
  {
    // remove unnecessary row structure for null document.
    m_row_set.pop_back();
    return NULL;
  }

  return (const char*) data.begin();
}


const char * mysqlx_result_struct::get_next_generated_id()
{
  if (m_doc_id_list.empty() && m_current_id_index == 0)
  {
    if (!m_reply)
      return NULL;
    for (auto id : m_reply->generated_ids())
      m_doc_id_list.push_back(id);
  }

  if (m_current_id_index >= m_doc_id_list.size())
    return NULL;

  return m_doc_id_list[m_current_id_index++].c_str();
}


mysqlx_error_struct* mysqlx_result_struct::get_next_warning()
{
  if (!m_warn_it.next())
    return NULL;

  m_current_warning.reset(new mysqlx_error_struct(m_warn_it.entry().get_error(), true));

  return m_current_warning.get();
}

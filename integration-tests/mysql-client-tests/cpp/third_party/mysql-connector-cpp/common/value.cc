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

#include <mysql/cdk.h>

#include "value.h"

#include <string>

/*
  Implementation of result and row objects and conversion of raw bytes
  into values.
*/

using namespace ::mysqlx::impl::common;


void Value::print(std::ostream &out) const
{
  switch (m_type)
  {
  case VNULL: out << "<null>"; return;
  case UINT64: out << m_val.v_uint; return;
  case INT64: out << m_val.v_sint; return;
  case DOUBLE: out << m_val.v_double; return;
  case FLOAT: out << m_val.v_float; return;
  case BOOL: out << (m_val.v_bool ? "true" : "false"); return;
  case STRING: out << m_str; return;
  case USTRING: out << cdk::string(m_ustr); return;
  case RAW: out << "<" << m_str.length() << " raw bytes>"; return;
  default:  out << "<unknown value>"; return;
  }
}


// Trivial Format_info for raw byte values

class Raw_format_info
  : public cdk::Format_info
{
  bool for_type(cdk::Type_info) const override { return true; }
  void get_info(cdk::Format<cdk::TYPE_BYTES>&) const override {}
  using cdk::Format_info::get_info;
};


/*
  Describe the given Value to a CDK value processor.
*/

void Value::Access::process_val(
  const Value &val,
  cdk::Value_processor &prc
)
{
  switch (val.get_type())
  {
    case Value::VNULL:    prc.null(); break;
    case Value::INT64:    prc.num(val.get_sint()); break;
    case Value::UINT64:    prc.num(val.get_uint()); break;
    case Value::FLOAT:   prc.num(val.get_float()); break;
    case Value::DOUBLE:  prc.num(val.get_double()); break;
    case Value::BOOL:    prc.yesno(val.get_bool()); break;
    case Value::STRING:  prc.str(val.m_str); break;
    case Value::USTRING:  prc.str(val.m_ustr); break;
    case Value::RAW:
    {
      size_t size;
      const byte*  ptr = val.get_bytes(&size);
      prc.value(cdk::TYPE_BYTES, Raw_format_info(), bytes((byte*)ptr, size));
      break;
    }
    default:
      // note: only above types are valid for value processor
      assert(false);
    break;
  }
}

/*
  Describe given value to a CDK expression processor. For values of type EXPR,
  the expression string is parsed and the parsed expression is described.

  Parameter pm determine DOCUMENT or TABLE mode in which expression string
  is parsed.
*/

void Value::Access::process(
  parser::Parser_mode::value pm,
  const Value &val,
  cdk::Expression::Processor &prc
)
{
  if (Value::EXPR == val.get_type())
  {
    parser::Expression_parser parser{ pm, val.get_string() };
    parser.process(prc);
    return;
  }

  cdk::Value_processor *vprc = prc.scalar()->val();
  if (vprc)
      process_val(val, *vprc);
}


const std::string& Value::get_string() const
{
  switch (m_type)
  {
  case USTRING:

    // UTF8 conversion

    if (!m_val.v_bool)
    {
      const_cast<Value*>(this)->m_str = cdk::string(m_ustr);
      const_cast<Value*>(this)->m_val.v_bool = true;
    }

    FALLTHROUGH;

  case RAW:
  case STRING:
  case EXPR:
  case JSON:
    return m_str;

  default:
    throw Error("Value cannot be converted to string");
  }
}


const std::u16string& Value::get_ustring() const
{
  switch (m_type)
  {
  case EXPR:
  case JSON:
  case RAW:
  case STRING:

    // UTF8 conversion

    if (!m_val.v_bool)
    {
      const_cast<Value*>(this)->m_ustr = cdk::string(m_str);
      const_cast<Value*>(this)->m_val.v_bool = true;
    }

    FALLTHROUGH;

  case USTRING:
    return m_ustr;

  default:
    throw Error("Value cannot be converted to string");
  }
}

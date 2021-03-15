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

#ifndef MYSQLX_COMMON_INT_VALUE_H
#define MYSQLX_COMMON_INT_VALUE_H


#include <mysqlx/common.h>
#include <mysql/cdk.h>
#include <expr_parser.h>


namespace mysqlx {

namespace impl {
namespace common {

template <cdk::Type_info T> class Format_descr;

}}  // impl::common


MYSQLX_ABI_BEGIN(2,0)
namespace common {

struct Value::Access
{

  static Value mk_str(const cdk::string &str)
  {
    return (std::u16string)str;
  }

  static Value mk_expr(const string &expr)
  {
    return { Value::EXPR, expr };
  }

  static Value mk_json(const string &json)
  {
    return { Value::JSON, json };
  }

  // Create value from raw bytes, given CDK format description.

  template<cdk::Type_info T>
  static Value mk(cdk::bytes data, impl::common::Format_descr<T> &format);

  // Describe value to a CDK expression or value processor.

  static void
  process(parser::Parser_mode::value, const Value&, cdk::Expression::Processor&);

  static void
  process_val(const Value&, cdk::Value_processor&);
};

}
MYSQLX_ABI_END(2,0)


namespace impl {
namespace common {

using cdk::string;
using cdk::byte;
using cdk::bytes;
using mysqlx::common::Value;


/*
  Wrappres which present a given Value instance as a CDK scalar or expression.
*/


class Value_scalar
  : public cdk::Any
{
  const Value &m_val;

public:

  Value_scalar(const Value &val)
    : m_val(val)
  {}

  void process(Processor &prc) const
  {
    auto *sprc = prc.scalar();
    if (!sprc)
      return;
    Value::Access::process_val(m_val, *sprc);
  }
};


class Value_expr
  : public cdk::Expression
{
  const Value &m_val;
  parser::Parser_mode::value m_pm;

public:

  Value_expr(const Value &val, parser::Parser_mode::value pm)
    : m_val(val), m_pm(pm)
  {}

  void process(Processor &prc) const
  {
    Value::Access::process(m_pm, m_val, prc);
  }
};


}  // common
}  // impl
}  // mysqlx

#endif

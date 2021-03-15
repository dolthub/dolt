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

#include <mysql/cdk/foundation/diagnostics.h>

PUSH_SYS_WARNINGS_CDK
#include <algorithm>
POP_SYS_WARNINGS_CDK

using namespace cdk;

typedef foundation::Diagnostic_arena::Entry Entry;
typedef std::vector<Entry*> Entry_list;


namespace cdk {
namespace foundation {

using api::Severity;

/*
  Diagnostic_arena implementation.
*/


void delete_entry(Entry *e)
{
  delete e;
}

void Diagnostic_arena::clear()
{
  std::for_each(m_entries.begin(), m_entries.end(), delete_entry);
  m_entries.clear();
  m_counts.clear();
}


/*
  Diagnostic_iterator implementation.
*/


/*
  Helper class to be used as predicate for std algorithms.
*/

class check_level
{
  const Severity::value m_level;

public:

  check_level(Severity::value l) : m_level(l)
  {}

  bool operator() (Entry *e)
  {
    return e->severity() >= m_level;
  }
};


void Diagnostic_iterator::reset(Severity::value level)
{
  m_at_begin= true;
  m_level= level;
  if (NULL == m_entries)
    return;
}


bool Diagnostic_iterator::next()
{
  if (NULL == m_entries)
    return false;

  if (m_at_begin)
  {
    m_it = m_entries->begin();
    m_at_begin = false;
  }
  else
    ++m_it;

  // After moving to next entry, find first one that matches iterator's level
  m_it= std::find_if(m_it, m_entries->end(), check_level(m_level));

  if (m_it == m_entries->end())
    return false;

  return true;
}


const Error& Diagnostic_arena::get_error()
{
  Entry_list::const_iterator it;
  it= std::find_if(m_entries.begin(), m_entries.end(),
                   check_level(api::Severity::ERROR));

  if (it == m_entries.end())
    throw_error("No error entry in diagnostic arena");

  return (*it)->get_error();
}

}}  // cdk::foundation

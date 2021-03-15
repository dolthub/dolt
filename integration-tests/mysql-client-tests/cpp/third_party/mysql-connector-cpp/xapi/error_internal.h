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

#ifndef XAPI_INTERNAL_ERROR
#define XAPI_INTERNAL_ERROR


class Mysqlx_exception
{
public:

  enum Mysqlx_exception_type {
    MYSQLX_EXCEPTION_INTERNAL, MYSQLX_EXCEPTION_EXTERNAL
  };

  Mysqlx_exception(Mysqlx_exception_type t, uint32_t code, std::string message)
    : m_type(t), m_code(code), m_message(message)
  {}

  Mysqlx_exception(std::string message)
    : m_message(message)
  {}

  std::string message() const { return m_message; }
  Mysqlx_exception_type type() const { return m_type; }
  uint32_t code() const { return m_code; }

protected:

  Mysqlx_exception()
  {}

  Mysqlx_exception_type m_type = MYSQLX_EXCEPTION_INTERNAL;
  uint32_t m_code = 0;
  std::string m_message;
};


typedef struct mysqlx_error_struct mysqlx_error_t;

struct Mysqlx_diag_base
{
  virtual ~Mysqlx_diag_base() {}
  virtual mysqlx_error_t * get_error() = 0;
};

typedef struct mysqlx_error_struct : public Mysqlx_diag_base
{
  std::string m_message;
  unsigned int m_error_num;
  bool m_is_warning;

  mysqlx_error_struct() : m_error_num(0), m_is_warning(false)
  {}

  mysqlx_error_struct(const char *m, unsigned int n,
                      bool is_warning = false) :
                      m_is_warning(is_warning)
  {
    set(m, n);
  }

  mysqlx_error_struct(const cdk::Error* cdk_error,
                      bool is_warning = false) :
                      m_is_warning(is_warning)
  {
    set(cdk_error);
  }

  mysqlx_error_struct(const cdk::Error &cdk_error,
                      bool is_warning = false) :
                      m_is_warning(is_warning)
  {
    set(&cdk_error);
  }

  void set(const Mysqlx_exception &ex)
  {
    m_message = ex.message();
    m_error_num = ex.code();
  }

  void set(const char *m, unsigned int n)
  {
    m_message = std::string(m);
    m_error_num = n;
  }

  void set(const cdk::Error* cdk_error)
  {
    if (!cdk_error)
    {
      m_message = "";
      m_error_num = 0;
      return;
    }

    m_message = cdk_error->description();

    if (!m_is_warning || cdk_error->code().category() == cdk::server_error_category())
      m_error_num = (unsigned int)cdk_error->code().value();
    else
      m_error_num = 0;
  }

  void reset()
  {
    set((const cdk::Error*)NULL);
  }

  unsigned int error_num()
  {
    return m_error_num;
  }

  const char* message()
  {
    return m_message.size() ? m_message.data() : NULL;
  }

  mysqlx_error_t * get_error()
  {
    if (message() || error_num())
      return this;
    return NULL;
  }

} mysqlx_error_t;


struct mysqlx_dyn_error_struct : public mysqlx_error_struct
{
  using mysqlx_error_struct::mysqlx_error_struct;
};

class Mysqlx_diag : public Mysqlx_diag_base
{
  protected:
  mysqlx_error_struct m_error;

  public:
  virtual void set_diagnostic(const Mysqlx_exception &ex)
  { m_error.set(ex); }

  virtual void set_diagnostic(const char *msg, unsigned int num)
  { m_error.set(msg, num); }

  virtual void set_diagnostic(mysqlx_error_struct &&error)
  {
    m_error = std::move(error);
  }

  void clear()
  {
    m_error.reset();
  }

  mysqlx_error_t * get_error()
  {
    if (m_error.message() || m_error.error_num())
      return &m_error;
    return NULL;
  }

};

#endif

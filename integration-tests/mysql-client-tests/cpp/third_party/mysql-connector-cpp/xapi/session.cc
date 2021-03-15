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

#include <mysqlx/common.h>
#include <mysqlx/xapi.h>
#include "mysqlx_cc_internal.h"
#include <algorithm>
#include <string>

using namespace mysqlx::common;

mysqlx_session_struct::mysqlx_session_struct( mysqlx_client_t *cli)
{
  if (cli)
    m_impl = std::make_shared<Session_impl>(cli->get_impl());
  else
    throw_error("Invalid client pool");
}


mysqlx_session_struct::mysqlx_session_struct(
  mysqlx_session_options_struct *opt
)
{
  cdk::ds::Multi_source ds;
  opt->get_data_source(ds);
  m_impl = std::make_shared<Session_impl>(ds);
}


mysqlx_session_struct::mysqlx_session_struct(
  const std::string &host, unsigned short port,
  const std::string &usr, const std::string *pwd,
  const std::string *db
)
  : mysqlx_session_struct(mysqlx_session_options_struct(host, port, usr, pwd, db))
{}

mysqlx_session_struct::mysqlx_session_struct(
  const std::string &conn_str
)
  : mysqlx_session_struct(mysqlx_session_options_struct(conn_str))
{}


mysqlx_stmt_struct*
mysqlx_session_struct::sql_query(const char *query_utf8, uint32_t length)
{
  if (!query_utf8 || !(*query_utf8))
    throw Mysqlx_exception("Query is empty");

  if (length == MYSQLX_NULL_TERMINATED)
    length = (uint32_t)strlen(query_utf8);

  std::string query(query_utf8, length);
  return new_stmt<OP_SQL>(cdk::string(query));  // note: UTF8 conversion
}


mysqlx_error_struct * mysqlx_session_struct::get_last_error()
{
  cdk::Session &sess = get_session();

  // Return session errors from CDK first
  if (sess.entry_count())
  {
      m_error.set(&sess.get_error());
  }
  else if (!m_error.message() && !m_error.error_num())
    return NULL;

  return &m_error;
}


const cdk::Error * mysqlx_session_struct::get_cdk_error()
{
  if (get_session().entry_count())
    return &get_session().get_error();

  return NULL;
}


void mysqlx_session_struct::reset_diagnostic()
{
  m_error.reset();
}


void mysqlx_session_struct::transaction_begin()
{
  // Note: the internal implementation object handles registered results etc.
  stmt_traits<OP_TRX_BEGIN>::Impl stmt(m_impl);
  stmt.execute();
}

void mysqlx_session_struct::transaction_commit()
{
  stmt_traits<OP_TRX_COMMIT>::Impl stmt(m_impl);
  stmt.execute();
}

void mysqlx_session_struct::transaction_rollback(const char *sp)
{
  stmt_traits<OP_TRX_ROLLBACK>::Impl stmt(
    m_impl,
    sp ? std::string(sp) : std::string()
  );
  stmt.execute();
}

const char * mysqlx_session_struct::savepoint_set(const char *sp)
{
  stmt_traits<OP_TRX_SAVEPOINT_SET>::Impl stmt(
    m_impl,
    sp ? std::string(sp) : std::string()
  );

  stmt.execute();
  m_savepoint_name = stmt.get_name();
  return m_savepoint_name.c_str();
}

void mysqlx_session_struct::savepoint_remove(const char *sp)
{
  if (!sp || !sp[0])
    throw_error("Invalid empty save point name");
  stmt_traits<OP_TRX_SAVEPOINT_RM>::Impl stmt(m_impl, std::string(sp));
  stmt.execute();
}


/*
   ============================================================================
   Client object implementation
*/

mysqlx_client_struct::mysqlx_client_struct(const char *conn_str,
                                           const char *client_opt)
{
  mysqlx_session_options_struct opt(conn_str);
  if (client_opt)
    opt.set_client_opts(client_opt);
  cdk::ds::Multi_source ds;
  opt.get_data_source(ds);
  m_impl.reset(new  Session_pool(ds));
  m_impl->set_pool_opts(opt);
}


mysqlx_client_struct::mysqlx_client_struct(mysqlx_session_options_t *opt)
{
  cdk::ds::Multi_source ds;
  opt->get_data_source(ds);
  m_impl.reset(new  Session_pool(ds));
  m_impl->set_pool_opts(*opt);
}


using cdk::foundation::connection::TLS;


TLS::Options::SSL_MODE uint_to_ssl_mode(unsigned int mode)
{
  switch (mode)
  {
    case SSL_MODE_DISABLED:
      return TLS::Options::SSL_MODE::DISABLED;
    case SSL_MODE_REQUIRED:
      return TLS::Options::SSL_MODE::REQUIRED;
    case SSL_MODE_VERIFY_CA:
      return TLS::Options::SSL_MODE::VERIFY_CA;
    case SSL_MODE_VERIFY_IDENTITY:
      return TLS::Options::SSL_MODE::VERIFY_IDENTITY;
    default:
      assert(false);
      // Quiet compile warnings
      return TLS::Options::SSL_MODE::DISABLED;
  }
}

unsigned int ssl_mode_to_uint(TLS::Options::SSL_MODE mode)
{
  switch (mode)
  {
    case TLS::Options::SSL_MODE::DISABLED:
      return SSL_MODE_DISABLED;
    case TLS::Options::SSL_MODE::REQUIRED:
      return SSL_MODE_REQUIRED;
    case TLS::Options::SSL_MODE::VERIFY_CA:
      return SSL_MODE_VERIFY_CA;
    case TLS::Options::SSL_MODE::VERIFY_IDENTITY:
      return SSL_MODE_VERIFY_IDENTITY;
    default:
      assert(false);
      // Quiet compile warnings
      return 0;
  }
}


const char* opt_name(mysqlx_opt_type_t opt)
{
  using Option = Settings_impl::Session_option_impl;
  return Settings_impl::option_name(Option(opt));
}

const char* ssl_mode_name(mysqlx_ssl_mode_t m)
{
  using SSL_mode = Settings_impl::SSL_mode;
  return Settings_impl::ssl_mode_name(SSL_mode(m));
}


struct Error_bad_option : public Mysqlx_exception
{
  Error_bad_option()
    : Mysqlx_exception("Unrecognized connection option")
  {}

  Error_bad_option(const std::string &opt) : Error_bad_option()
  {
    m_message += ": " + opt;
  }

  Error_bad_option(unsigned int opt) : Error_bad_option()
  {
    std::ostringstream buf;
    buf << opt;
    m_message += " (" + buf.str() + ")";
  }
};

struct Error_dup_option : public Mysqlx_exception
{
  Error_dup_option(mysqlx_opt_type_t opt)
  {
    m_message = "Option ";
    m_message += opt_name(opt);
    m_message += " defined twice";
  }
};

struct Error_bad_mode : public Mysqlx_exception
{
  Error_bad_mode(const std::string &m)
  {
    m_message = "Unrecognized ssl-mode: " + m;
  }
};

struct Error_ca_mode : public Mysqlx_exception
{
  Error_ca_mode()
    : Mysqlx_exception("The ssl-ca option is not compatible with ssl-mode")
  {}

  Error_ca_mode(mysqlx_ssl_mode_t m) : Error_ca_mode()
  {
    m_message += " ";
    m_message += ssl_mode_name(m);
  }

  Error_ca_mode(TLS::Options::SSL_MODE m)
    : Error_ca_mode(mysqlx_ssl_mode_t(ssl_mode_to_uint(m)))
  {}
};

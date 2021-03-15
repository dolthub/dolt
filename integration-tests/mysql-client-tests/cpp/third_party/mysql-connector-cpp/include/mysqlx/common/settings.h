/*
 * Copyright (c) 2017, 2019, Oracle and/or its affiliates. All rights reserved.
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

#ifndef MYSQLX_COMMON_SETTINGS_H
#define MYSQLX_COMMON_SETTINGS_H

/*
  Classes and code handling session settings. They are used to process session
  creation options, check their consistency and present the settings in the
  form expected by CDK.

  Known session options and their values are defined
  in mysql_common_constants.h header as SESSION_OPTION_LIST() and accompanying
  macros.
*/

#include "../common_constants.h"
#include "value.h"

PUSH_SYS_WARNINGS
#include <vector>
#include <map>
#include <bitset>
#include <sstream>
POP_SYS_WARNINGS


namespace cdk {
namespace ds {

class Multi_source;
struct Attr_processor;

}}


namespace mysqlx {

MYSQLX_ABI_BEGIN(2,0)

namespace common {


/*
  Class for storing session configuration settings.
*/

class PUBLIC_API Settings_impl
{
public:

  /*
    Enumerations with available session options and their values.
  */

#define SETTINGS_OPT_ENUM_str(X,N)  X = N,
#define SETTINGS_OPT_ENUM_num(X,N)  X = N,
#define SETTINGS_OPT_ENUM_any(X,N)  X = N,
#define SETTINGS_OPT_ENUM_bool(X,N) X = N,

  enum Session_option_impl{
    SESSION_OPTION_LIST(SETTINGS_OPT_ENUM)
    LAST
  };



  /*
    Enumerations with available client options and their values.
  */

#define CLIENT_OPT_IMPL_ENUM_str(X,N)  X = -N,
#define CLIENT_OPT_IMPL_ENUM_bool(X,N) X = -N,
#define CLIENT_OPT_IMPL_ENUM_num(X,N)  X = -N,
#define CLIENT_OPT_IMPL_ENUM_any(X,N)  X = -N,

  enum Client_option_impl {
    CLIENT_OPTION_LIST(CLIENT_OPT_IMPL_ENUM)
  };



  static  const char* option_name(int opt);


#define SETTINGS_VAL_ENUM(X,N)  X = N,

  enum class SSL_mode {
    SSL_MODE_LIST(SETTINGS_VAL_ENUM)
    LAST
  };

  static  const char* ssl_mode_name(SSL_mode mode);


  enum class Auth_method {
    AUTH_METHOD_LIST(SETTINGS_VAL_ENUM)
    LAST
  };

  static  const char* auth_method_name(Auth_method method);

  enum class Compression_mode {
    COMPRESSION_MODE_LIST(SETTINGS_VAL_ENUM)
    LAST
  };

  static const char* compression_mode_name(Compression_mode mode);

protected:

  using Value = common::Value;
  using opt_val_t = std::pair<int, Value>;
  // TODO: use multimap instead?
  using option_list_t = std::vector<opt_val_t>;
  using connection_attr = std::map<std::string,std::string>;
  using iterator = option_list_t::const_iterator;

public:

  /*
    Examine settings stored in this object.
  */

  bool has_option(int) const;
  const Value& get(int) const;


  // Iterating over options stored in this object.

  iterator begin() const
  {
    return m_data.m_options.cbegin();
  }

  iterator end() const
  {
    return m_data.m_options.cend();
  }

  /*
    Clear individual or all settings.
  */

  void clear();
  void erase(int);

  /*
    Session options include connection options that specify data source
    (one or many) for which given session is created. This method initializes
    CDK Multi_source object to describe the data source(s) based on the
    connection options.
  */

  void get_data_source(cdk::ds::Multi_source&);

  void get_attributes(cdk::ds::Attr_processor&);

  // Set options based on URI

  void set_from_uri(const std::string &);

  // Set Client options based on JSON object

  void set_client_opts(const std::string &);

  // Set Client options from othe Settings object

  void set_client_opts(const Settings_impl &);

  /*
    Public API has no methods to directly set individual options. Instead,
    to change session settings implementation should create a Setter object
    and use its methods to do the changes. A Settings_impl::Setter object
    provides "transactional" semantics for changing session options -- only
    consistent option changes modify the original Settings_impl object.

    Note: This Setter class is defined in "implementation" header
    common/settings.h. The public API leaves it undefined.
  */

  class Setter;

protected:

  struct PUBLIC_API Data
  {
    Data()
    {
      init_connection_attr();
    }
    DLL_WARNINGS_PUSH
    option_list_t           m_options;
    connection_attr         m_connection_attr;
    DLL_WARNINGS_POP
    unsigned m_host_cnt = 0;
    bool m_user_priorities = false;
    bool m_ssl_ca = false;
    SSL_mode m_ssl_mode = SSL_mode::LAST;
    bool m_tcpip = false; // set to true if TCPIP connection was specified
    bool m_sock = false;  // set to true if socket connection was specified
    bool m_tls_vers = false;
    bool m_tls_ciphers = false;

    void erase(int);
    void init_connection_attr();
    void clear_connection_attr();

  };

  Data m_data;

};


#define SETTINGS_OPT_NAME_str(X,N)  case N: return #X;
#define SETTINGS_OPT_NAME_bool(X,N)  case N: return #X;
#define SETTINGS_OPT_NAME_num(X,N)  case N: return #X;
#define SETTINGS_OPT_NAME_any(X,N)  case N: return #X;
#define SETTINGS_OPT_NAME_bool(X,N)  case N: return #X;


#define CLIENT_OPT_NAME_str(X,N)  case -N: return #X;
#define CLIENT_OPT_NAME_bool(X,N)  case -N: return #X;
#define CLIENT_OPT_NAME_num(X,N)  case -N: return #X;
#define CLIENT_OPT_NAME_any(X,N)  case -N: return #X;


inline
const char* Settings_impl::option_name(int opt)
{
  switch (opt)
  {
    SESSION_OPTION_LIST(SETTINGS_OPT_NAME)
    CLIENT_OPTION_LIST(CLIENT_OPT_NAME)
    default:
      return nullptr;
  }
}


#define SETTINGS_VAL_NAME(X,N) case N: return #X;

inline
const char* Settings_impl::ssl_mode_name(SSL_mode mode)
{
  switch (unsigned(mode))
  {
    SSL_MODE_LIST(SETTINGS_VAL_NAME)
    default:
      return nullptr;
  }
}

inline
const char* Settings_impl::auth_method_name(Auth_method method)
{
  switch (unsigned(method))
  {
    AUTH_METHOD_LIST(SETTINGS_VAL_NAME)
    default:
      return nullptr;
  }
}

inline
const char* Settings_impl::compression_mode_name(Compression_mode mode)
{
  switch (unsigned(mode))
  {
    COMPRESSION_MODE_LIST(SETTINGS_VAL_NAME)
    default:
      return nullptr;
  }
}

/*
  Note: For options that can repeat, returns the last value.
*/

inline
const common::Value& Settings_impl::get(int opt) const
{
  using std::find_if;

  auto it = find_if(m_data.m_options.crbegin(), m_data.m_options.crend(),
    [opt](opt_val_t el) -> bool { return el.first == opt; }
  );

  static Value null_value;

  if (it == m_data.m_options.crend())
    return null_value;

  return it->second;
}


inline
bool Settings_impl::has_option(int opt) const
{
  // For options whose value is a list, we return true if we know the option
  // was set even if no actual values are stored in m_options, which is the
  // case when option value is an empty list.

  switch (opt)
  {
  case Session_option_impl::TLS_VERSIONS:
    if (m_data.m_tls_vers)
      return true;
    break;

  case Session_option_impl::TLS_CIPHERSUITES:
    if (m_data.m_tls_ciphers)
      return true;
    break;

  default:
    break;
  }

  return m_data.m_options.cend() !=
  find_if(m_data.m_options.cbegin(), m_data.m_options.cend(),
    [opt](opt_val_t el) -> bool { return el.first == opt; }
  );

}


inline
void Settings_impl::erase(int opt)
{
  m_data.erase(opt);
}


/*
  Note: Removes all occurrences of the given option. Also updates the context
  used for checking option consistency.
*/

inline
void Settings_impl::Data::erase(int opt)
{
  remove_from(m_options,
    [opt](opt_val_t el) -> bool
  {
    return el.first == opt;
  }
  );

  /*
    TODO: removing HOST from multi-host settings can leave "orphaned"
    PORT/PRIORITY settings. Do we correctly detect that?
  */

  switch (opt)
  {
  case Session_option_impl::HOST:
    m_host_cnt = 0;
    FALLTHROUGH;
  case Session_option_impl::PORT:
    if (0 == m_host_cnt)
      m_tcpip = false;
    break;
  case Session_option_impl::SOCKET:
    m_sock = false;
    break;
  case Session_option_impl::PRIORITY:
    m_user_priorities = false;
    break;
  case Session_option_impl::SSL_CA:
    m_ssl_ca = false;
    break;
  case Session_option_impl::SSL_MODE:
    m_ssl_mode = SSL_mode::LAST;
    break;
  case Session_option_impl::CONNECTION_ATTRIBUTES:
    clear_connection_attr();
    break;
  default:
    break;
  }
}


}  // common namespace

MYSQLX_ABI_END(2,0)

}  // mysqlx namespace

#endif


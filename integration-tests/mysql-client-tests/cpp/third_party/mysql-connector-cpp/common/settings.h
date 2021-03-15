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

#ifndef MYSQLX_COMMON_SETTINGS_INT_H
#define MYSQLX_COMMON_SETTINGS_INT_H

#include "common.h"
#include "value.h"

#include <mysql/cdk.h>
#include <json_parser.h>
#include <uri_parser.h>

PUSH_SYS_WARNINGS
#include <vector>
#include <bitset>
#include <sstream>
POP_SYS_WARNINGS

namespace mysqlx {
MYSQLX_ABI_BEGIN(2,0)
namespace common {

/*
  A class for "transactional" adding new settings to a given
  Settings_impl instance.

  The primary way of specifying new options is via CDK document containing
  key-value pairs with options or option names and their values. A Setter
  instance can act as a processor for such CDK document.
*/

class Settings_impl::Setter
  : public cdk::JSON::Processor
  , public parser::URI_processor
  , cdk::JSON::Processor::Any_prc
  , cdk::JSON::Processor::Any_prc::Scalar_prc
  , cdk::JSON::Processor::Any_prc::List_prc
{
  Settings_impl &m_settings;
  Settings_impl::Data m_data;


  // SessionOption is > 0
  // ClientOption is < 0
  int m_cur_opt = 0;

  void set_comma_separated(int opt, const std::string& val);

  /*
    Note: Methods below would be best implemented inside Settings_impl::Data
    class, but this can break ABI, so we put them here instead.
  */

  using iterator = option_list_t::const_reverse_iterator;

  iterator find_opt(int opt, iterator start) const;

  iterator find_opt(int opt) const
  {
    return find_opt(opt, m_data.m_options.crbegin());
  }

  iterator end() const
  {
    return m_data.m_options.crend();
  }

  bool has_option(Session_option_impl opt)
  {
    return end() != find_opt(opt);
  }

  bool has_option(Client_option_impl opt)
  {
    return end() != find_opt(-opt);
  }

public:

  Setter(Settings_impl &settings)
    : m_settings(settings)
    , m_data(settings.m_data)
  {}


  void set_client_opts(const Settings_impl &opts)
  {
    Setter set(*this);
    for (auto &opt_val : opts.m_data.m_options)
    {
      set.add_option(opt_val.first, opt_val.second);
    }
    set.commit();
  }

  /*
    This method should be called after setting options to actually update
    settings in the Settings_impl instance. Settings are updated only if
    all consistency checks are passed.
  */

  void commit()
  {
    if (has_option(Session_option_impl::DNS_SRV))
    {
      if (0 == m_data.m_host_cnt)
      {
        throw_error("No DNS name specified for SRV lookup");
      }

      if (1 < m_data.m_host_cnt)
      {
        throw_error(
          "Specifying multiple hostnames with DNS SRV look up is not allowed."
        );
      }

      if (m_data.m_sock)
      {
        throw_error(
          "Using Unix domain sockets with DNS SRV lookup is not allowed."
        );
      }

      if (m_data.m_user_priorities)
      {
        throw_error(
          "Specifying a priority with DNS SRV lookup is not allowed."
        );
      }

      if (has_option(Session_option_impl::PORT))
      {
        throw_error(
          "Specifying a port number with DNS SRV lookup is not allowed."
        );
      }
    }

    /*
      If more hosts are added to the settings, error if the first host was
      defined by PORT only, without explicit HOST setting.
    */

    if (
      m_data.m_tcpip && m_settings.m_data.m_tcpip
      && 0 == m_settings.m_data.m_host_cnt
    )
      throw_error("PORT without explicit HOST in multi-host settings");

    /*
      Check if priority is missing in case some priorities were specified
      earlier.
    */

    if (m_data.m_user_priorities && (m_host && !m_prio))
      throw_error("Expected PRIORITY for a host in multi-host settings");

    /*
      If all is OK, copy settings collected here to the parent settings
      object.
    */

    m_settings.m_data = std::move(m_data);
  }

  // JSON doc processor

  void doc_end() override
  {
    commit();
  }

  Any_prc* key_val(const string &opt) override
  {
    /*
      Note: This overload is used only when getting options from a
      JSON document. Currently only client options can be set that way,
      and the only possible top-level client option is 'pooling'.

      TODO: Generic infrastructure for handling an alternative way of setting
      options using structured documents (current implementation assumes
      flat options structure).
    */

    if (to_upper(opt) != "POOLING")
    {
      std::string msg = "Invalid client option: " + opt;
      throw_error(msg.c_str());
      return nullptr;
    }

    return key_val(Client_option_impl::POOLING);
  }

  Any_prc* key_val(int opt)
  {
    m_cur_opt = opt;
    return this;
  }

private:


  /*
    Add or replace option value, report error if option defined twice (except
    for options which build the list of hosts).
  */

  template <typename T> void add_option(int, const T&);

  // State used for option consistency checks.

  bool m_host = false;
  bool m_port = false;
  bool m_socket = false;
  bool m_prio = false;
  std::set<int> m_opt_set;
  int m_prev_option = 0;

  /*
    Certaion options can be defined multiple times only if m_multi is true.
    This is used to handle options that take list of values.
  */

  bool m_multi = false;

  // Set option value doing all consistency checks.

  template <int OPT, typename T>
  void set_option(const T &val)
  {
    if (OPT == Session_option_impl::CONNECT_TIMEOUT)
      throw_error("The connection timeout value must be a positive integer (including 0)");

    add_option(OPT, val);
  }

  template <int OPT>
  void set_option(const int &val)
  {
    if (0 > val)
      throw_error("Option value can not be a negative number");
    set_option<OPT>((unsigned)val);
  }

  template <int OPT, typename T>
  void set_cli_option(const T &val)
  {
    add_option(OPT, val);
  }

  template <int OPT>
  void set_cli_option(const int &val)
  {
    if (0 > val)
      throw_error("Option value can not be a negative number");
    set_option<OPT>((unsigned)val);
  }


  // Any processor

  Scalar_prc* scalar() override
  {
    return this;
  }

  // Array values.

  List_prc* arr() override
  {
    switch (m_cur_opt)
    {
    // Note: allow multiple definitions of these options to store list
    // of values, but only if they were not defined before.

    case Session_option_impl::TLS_CIPHERSUITES:
      m_multi = !m_data.m_tls_ciphers;
      m_data.m_tls_ciphers = true;
      break;

    case Session_option_impl::TLS_VERSIONS:
      m_multi = !m_data.m_tls_vers;
      m_data.m_tls_vers = true;
      break;

    default:
      {
        std::stringstream err_msg;
        err_msg << "Option " << option_name(m_cur_opt) <<
          " does not accept array values";
        throw_error(err_msg.str().c_str());
      }
      return nullptr; // Keep compiler happy
    }

    // Even if no values given for a list option, we still consider it set to
    // an empty list
    m_opt_set.insert(m_cur_opt);
    return this;
  }

  virtual void list_end() override
  {
    m_multi = false;
  }

  Element_prc* list_el() override
  {
    return this;
  }

  // Document values.

  Doc_prc* doc() override
  {
    switch (m_cur_opt)
    {
    case Client_option_impl::POOLING:
      return &m_pool_processor;

    case Session_option_impl::CONNECTION_ATTRIBUTES:
      return &m_attr_processor;

    default:
      {
        std::stringstream err_msg;
        err_msg << "Option " << option_name(m_cur_opt) <<
          " does not accept document values";
        throw_error(err_msg.str().c_str());
      }
    }

    return nullptr;
  }

  // Scalar processor

  void str(const string &val) override;
  void num(uint64_t val) override;
  void null() override;

  void num(int64_t val) override
  {
    if (0 > val)
      throw_error("Option value can not be a negative number");
    num(uint64_t(val));
  }

  void yesno(bool) override;

  // These value types should not be used

  void num(float) override
  {
    assert(false);
  }

  void num(double) override
  {
    assert(false);
  }

public:

  // URI processor

  void scheme(const std::string &) override;
  void user(const std::string &usr) override;
  void password(const std::string &pwd) override;
  void schema(const std::string &db) override;

  void host(unsigned short priority, const std::string &host) override;

  void host(
    unsigned short priority,
    const std::string &host,
    unsigned short port
  ) override;

  void socket(unsigned short priority, const std::string &path) override;

  void pipe(unsigned short /*priority*/, const std::string &/*pipe*/) override
  {
    // should not happen
    assert(false);
  }

  /*
    Callbacks for reporting the query component, which is a sequence
    of key-value pair. Keys without any value are allowed. Key value
    can be a list: "...&key=[v1,..,vN]&...".
  */

  void key_val(const std::string &key, const std::string &val) override;
  void key_val(const std::string &key) override;
  void key_val(const std::string &key, const std::list<std::string>&) override;

  static int get_uri_option(const std::string&);

private:

  // Processors for processing document option values.

  // Connection attributes.

  struct Attr_processor
      : parser::JSON_parser::Processor
      , Any_prc
      , Scalar_prc
  {
    Settings_impl::Data &m_data;
    string m_key;
    Attr_processor(Settings_impl::Data &data) //ings_impl::Data &data)
      : m_data(data)
    {}

    Any_prc* key_val(const string &key) override
    {
      if (key.length() == 0)
        throw_error("Invalid empty key on connection attributes");
      if (key[0] == '_')
        throw_error("Connection attribute names cannot start with \"_\".");
      m_key = key;
      return this;
    }

    Scalar_prc* scalar() override
    {
      return this;
    }

    // Arrays and documents are not valid... throw error
    List_prc*   arr() override
    {
      throw_error("Connection attribute can not be an array");
      return nullptr;
    }

    // Report that any value is a document.

    Doc_prc*    doc() override
    {
      throw_error("Connection attribute can not be a document");
      return nullptr;
    }

    void null() override
    {
      m_data.m_connection_attr[m_key];
    }
    void str(const string &val) override
    {
      m_data.m_connection_attr[m_key] = val;
    }
    virtual void num(uint64_t)override
    {throw_error("Connection attributes values can't be of integer type");}
    virtual void num(int64_t) override
    {throw_error("Connection attributes values can't be of integer type");}
    virtual void num(float)   override
    {throw_error("Connection attributes values can't be of integer type");}
    virtual void num(double)  override
    {throw_error("Connection attributes values can't be of integer type");}
    virtual void yesno(bool)  override
    {throw_error("Connection attributes values can't be of boolean type");}

  }
  m_attr_processor{ m_data };

  // Pool settings.

  struct Pool_processor
      : parser::JSON_parser::Processor
      , Any_prc
  {
    Setter &m_setter;
    string m_key;

    Pool_processor(Setter &setter) //ings_impl::Data &data)
      : m_setter(setter)
    {}

    Any_prc* key_val(const string &key) override
    {
      std::string upper_key = to_upper(key);

      if (upper_key == "ENABLED")
        return this;
      else if (upper_key == "MAXSIZE")
        return m_setter.key_val(Client_option_impl::POOL_MAX_SIZE);
      else if (upper_key == "QUEUETIMEOUT")
        return m_setter.key_val(Client_option_impl::POOL_QUEUE_TIMEOUT);
      else if (upper_key == "MAXIDLETIME")
        return m_setter.key_val(Client_option_impl::POOL_MAX_IDLE_TIME);

      std::string msg = "Invalid pooling option: " + key;
      throw_error(msg.c_str());
      // Quiet compiler warnings
      return nullptr;
    }

    Scalar_prc* scalar() override
    {
      // 'pooling.enabled' is equivalent to scalar value of POOLING option
      return m_setter.key_val(Client_option_impl::POOLING)->scalar();
    }

    List_prc*   arr() override
    {
      throw_error("Value of 'pooling.enabled' option can be only true or false");
      return nullptr;
    }

    Doc_prc*    doc() override
    {
      throw_error("Value of 'pooling.enabled' option can be only true or false");
      return nullptr;
    }
  }
  m_pool_processor{ *this };

};


inline
auto Settings_impl::Setter::find_opt(int opt, iterator start) const
-> iterator
{
  return std::find_if(start, m_data.m_options.crend(),
    [opt](opt_val_t el) -> bool { return el.first == opt; }
  );
}


/*
  Logic for handling individual options.
*/


// Options which build a list of hosts.



template<>
inline void
Settings_impl::Setter::set_option<Settings_impl::Session_option_impl::HOST>(
  const std::string &val
)
{
  if (0 == m_data.m_host_cnt && m_port)
    throw_error("PORT without prior host specification in multi-host settings");

  /*
    In the case of explicit priorities, if a previous host was added, check that
    a priority was specified for the previous host.
  */

  if (m_data.m_user_priorities && m_host && !m_prio)
    throw_error("PRIORITY not set for all hosts in a multi-host settings");

  m_host = true;
  m_port = false;
  m_socket = false;
  m_prio = false;
  ++m_data.m_host_cnt;
  m_data.m_tcpip = true;
  add_option(Session_option_impl::HOST, val);
}



template<>
inline void
Settings_impl::Setter::set_option<Settings_impl::Session_option_impl::SOCKET>(
#ifdef _WIN32
  const std::string&
#else
  const std::string &val
#endif
)
{
#ifdef _WIN32

  throw_error("SOCKET option not supported on Windows");

#else

  /*
    In the case of explicit priorities, if a previous host was added, check that
    a priority was specified for the previous host.
  */

  if (m_data.m_user_priorities && m_host && !m_prio)
    throw_error("PRIORITY not set for all hosts in a multi-host settings");

  m_host = true;
  m_socket = true;
  m_prio = false;
  m_port = false;
  ++m_data.m_host_cnt;
  m_data.m_sock = true;
  add_option(Session_option_impl::SOCKET, val);

#endif
}


template<>
inline void
Settings_impl::Setter::set_option<Settings_impl::Session_option_impl::PORT>(
  const unsigned &val
)
{
  if (m_port)
    throw_error("duplicate PORT value");  // TODO: overwrite instead?

  if (0 < m_data.m_host_cnt && (Session_option_impl::HOST != m_prev_option))
    throw_error("PORT must follow HOST setting in multi-host settings");

  if (m_socket)
    throw_error("Invalid PORT setting for socked-based connection");

  if (m_prio)
    throw_error("PORT should be specified before PRIORITY");

  if (val > 65535U)
    throw_error("Port value out of range");

  m_port = true;
  m_data.m_tcpip = true;
  add_option(Session_option_impl::PORT, val);
}


template<>
inline void
Settings_impl::Setter::set_option<Settings_impl::Session_option_impl::PRIORITY>(
  const unsigned &val
)
{
  switch (m_prev_option)
  {
  case Session_option_impl::HOST:
  case Session_option_impl::PORT:
  case Session_option_impl::SOCKET:
    break;
  default:
    throw_error("PRIORITY must directly follow host specification");
  };

  if (m_prio)
    throw_error("duplicate PRIORITY value");  // TODO: overwrite instead?

  /*
    Using PRIORITY implies multi-host settings and then each host must be
    defined explicitly.
  */

  if (!m_host)
    throw_error("PRIORITY without prior host specification");

  if (1 < m_data.m_host_cnt && !m_data.m_user_priorities)
    throw_error("PRIORITY not set for all hosts in a multi-host settings");

  if (val > 100)
    throw_error("PRIORITY should be a number between 0 and 100");

  m_data.m_user_priorities = true;
  m_prio = true;
  add_option(Session_option_impl::PRIORITY, val);
}


template<>
inline void
Settings_impl::Setter::set_option<Settings_impl::Session_option_impl::COMPRESSION>(
  const unsigned &val
)
{
  if (val >= size_t(Compression_mode::LAST))
    throw_error("Invalid Compression value");
  add_option(Session_option_impl::COMPRESSION, val);
}

template<>
inline void
Settings_impl::Setter::set_option<Settings_impl::Session_option_impl::COMPRESSION>(
  const std::string &val
  )
{
  using std::map;

#define COMPRESSION_MAP(X,N) { #X, Compression_mode::X },

  static map< std::string, Compression_mode > compression_map{
    COMPRESSION_MODE_LIST(COMPRESSION_MAP)
  };

  try {

    Compression_mode m = compression_map.at(to_upper(val));

    if (Compression_mode::LAST == m)
      throw std::out_of_range("");

    set_option<Session_option_impl::COMPRESSION>(unsigned(m));
    return;
  }
  catch (const std::out_of_range&)
  {
    std::string msg = "Invalid compression mode: " + val;
    throw_error(msg.c_str());
    // Quiet compiler warnings
    return;
  }
}


// SSL options.


template<>
inline void
Settings_impl::Setter::set_option<Settings_impl::Session_option_impl::SSL_MODE>(
  const unsigned &val
)
{
  if (val >= size_t(SSL_mode::LAST))
    throw_error("Invalid SSL_MODE value");
  m_data.m_ssl_mode = SSL_mode(val);

#ifndef WITH_SSL
  if (SSL_mode::DISABLED != m_ssl_mode)
    throw_error("secure connection requested but SSL is not supported")
#endif

  switch (m_data.m_ssl_mode)
  {
  case SSL_mode::VERIFY_CA:
  case SSL_mode::VERIFY_IDENTITY:
    break;

  default:
    if (m_data.m_ssl_ca)
      throw_error("SSL_MODE ... not valid when SSL_CA is set");
  }

  add_option(Session_option_impl::SSL_MODE, val);
}


template<>
inline void
Settings_impl::Setter::set_option<Settings_impl::Session_option_impl::SSL_CA>(
  const std::string &val
)
{
#ifndef WITH_SSL
  throw_error("SSL_CA option specified but SSL is not supported")
#endif

  switch (m_data.m_ssl_mode)
  {
  case SSL_mode::VERIFY_CA:
  case SSL_mode::VERIFY_IDENTITY:
  case SSL_mode::LAST:
    break;

  default:
    throw_error("SSL_CA option is not compatible with SSL_MODE ...");
  }

  m_data.m_ssl_ca = true;
  add_option(Session_option_impl::SSL_CA, val);
}


template <>
inline void
Settings_impl::Setter::set_option<Settings_impl::Session_option_impl::CONNECT_TIMEOUT>(
  const uint64_t &timeout
)
{
  add_option(Settings_impl::Session_option_impl::CONNECT_TIMEOUT, timeout);
}

template<>
inline void
Settings_impl::Setter::set_option<Settings_impl::Session_option_impl::SSL_MODE>(
  const std::string &val
)
{
  using std::map;

#define SSL_MODE_MAP(X,N) { #X, SSL_mode::X },

  static map< std::string, SSL_mode > option_map{
    SSL_MODE_LIST(SSL_MODE_MAP)
  };

  try {

    SSL_mode opt = option_map.at(to_upper(val));

    if (SSL_mode::LAST == opt)
      throw std::out_of_range("");

    set_option<Session_option_impl::SSL_MODE>(unsigned(opt));
    return;
  }
  catch (const std::out_of_range&)
  {
    std::string msg = "Invalid ssl mode value: " + val;
    throw_error(msg.c_str());
    // Quiet compiler warnings
    return;
  }
}


// Authentication options.

template<>
inline void
Settings_impl::Setter::set_option<Settings_impl::Session_option_impl::AUTH>(
  const unsigned &val
)
{
  if (val >= size_t(Auth_method::LAST))
    throw_error("Invalid auth method");
  add_option(Session_option_impl::AUTH, val);
}


template<>
inline void
Settings_impl::Setter::set_option<Settings_impl::Session_option_impl::AUTH>(
  const std::string &val
)
{
  using std::map;

#define AUTH_MAP(X,N) { #X, Auth_method::X },

  static map< std::string, Auth_method > auth_map{
    AUTH_METHOD_LIST(AUTH_MAP)
  };

  try {

    Auth_method m = auth_map.at(to_upper(val));

    if (Auth_method::LAST == m)
      throw std::out_of_range("");

    set_option<Session_option_impl::AUTH>(unsigned(m));
    return;
  }
  catch (const std::out_of_range&)
  {
    std::string msg = "Invalid auth method: " + val;
    throw_error(msg.c_str());
    // Quiet compiler warnings
    return;
  }
}


// Connection attributes.


template<>
inline void
Settings_impl::Setter::set_option<
  Settings_impl::Session_option_impl::CONNECTION_ATTRIBUTES>(const bool& val)
{
  if (val)
    m_data.init_connection_attr();
  else
    m_data.clear_connection_attr();
}


template<>
inline void
Settings_impl::Setter::set_option<
Settings_impl::Session_option_impl::CONNECTION_ATTRIBUTES>(const std::string &val)
{

  struct processor
    : parser::JSON_parser::Processor
    , Any_prc
    , Scalar_prc
  {
    Settings_impl::Data &m_data;
    string m_key;
    processor(Settings_impl::Data &data)
      : m_data(data)
    {}

    Any_prc* key_val(const string &key) override
    {
      if (key.length() == 0)
        throw_error("Invalid empty key on connection attributes");
      if (key[0] == '_')
        throw_error("Connection attribute names cannot start with \"_\".");
      m_key = key;
      return this;
    }

    Scalar_prc* scalar() override
    {
      return this;
    }

    // Arrays and documents are not valid... throw error
    List_prc*   arr() override
    {
      throw_error("Connection attribute can not be an array");
      return nullptr;
    }

    // Report that any value is a document.

    Doc_prc*    doc() override
    {
      throw_error("Connection attribute can not be a document");
      return nullptr;
    }

    void null() override
    {
      m_data.m_connection_attr[m_key];
    }
    void str(const string &val) override
    {
      m_data.m_connection_attr[m_key] = val;
    }
    virtual void num(uint64_t)override
    {throw_error("Connection attributes values can't be of integer type");}
    virtual void num(int64_t) override
    {throw_error("Connection attributes values can't be of integer type");}
    virtual void num(float)   override
    {throw_error("Connection attributes values can't be of integer type");}
    virtual void num(double)  override
    {throw_error("Connection attributes values can't be of integer type");}
    virtual void yesno(bool)  override
    {throw_error("Connection attributes values can't be of boolean type");}

  };

  parser::JSON_parser parser(val);
  processor prc(m_data);

  parser.process(prc);
}


// TODO: support std::string for PWD and other options that are ascii only?

template<>
inline void
Settings_impl::Setter::set_option<Settings_impl::Session_option_impl::URI>(
  const std::string &val
)
{
  parser::URI_parser  parser(val);
  parser.process(*this);
}


template<>
inline void
Settings_impl::Setter::set_cli_option<
  Settings_impl::Client_option_impl::POOL_MAX_SIZE
>(const uint64_t &val)
{
  if (val == 0)
    throw_error("Max pool size has to be greater than 0");
  add_option(Settings_impl::Client_option_impl::POOL_MAX_SIZE, val);
}

inline void
Settings_impl::Setter::set_comma_separated(int opt, const std::string& val)
{
  std::string lval = "";

  for (auto it = val.begin(); it != val.end(); it++)
  {
    const char c = *it;
    if (isspace(static_cast<unsigned char>(c)) ||
        c == ',')
    {
      if (lval.length())
      {
        add_option(opt, lval);
        lval = "";

        // If first add_option() was OK, then we disable duplicate checks
        // to allow adding remaining values of the option in the
        // following iterations

        m_multi = true; // _state = LIST_PROCESS;
      }
      continue;
    }
    lval += c;
  }

  if (lval.length())
    add_option(opt, lval); // Add the last value

  m_multi = false; // _state = LIST_END;
}

template<>
inline void
Settings_impl::Setter::set_option<
  Settings_impl::Session_option_impl::TLS_CIPHERSUITES
>(const std::string &val)
{
  m_data.m_tls_ciphers = true;  // record that the option was set

  // If in multi mode, the value is a single list element, otherwise
  // the value can be a comma separated list

  if (!m_multi)
    set_comma_separated((int)Settings_impl::Session_option_impl::TLS_CIPHERSUITES, val);
  else
    add_option((int)Settings_impl::Session_option_impl::TLS_CIPHERSUITES, val);
}

template<>
inline void
Settings_impl::Setter::set_option<
  Settings_impl::Session_option_impl::TLS_VERSIONS
>(const std::string &val)
{
  m_data.m_tls_vers = true;  // record that the option was set

  // If in multi mode, the value is a single list element, otherwise
  // the value can be a comma separated list

  if (!m_multi)
    set_comma_separated((int)Settings_impl::Session_option_impl::TLS_VERSIONS, val);
  else
    add_option((int)Settings_impl::Session_option_impl::TLS_VERSIONS, val);
}


// Generic add_option() method.


template <typename T>
inline
void Settings_impl::Setter::add_option(int opt, const T &val)
{
  auto &options = m_data.m_options;
  m_prev_option = opt;

  switch (opt)
  {
  case Session_option_impl::HOST:
  case Session_option_impl::SOCKET:
  case Session_option_impl::PORT:
  case Session_option_impl::PRIORITY:
    options.emplace_back(opt, val);
    return;

  case Session_option_impl::TLS_CIPHERSUITES:
  case Session_option_impl::TLS_VERSIONS:
    if (m_multi)
    {
      options.emplace_back(opt, val);
      m_opt_set.insert(opt);  // needed for double check when m_multi is false
      return;
    }
    // if multi mode not enabled, fall-through to check for doubled option

  default:
    // Check for doubled option
    if (0 < m_opt_set.count(opt))
    {
      std::string msg = "Option ";
      msg += option_name(opt);
      msg += " defined twice";
      throw_error(msg.c_str());
      return;
    }
    m_opt_set.insert(opt);
  }

  auto it = options.begin();
  for (; it != options.end(); ++it)
  {
    if (it->first == opt)
    {
      it->second = val;
      break;
    }
  }

  if (it == options.end())
  {
    options.emplace_back(opt, val);
  }
}


// Value processor

inline
void Settings_impl::Setter::str(const string &val)
{
  // TODO: avoid utf8 conversions
  std::string utf8_val(val);

  auto to_number = [&]() -> uint64_t
  {
    std::size_t pos;
    long long x = std::stoll(utf8_val, &pos);

    if (x < 0)
      throw_error("Option ... accepts only non-negative values");

    if (pos != val.length())
      throw_error("Option ... accepts only integer values");

    return x;
  };

#define SET_OPTION_STR_str(X,N) \
  case Session_option_impl::X: return set_option<Session_option_impl::X,std::string>(utf8_val);
#define SET_OPTION_STR_any(X,N) SET_OPTION_STR_str(X,N)
#define SET_OPTION_STR_bool(X,N) SET_OPTION_STR_num(X,N)
#define SET_OPTION_STR_num(X,N) \
  case Session_option_impl::X: \
  try \
  { \
    return set_option<Session_option_impl::X,uint64_t>(to_number()); \
  } \
  catch (const std::invalid_argument&) \
  { \
    throw_error("Can not convert to integer value"); \
  }

  #define SET_OPTION_STR_bool(X,N) SET_OPTION_STR_num(X,N)

  switch (m_cur_opt)
  {
    SESSION_OPTION_LIST(SET_OPTION_STR)

  default:
    throw_error("Option ... could not be processed.");
  }

}


inline
void Settings_impl::Setter::num(uint64_t val)
{
#define SET_OPTION_NUM_num(X,N) \
  case Session_option_impl::X: return set_option<Session_option_impl::X,unsigned>((unsigned)val);
#define SET_OPTION_NUM_any(X,N) SET_OPTION_NUM_num(X,N)
#define SET_OPTION_NUM_bool(X,N) SET_OPTION_NUM_num(X,N)
#define SET_OPTION_NUM_str(X,N)

#define SET_CLI_OPTION_NUM_num(X,N) \
  case Client_option_impl::X: return set_cli_option<Client_option_impl::X,uint64_t>(val);
#define SET_CLI_OPTION_NUM_bool(X,N) SET_CLI_OPTION_NUM_num(X,N)
#define SET_CLI_OPTION_NUM_any(X,N) SET_CLI_OPTION_NUM_num(X,N)
#define SET_CLI_OPTION_NUM_str(X,N)

  /*
    This cannot be processed inside switch because the numeric
    values are converted to unsigned int. For timeout uint64_t is
    required
  */
  if (m_cur_opt == Session_option_impl::CONNECT_TIMEOUT)
    return set_option<Session_option_impl::CONNECT_TIMEOUT>(val);

  if (m_cur_opt == Session_option_impl::CONNECT_TIMEOUT)
    return set_option<Session_option_impl::CONNECT_TIMEOUT>(val);

  if (m_cur_opt < 0 && !check_num_limits<int64_t>(val))
    throw_error("Option ... value too big");

  switch (m_cur_opt)
  {
    SESSION_OPTION_LIST(SET_OPTION_NUM)
    CLIENT_OPTION_LIST(SET_CLI_OPTION_NUM)
    default:break;
  }

  throw_error("Option ... does not accept numeric values.");
}


inline
void Settings_impl::Setter::null()
{
  switch (m_cur_opt)
  {
  case Session_option_impl::HOST:
  case Session_option_impl::PORT:
  case Session_option_impl::PRIORITY:
  case Session_option_impl::USER:
    throw_error("Option ... can not be unset");
    break;
  case Session_option_impl::LAST:
    break;
  default:
    m_data.erase(m_cur_opt);
  }

}


inline
void Settings_impl::Setter::yesno(bool val)
{

#define SET_OPTION_BOOL_bool(X,N) \
  case Session_option_impl::X: return set_option<Session_option_impl::X, bool>(val);
#define SET_OPTION_BOOL_any(X,N) \
  case Session_option_impl::X: break;
#define SET_OPTION_BOOL_num(X,N) SET_OPTION_BOOL_any(X,N)
#define SET_OPTION_BOOL_str(X,N) SET_OPTION_BOOL_any(X,N)

#define SET_CLI_OPTION_BOOL_bool(X,N) \
  case Client_option_impl::X: return set_option<Client_option_impl::X, bool>(val);
#define SET_CLI_OPTION_BOOL_any(X,N) \
  case Client_option_impl::X: break;
#define SET_CLI_OPTION_BOOL_num(X,N) SET_CLI_OPTION_BOOL_any(X,N)
#define SET_CLI_OPTION_BOOL_str(X,N) SET_CLI_OPTION_BOOL_any(X,N)

  switch (m_cur_opt)
  {
    SESSION_OPTION_LIST(SET_OPTION_BOOL)
    CLIENT_OPTION_LIST(SET_CLI_OPTION_BOOL)
  default:
    break;
  }

  /*
    Special handling of CONNECTION_ATTRIBUTES option which is declared
    as string option, but it can be also set to a bool value.
  */

  switch (m_cur_opt)
  {
    SET_OPTION_BOOL_bool(CONNECTION_ATTRIBUTES, val)
  default:
    break;
  }

  throw_error("Option ... can not be bool");
}


// URI processor

inline
void Settings_impl::Setter::scheme(const std::string &_scheme)
{
  if(_scheme == "mysqlx+srv")
    set_option<Session_option_impl::DNS_SRV>(true);
}

inline
void Settings_impl::Setter::user(const std::string &usr)
{
  set_option<Session_option_impl::USER>(usr);
}

inline
void Settings_impl::Setter::password(const std::string &pwd)
{
  set_option<Session_option_impl::PWD>(pwd);
}

inline
void Settings_impl::Setter::schema(const std::string &db)
{
  set_option<Session_option_impl::DB>(db);
}

inline
void Settings_impl::Setter::host(
  unsigned short priority, const std::string &host
)
{
  set_option<Session_option_impl::HOST>(host);
  if (0 < priority)
    set_option<Session_option_impl::PRIORITY>(priority - 1);
}

inline
void Settings_impl::Setter::host(
  unsigned short priority,
  const std::string &host,
  unsigned short port
)
{
  set_option<Session_option_impl::HOST>(host);
  set_option<Session_option_impl::PORT>(port);
  if (0 < priority)
    set_option<Session_option_impl::PRIORITY>(priority - 1);
}

inline
void Settings_impl::Setter::socket(unsigned short priority, const std::string &path)
{
  set_option<Session_option_impl::SOCKET>(path);
  if (0 < priority)
    set_option<Session_option_impl::PRIORITY>(priority - 1);
}

inline
void Settings_impl::Setter::key_val(const std::string &key, const std::string &val)
{
  try {
    auto option = get_uri_option(key);
    switch(option)
    {
    case Settings_impl::Session_option_impl::CONNECTION_ATTRIBUTES:
        {
          std::string tmp = to_lower(val);

          if (tmp == "false")
          {
            m_data.m_connection_attr.clear();
          }
          else if (tmp == "true")
          {
            m_data.init_connection_attr();
          }
          else
          {
            throw_error("The value of a \"session-connect-attribute\" must be "
                        "either a Boolean or a list of key-value pairs.");
          }
        }
        break;
      default:
        key_val(get_uri_option(key))->scalar()->str(val);
    }
  }
  catch (const std::out_of_range&)
  {
    throw_error("Invalid URI option ...");
  }
}

inline
void Settings_impl::Setter::key_val(const std::string &key)
{
  try {
    switch(get_uri_option(key))
    {
      case Session_option_impl::CONNECTION_ATTRIBUTES:
        m_data.init_connection_attr();
        return;
      default:
        throw_error("Option ... requires a value");
    }
  }
  catch (const std::out_of_range&)
  {
    throw_error("invalid URI option ...");
  }
}

inline
void Settings_impl::Setter::key_val(const std::string &key,
                                    const std::list<std::string> &list)
{
  try {
    int option = get_uri_option(key);
    switch(option)
    {
      case Settings_impl::Session_option_impl::CONNECTION_ATTRIBUTES:
        for(auto el : list)
        {
          if (el.empty())
            continue;
          size_t eq = el.find("=");
          std::string attr = el.substr(0,eq);
          if (attr[0]== '_')
            throw_error("Connection attribute names cannot start with \"_\".");
          auto &attr_pos = m_data.m_connection_attr[attr];
          if (eq != std::string::npos)
            attr_pos = el.substr(eq+1);

        }
        break;

      case Settings_impl::Session_option_impl::TLS_CIPHERSUITES:
      case Settings_impl::Session_option_impl::TLS_VERSIONS:
        {
          auto *prc = key_val(option)->arr();
          if (!prc)
            break;

          prc->list_begin();
          for (auto el : list)
          {
            if (el.empty())
              continue;
            safe_prc(prc->list_el())->scalar()->str(el);
          }
          prc->list_end();
        }
        break;

      default:
        std::stringstream err;
        err << "Option " << key << " does not accept a list value";
        throw_error(err.str().c_str());
        break;

    }
  }
  catch (const std::out_of_range&)
  {
    throw_error("Invalid URI option ...");
  }
}


inline
int
Settings_impl::Setter::get_uri_option(const std::string &name)
{
  using std::map;

#define URI_OPT_MAP(X,Y) { X, Y },

  static map< std::string, int > uri_map{
    URI_OPTION_LIST(URI_OPT_MAP)
  };

  int opt = uri_map.at(to_lower(name));
  assert(Session_option_impl::LAST != opt);
  return opt;
}


}  // common
MYSQLX_ABI_END(2,0)
}  // mysqlx namespace

#endif

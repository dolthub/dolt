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

#ifndef MYSQLX_DEVAPI_SETTINGS_H
#define MYSQLX_DEVAPI_SETTINGS_H

/**
  @file
  TODO
*/

#include "common.h"
#include "detail/settings.h"


namespace mysqlx {
MYSQLX_ABI_BEGIN(2,0)


/*
  TODO: Cross-references to session options inside Doxygen docs do not work.
*/

/**
  Session creation options

  @note `PRIORITY` should be defined after a `HOST` (`PORT`) to which
  it applies.

  @note Specifying `SSL_CA` option requires `SSL_MODE` value of `VERIFY_CA`
  or `VERIFY_IDENTITY`. If `SSL_MODE` is not explicitly given then
  setting `SSL_CA` implies `VERIFY_CA`.
*/

class SessionOption
{
#define SESS_OPT_ENUM_any(X,N) X = N,
#define SESS_OPT_ENUM_bool(X,N) X = N,
#define SESS_OPT_ENUM_num(X,N) X = N,
#define SESS_OPT_ENUM_str(X,N) X = N,
#define SESS_OPT_ENUM_bool(X,N) X = N,

public:

  enum Enum {
    SESSION_OPTION_LIST(SESS_OPT_ENUM)
    LAST
  };

  SessionOption(Enum opt)
    : m_opt(opt)
  {}

  SessionOption()
  {}

  bool operator==(const SessionOption &other) const
  {
    return m_opt == other.m_opt;
  }

  bool operator==(Enum opt) const
  {
    return m_opt == opt;
  }

  bool operator!=(Enum opt) const
  {
    return m_opt != opt;
  }

  operator int()
  {
    return m_opt;
  }

protected:
  int m_opt = LAST;
};


/**
  Client creation options
*/

class ClientOption
  : protected SessionOption
{

#define CLIENT_OPT_ENUM_any(X,N) X = -N,
#define CLIENT_OPT_ENUM_bool(X,N) X = -N,
#define CLIENT_OPT_ENUM_num(X,N) X = -N,
#define CLIENT_OPT_ENUM_str(X,N) X = -N,


public:

  using SessionEnum = SessionOption::Enum;

  enum Enum {
    CLIENT_OPTION_LIST(CLIENT_OPT_ENUM)
  };

  ClientOption()
  {}

  ClientOption(SessionOption opt)
    : SessionOption(opt)
  {}

  ClientOption(Enum opt)
  {
    m_opt = opt;
  }

  ClientOption(SessionEnum opt)
    : SessionOption(opt)
  {}

  bool operator==(Enum opt) const
  {
    return m_opt == opt;
  }

  bool operator==(SessionEnum opt) const
  {
    return SessionOption::operator==(opt);
  }

  bool operator!=(Enum opt) const
  {
    return m_opt != opt;
  }

  bool operator!=(SessionEnum opt) const
  {
    return SessionOption::operator!=(opt);
  }

  inline operator int()
  {
    return SessionOption::operator int();
  }

};


/// @cond DISABLED
// Note: Doxygen gets confused here and renders docs incorrectly.

inline
std::string OptionName(ClientOption opt)
{
#define CLT_OPT_NAME_any(X,N) case ClientOption::X: return #X;
#define CLT_OPT_NAME_bool(X,N) CLT_OPT_NAME_any(X,N)
#define CLT_OPT_NAME_num(X,N) CLT_OPT_NAME_any(X,N)
#define CLT_OPT_NAME_str(X,N) CLT_OPT_NAME_any(X,N)

#define SESS_OPT_NAME_any(X,N) case SessionOption::X: return #X;
#define SESS_OPT_NAME_bool(X,N) SESS_OPT_NAME_any(X,N)
#define SESS_OPT_NAME_num(X,N) SESS_OPT_NAME_any(X,N)
#define SESS_OPT_NAME_str(X,N) SESS_OPT_NAME_any(X,N)
#define SESS_OPT_NAME_bool(X,N) SESS_OPT_NAME_any(X,N)


  switch (opt)
  {
    CLIENT_OPTION_LIST(CLT_OPT_NAME)
    SESSION_OPTION_LIST(SESS_OPT_NAME)

  default:
    throw_error("Unexpected Option"); return "";
  };
}

/// @endcond


inline std::string ClientOptionName(ClientOption opt)
{
  return OptionName(opt);
}

inline std::string SessionOptionName(SessionOption opt)
{
  return OptionName(opt);
}


/**
  Modes to be used with `SSL_MODE` option
  \anchor SSLMode
*/

enum_class SSLMode
{
#define SSL_ENUM(X,N) X = N,

  SSL_MODE_LIST(SSL_ENUM)
};


/// @cond DISABLED

inline
std::string SSLModeName(SSLMode m)
{
#define MODE_NAME(X,N) case SSLMode::X: return #X;

  switch (m)
  {
    SSL_MODE_LIST(MODE_NAME)
  default:
    {
      std::ostringstream buf;
      buf << "<UKNOWN (" << unsigned(m) << ")>" << std::ends;
      return buf.str();
    }
  };
}

/// @endcond


/**
  Authentication methods to be used with `AUTH` option.
  \anchor AuthMethod
*/

enum_class AuthMethod
{
#define AUTH_ENUM(X,N) X=N,

  AUTH_METHOD_LIST(AUTH_ENUM)
};


/// @cond DISABLED

inline
std::string AuthMethodName(AuthMethod m)
{
#define AUTH_NAME(X,N) case AuthMethod::X: return #X;

  switch (m)
  {
    AUTH_METHOD_LIST(AUTH_NAME)
  default:
    {
      std::ostringstream buf;
      buf << "<UKNOWN (" << unsigned(m) << ")>" << std::ends;
      return buf.str();
    }
  };
}

/// @endcond

/**
  Values to be used with `COMPRESSION` option
  \anchor CompressionMode
*/

enum_class CompressionMode
{
#define COMPRESSION_ENUM(X,N) X = N,

  COMPRESSION_MODE_LIST(COMPRESSION_ENUM)
};

/// @cond DISABLED

inline
std::string CompressionModeName(CompressionMode m)
{
#define COMPRESSION_NAME(X,N) case CompressionMode::X: return #X;

  switch (m)
  {
    COMPRESSION_MODE_LIST(COMPRESSION_NAME)
  default:
    {
      std::ostringstream buf;
      buf << "<UKNOWN (" << unsigned(m) << ")>" << std::ends;
      return buf.str();
    }
  };
}

/// @endcond


namespace internal {


/*
  Encapsulate public enumerations in the Settings_traits class to be used
  by Settings_detail<> template.
*/

struct Settings_traits
{
  using Options         = mysqlx::SessionOption;
  using COptions        = mysqlx::ClientOption;
  using SSLMode         = mysqlx::SSLMode;
  using AuthMethod      = mysqlx::AuthMethod;
  using CompressionMode = mysqlx::CompressionMode;

  static std::string get_mode_name(SSLMode mode)
  {
    return SSLModeName(mode);
  }

  static std::string get_option_name(COptions opt)
  {
    return ClientOptionName(opt);
  }

  static std::string get_auth_name(AuthMethod m)
  {
    return AuthMethodName(m);
  }
};


template<>
PUBLIC_API
void
internal::Settings_detail<internal::Settings_traits>::
do_set(session_opt_list_t &&opts);


template<typename Option, typename base_iterator>
class iterator
  : public std::iterator<std::input_iterator_tag,
                         std::pair<Option, mysqlx::Value>>
{
  base_iterator m_it;
  std::pair<Option, mysqlx::Value> m_pair;

public:
  iterator(const base_iterator &init)
    : m_it(init)
  {}

  iterator(const iterator &init)
    : m_it(init.m_it)
  {}


  std::pair<Option, mysqlx::Value>& operator*()
  {
    auto &el = *m_it;
    m_pair.first = static_cast<typename Option::Enum>(el.first);
    m_pair.second = el.second;
    return m_pair;
  }

  std::pair<ClientOption, mysqlx::Value>* operator->()
  {
    return &operator*();
  }

  iterator& operator++()
  {
    ++m_it;
    return *this;
  }

  iterator& operator--()
  {
    --m_it;
    return *this;
  }

  bool operator !=(const iterator &other)
  {
    return m_it != other.m_it;
  }

};


} // internal namespace


class Client;
class Session;

/**
  Represents session options to be passed at session creation time.

  SessionSettings can be constructed using a connection string, common
  connect options (host, port, user, password, database) or with a list
  of `SessionOption` constants, each followed by the option value.

  Examples:
  ~~~~~~

    SessionSettings from_url("mysqlx://user:pwd@host:port/db?ssl-mode=required");

    SessionSettings from_options("host", port, "user", "pwd", "db");

    SessionSettings from_option_list(
      SessionOption::USER, "user",
      SessionOption::PWD,  "pwd",
      SessionOption::HOST, "host",
      SessionOption::PORT, port,
      SessionOption::DB,   "db",
      SessionOption::SSL_MODE, SSLMode::REQUIRED
    );
  ~~~~~~

  The HOST, PORT and SOCKET settings can be repeated to build a list of hosts
  to be used by the connection fail-over logic when creating a session (see
  description of `Session` class). In that case each host can be assigned
  a priority by setting the `PRIORITY` option. If priorities are not explicitly
  assigned, hosts are tried in the order in which they are specified in session
  settings. If priorities are used, they must be assigned to all hosts
  specified in the settings.

  @ingroup devapi
*/

class SessionSettings
  : private internal::Settings_detail<internal::Settings_traits>
{
  using Value = mysqlx::Value;

public:

  /**
    Create session settings from a connection string.

    Connection sting has the form

          "user:pass@connection-data/db?option&option"

    with optional `mysqlx://` prefix.

    The `connetction-data` part is either a single host address or a coma
    separated list of hosts in square brackets: `[host1, host2, ..., hostN]`.
    In the latter case the connection fail-over logic will be used when
    creating the session.

    A single host address is either a DNS host name, an IPv4 address of
    the form `nn.nn.nn.nn` or an IPv6 address of the form `[nn:nn:nn:...]`.
    On Unix systems a host can be specified as a path to a Unix domain
    socket - this path must start with `/` or `.`.

    Characters like `/` in the connection data, which otherwise have a special
    meaning inside a connection string, must be represented using percent
    encoding (e.g., `%2F` for `/`). Another option is to enclose a host name or
    a socket path in round braces. For example, one can write

        "mysqlx://(./path/to/socket)/db"

    instead of

        "mysqlx://.%2Fpath%2Fto%2Fsocket/db"

    To specify priorities for hosts in a multi-host settings, use list of pairs
    of the form `(address=host,priority=N)`. If priorities are specified, they
    must be given to all hosts in the list.

    The optional `db` part of the connection string defines the default schema
    of the session.

    Possible connection options are:

    - `ssl-mode=...` : see `SessionOption::SSL_MODE`; the value is a case
         insensitive name of the SSL mode
    - `ssl-ca=...` : see `SessionOption::SSL_CA`
    - `auth=...`: see `SessionOption::AUTH`; the value is a case insensitive
        name of the authentication method
    - `connect-timeout=...`: see `SessionOption::CONNECT_TIMEOUT`
    - `connection-attributes=[...]` : see `SessionOption::CONNECTION_ATTRIBUTES`
      but the key-value pairs are not given by a JSON document but as a list;\n
      Examples:\n
      `"mysqlx://user@host?connection-attributes=[foo=bar,qux,baz=]"` -
        specify additional attributes to be sent\n
      `"mysqlx://user@host?connection-attributes=false"` -
        send no connection attributes\n
      `"mysqlx://user@host?connection-attributes=true"` -
        send default connection attributes\n
      `"mysqlx://user@host?connection-attributes=[]"` -
        the same as setting to `true`\n
      `"mysqlx://user@host?connection-attributes"` -
        the same as setting to `true`\n
    - `tls-versions=[...]` : see `SessionOption::TLS_VERSIONS`
    - `tls-ciphersuites=[...]` : see `SessionOption::TLS_CIPHERSUITES`
  */

  SessionSettings(const string &uri)
  {
    try {
      Settings_detail::set_from_uri(uri);
    }
    CATCH_AND_WRAP
  }


  /**
    Explicitly specify basic connection settings.

    @note Session settings constructed this way request an SSL connection
    by default.
  */

  SessionSettings(
    const std::string &host, unsigned port,
    const string  &user,
    const char *pwd = NULL,
    const string &db = string()
  )
  {
    set(
      SessionOption::HOST, host,
      SessionOption::PORT, port,
      SessionOption::USER, user
    );

    if (pwd)
      set(SessionOption::PWD, std::string(pwd));

    if (!db.empty())
      set(SessionOption::DB, db);
  }

  SessionSettings(
    const std::string &host, unsigned port,
    const string  &user,
    const std::string &pwd,
    const string &db = string()
  )
    : SessionSettings(host, port, user, pwd.c_str(), db)
  {}

  /**
    Basic settings with the default port

    @note Session settings constructed this way request an SSL connection
    by default.
  */

  SessionSettings(
    const std::string &host,
    const string  &user,
    const char    *pwd = NULL,
    const string  &db = string()
  )
    : SessionSettings(host, DEFAULT_MYSQLX_PORT, user, pwd, db)
  {}

  SessionSettings(
    const std::string &host,
    const string  &user,
    const std::string &pwd,
    const string  &db = string()
  )
    : SessionSettings(host, DEFAULT_MYSQLX_PORT, user, pwd, db)
  {}

  /**
    Basic settings for a session on the localhost.

    @note Session settings constructed this way request an SSL connection
    by default.
  */

  SessionSettings(
    unsigned port,
    const string  &user,
    const char    *pwd = NULL,
    const string  &db = string()
  )
    : SessionSettings("localhost", port, user, pwd, db)
  {}

  SessionSettings(
    unsigned port,
    const string  &user,
    const std::string &pwd,
    const string  &db = string()
  )
    : SessionSettings("localhost", port, user, pwd.c_str(), db)
  {}

  /*
    Templates below are here to take care of the optional password
    parameter of type const char* (which can be either 3-rd or 4-th in
    the parameter list). Without these templates passing
    NULL as password is ambiguous because NULL is defined as 0,
    which has type int, and then it could be treated as port value.
  */

  template <
    typename    HOST,
    typename    PORT,
    typename    USER,
    typename... T,
    typename std::enable_if<
      std::is_constructible<SessionSettings, HOST, PORT, USER, const char*, T...>::value
    >::type* = nullptr
  >
  SessionSettings(HOST h, PORT p, USER u, long, T... args)
    : SessionSettings(h, p, u, nullptr, args...)
  {}


  template <
    typename    PORT,
    typename    USER,
    typename... T,
    typename std::enable_if<
      std::is_constructible<SessionSettings, PORT, USER, const char*, T...>::value
    >::type* = nullptr
  >
  SessionSettings(PORT p, USER u, long, T... args)
    : SessionSettings(p, u, nullptr, args...)
  {}


  /**
    Specify settings as a list of session options.

    The list of options consist of a SessionOption constant,
    identifying the option to set, followed by the value of the option.

    Example:
    ~~~~~~
      SessionSettings from_option_list(
        SessionOption::USER, "user",
        SessionOption::PWD,  "pwd",
        SessionOption::HOST, "host",
        SessionOption::PORT, port,
        SessionOption::DB,   "db",
        SessionOption::SSL_MODE, SessionSettings::SSLMode::REQUIRED
      );
    ~~~~~~
  */

  template <typename... R>
  SessionSettings(SessionOption::Enum opt, R&&...rest)
  {
    try {
      // set<true> means that only SessionOption can be used
      Settings_detail::set<true>(opt, std::forward<R>(rest)...);
    }
    CATCH_AND_WRAP
  }

  /*
    Return an iterator pointing to the first element of the SessionSettings.
  */
  using iterator = internal::iterator<SessionOption, Settings_detail::iterator>;

  iterator begin()
  {
    try {
      return Settings_detail::begin();
    }
    CATCH_AND_WRAP
  }

  /*
    Return an iterator pointing to the last element of the SessionSettings.
  */

  iterator end()
  {
    try {
      return Settings_detail::end();
    }
    CATCH_AND_WRAP
  }


  /**
    Find the specified option @p opt and returns its Value.

    Returns NULL Value if not found.

    @note For option such as `HOST`, which can repeat several times in
    the settings, only the last value is reported.
  */

  Value find(SessionOption opt)
  {
    try {
      return Settings_detail::get(opt);
    }
    CATCH_AND_WRAP
  }

  /**
    Set session options.

    Accepts a list of one or more `SessionOption` constants, each followed by
    the option value. Options specified here are added to the current settings.

    Repeated `HOST`, `PORT`, `SOCKET` and `PRIORITY` options build a list of
    hosts to be used by the fail-over logic. For other options, if they are set
    again, the new value overrides the previous setting.

    @note
    When using `HOST`, `PORT` and `PRIORITY` options to specify a single
    host, all have to be specified in the same `set()` call.
   */

  template<typename... R>
  void set(SessionOption opt, R&&... rest)
  {
    try {
      // set<true> means that only SessionOption can be used
      Settings_detail::set<true>(opt, std::forward<R>(rest)...);
    }
    CATCH_AND_WRAP
  }

  /**
    Clear all settings specified so far.
  */

  void clear()
  {
    try {
      Settings_detail::clear();
    }
    CATCH_AND_WRAP
  }

  /**
    Remove all settings for the given option @p opt.

    @note For option such as `HOST`, which can repeat several times in
    the settings, all occurrences are erased.
  */

  void erase(SessionOption opt)
  {
    try {
      Settings_detail::erase(static_cast<int>(opt));
    }
    CATCH_AND_WRAP
  }


  /**
    Check if option @p opt was defined.
  */

  bool has_option(SessionOption opt)
  {
    try {
      return Settings_detail::has_option(opt);
    }
    CATCH_AND_WRAP
  }

private:

  friend Client;
  friend Session;
};


/**
  ClientSettings are used to construct Client objects.

  It can be constructed using a connection string plus a JSON with client
  options, or by setting each ClientOption and SessionOption with its
  correspondant value.

  @ingroup devapi
 */

class ClientSettings
  : private internal::Settings_detail<internal::Settings_traits>
{

public:

  using Base = internal::Settings_detail<internal::Settings_traits>;
  using Value = mysqlx::Value;

  /*
    Return an iterator pointing to the first element of the SessionSettings.
  */

  using iterator = internal::iterator<ClientOption, Settings_detail::iterator>;

  iterator begin()
  {
    try {
      return Settings_detail::begin();
    }
    CATCH_AND_WRAP
  }

  /*
    Return an iterator pointing to the last element of the SessionSettings.
  */

  iterator end()
  {
    try {
      return Settings_detail::end();
    }
    CATCH_AND_WRAP
  }

  /**
    Create client settings from a connection string.

    @see SessionSettings
  */

  ClientSettings(const string &uri)
  {
    try {
      Settings_detail::set_from_uri(uri);
    }
    CATCH_AND_WRAP
  }

  /**
    Create client settings from a connection string and a ClientSettings object

    @see SessionSettings
  */

  ClientSettings(const string &uri, ClientSettings &opts)
  {
    try {
      Settings_detail::set_from_uri(uri);
      Settings_detail::set_client_opts(opts);
    }
    CATCH_AND_WRAP
  }

  /**
    Create client settings from a connection string and. Client options are
    expressed in JSON format. Here is an example:
    ~~~~~~
    { "pooling": {
        "enabled": true,
        "maxSize": 25,
        "queueTimeout": 1000,
        "maxIdleTime": 5000}
    }
    ~~~~~~

    All options are defined under a document with key vale "pooling". Inside the
    document, the available options are these:
    - `enabled` : boolean value that enable or disable connection pooling. If
                  disabled, session created from pool are the same as created
                  directly without client handle.
                  Enabled by default.
    - `maxSize` : integer that defines the max pooling sessions possible. If
                  uses tries to get session from pool when maximum sessions are
                  used, it will wait for an available session untill
                  `queueTimeout`.
                  Defaults to 25.
    - `queueTimeout` : integer value that defines the time, in milliseconds,
                       that client will wait to get an available session.
                       By default it doesn't timeouts.
    - `maxIdleTime` : integer value that defines the time, in milliseconds, that
                     an available session will wait in the pool before it is
                     removed.
                     By default it doesn't cleans sessions.

  */

  ClientSettings(const string &uri, const DbDoc &options)
  {
    try {
      Settings_detail::set_from_uri(uri);
      std::stringstream str_opts;
      str_opts << options;
      Settings_detail::set_client_opts(str_opts.str());
    }
    CATCH_AND_WRAP
  }

  /**
    Create client settings from a connection string and. Client options are
    expressed in JSON format. Here is an example:
    ~~~~~~
    { "pooling": {
        "enabled": true,
        "maxSize": 25,
        "queueTimeout": 1000,
        "maxIdleTime": 5000}
    }
    ~~~~~~

    All options are defined under a document with key vale "pooling". Inside the
    document, the available options are these:
    - `enabled` : boolean value that enable or disable connection pooling. If
                  disabled, session created from pool are the same as created
                  directly without client handle.
                  Enabled by default.
    - `maxSize` : integer that defines the max pooling sessions possible. If
                  uses tries to get session from pool when maximum sessions are
                  used, it will wait for an available session untill
                  `queueTimeout`.
                  Defaults to 25.
    - `queueTimeout` : integer value that defines the time, in milliseconds,
                       that client will wait to get an available session.
                       By default it doesn't timeouts.
    - `maxIdleTime` : integer value that defines the time, in milliseconds, that
                     an available session will wait in the pool before it is
                     removed.
                     By default it doesn't cleans sessions.

  */

  ClientSettings(const string &uri, const char *options)
  {
    try {
      Settings_detail::set_from_uri(uri);
      Settings_detail::set_client_opts(options);
    }
    CATCH_AND_WRAP
  }

  /**
    Create client settings from a connection string and  client settings as a
    list of client options.

    The list of options consist of a ClientOption constant,
    identifying the option to set, followed by the value of the option.

    Example:
    ~~~~~~
    ClientSettings from_option_list( "mysqlx://root@localhost",
                   ClientOption::POOLING, true,
                   ClientOption::POOL_MAX_SIZE, max_connections,
                   ClientOption::POOL_QUEUE_TIMEOUT, std::chrono::seconds(100),
                   ClientOption::POOL_MAX_IDLE_TIME, std::chrono::microseconds(1)
      );
    ~~~~~~

    ClientOption::POOL_QUEUE_TIMEOUT and ClientOption::POOL_MAX_IDLE_TIME can
    be specified using std::chrono::duration objects, or by integer values, with
    the latest to be specified in milliseconds.

    @see SessionSettings
  */

  template<typename...R>
  ClientSettings(const string &uri, mysqlx::ClientOption opt, R... rest)
  try
    : ClientSettings(uri)
  {
    // set<false> means that both SessionOption and ClientOption can be used
    Settings_detail::set<false>(opt, std::forward<R>(rest)...);
  }
  CATCH_AND_WRAP


  template <typename... R>
  ClientSettings(mysqlx::ClientOption opt, R&&...rest)
  {
    try {
      // set<false> means that both SessionOption and ClientOption can be used
      Settings_detail::set<false>(opt, std::forward<R>(rest)...);
    }
    CATCH_AND_WRAP
  }

  /**
    Find the specified option @p opt and returns its Value.

    Returns NULL Value if not found.

  */

  Value find(mysqlx::ClientOption opt)
  {
    try {
      return Settings_detail::get(opt);
    }
    CATCH_AND_WRAP
  }

  /**
    Set client and session options.

    Accepts a list of one or more `ClientOption` or `SessionOption` constants,
    each followed by the option value. Options specified here are added to the
    current settings.

    Repeated `HOST`, `PORT`, `SOCKET` and `PRIORITY` options build a list of
    hosts to be used by the fail-over logic. For other options, if they are set
    again, the new value overrides the previous setting.

    @note
    When using `HOST`, `PORT` and `PRIORITY` options to specify a single
    host, all have to be specified in the same `set()` call.
   */

  template<typename OPT, typename... R>
  void set(OPT opt, R&&... rest)
  {
    try {
      // set<false> means that both SessionOption and ClientOption can be used
      Settings_detail::set<false>(opt, std::forward<R>(rest)...);
    }
    CATCH_AND_WRAP
  }


  /**
    Clear all settings specified so far.
  */

  void clear()
  {
    try {
      Settings_detail::clear();
    }
    CATCH_AND_WRAP
  }

  /**
    Remove the given option @p opt.
  */

  void erase(mysqlx::ClientOption opt)
  {
    try {
      Settings_detail::erase(static_cast<int>(opt));
    }
    CATCH_AND_WRAP
  }


  /**
    Check if option @p opt was defined.
  */

  bool has_option(ClientOption::Enum opt)
  {
    try {
      return Settings_detail::has_option(opt);
    }
    CATCH_AND_WRAP
  }

  /**
    Check if option @p opt was defined.
  */

  bool has_option(SessionOption::Enum opt)
  {
    try {
      return Settings_detail::has_option(opt);
    }
    CATCH_AND_WRAP
  }

private:
  friend Client;
  friend Session;
};


MYSQLX_ABI_END(2,0)
}  // mysqlx

#endif

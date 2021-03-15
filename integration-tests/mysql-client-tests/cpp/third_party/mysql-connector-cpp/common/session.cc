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

#include <mysql/cdk.h>
#include <uri_parser.h>
#include <json_parser.h>
#include <mysqlx/common.h>

PUSH_SYS_WARNINGS
#include <chrono>
#include <ratio>
#include <thread>
POP_SYS_WARNINGS

#include "settings.h"
#include "session.h"
#include "result.h"

#ifndef _WIN32
#include <sys/types.h>
#include <unistd.h>
#include <sys/utsname.h>
#endif

using namespace ::mysqlx::impl::common;
using TCPIP_options = cdk::ds::TCPIP::Options;
using TLS_options = TCPIP_options::TLS_options;
using lock_guard = std::lock_guard<std::recursive_mutex>;


void Settings_impl::clear()
{
  m_data = Data();
}


void Settings_impl::set_from_uri(const std::string &uri)
{
  parser::URI_parser parser(uri);
  Setter set(*this);

  parser.process(set);
  set.commit();
}

void Settings_impl::set_client_opts(const std::string &opts)
{
  parser::JSON_parser parser(opts);
  Setter set(*this);
  //Commit is done inside the document processing, that's why its not done here,
  //because it would clean all settings.
  parser.process(set);
}

void Settings_impl::set_client_opts(const Settings_impl &opts)
{
  Setter set(*this);
  set.set_client_opts(opts);
  set.commit();
}

/*
  Get information about OS and platform architecture.
  platform - an output parameter containig the string with the
  platform architecture (such as 'i386' or 'x86_64' etc)

  Returns the string containing the OS type and its version.
  Note: it returns the version, not the number in the name of the OS.
        In Windows it will be Windows-6.3.x instead of Windows-8.1
*/
std::string get_os_version_info(std::string &platform)
{
  std::stringstream ver_info;
#ifdef _WIN32
  HMODULE ntdll = GetModuleHandleA("ntdll.dll");
  typedef long (NTAPI *tRtlGetVersion)(OSVERSIONINFO*);
  tRtlGetVersion pRtlGetVersion = nullptr;
  OSVERSIONINFO ver;
  _SYSTEM_INFO hw_info;

  memset(&ver, 0, sizeof(OSVERSIONINFO));
  ver.dwOSVersionInfoSize = sizeof(sizeof(OSVERSIONINFO));

  if (ntdll != nullptr)
    pRtlGetVersion = (tRtlGetVersion)GetProcAddress(ntdll, "RtlGetVersion");

  if (pRtlGetVersion)
  {
    pRtlGetVersion(&ver);
  }
  else
  {
    PUSH_SYS_WARNINGS
      DISABLE_WARNING(4996)
      if (GetVersionEx(&ver) == 0)
        ver_info << "<unknown>";
    POP_SYS_WARNINGS
  }

  // Check if version info was set to <unknown> because of error
  if (ver_info.str().length() == 0)
    ver_info << "Windows-"
      << ver.dwMajorVersion << "."
      << ver.dwMinorVersion << "."
      << ver.dwBuildNumber;

  GetSystemInfo(&hw_info);
  switch (hw_info.wProcessorArchitecture)
  {
  case PROCESSOR_ARCHITECTURE_AMD64:
    platform = "x86_64"; break;
  case PROCESSOR_ARCHITECTURE_ARM:
    platform = "arm"; break;
  case PROCESSOR_ARCHITECTURE_IA64:
    platform = "ia64"; break;
  case PROCESSOR_ARCHITECTURE_INTEL:
    platform = "i386"; break;
  case PROCESSOR_ARCHITECTURE_IA32_ON_WIN64:
    platform = "i686"; break;
  case PROCESSOR_ARCHITECTURE_PPC:
    platform = "powerpc"; break;
  case PROCESSOR_ARCHITECTURE_MIPS:
    platform = "mips"; break;
  case PROCESSOR_ARCHITECTURE_ALPHA:
  case PROCESSOR_ARCHITECTURE_ALPHA64:
    platform = "alpha"; break;
  default:
    platform = "<unknown>";
  }

#else
  struct utsname ver;
  if (uname(&ver) == -1)
  {
    ver_info << "<unknown>";
    platform = "<unknown>";
  }
  else
  {
    ver_info << ver.sysname << "-" << ver.release;
    platform = ver.machine;
  }
#endif

  return ver_info.str();
}

void mysqlx::common::Settings_impl::Data::init_connection_attr()
{
  //Already initialized... nothing to do here!
  if (!m_connection_attr.empty())
    return;

  std::string pid;
  std::string platform;
#ifdef _WIN32
  pid = std::to_string(GetCurrentProcessId());
#else
  pid = std::to_string(getpid());
#endif
  m_connection_attr["_pid"] =  pid;

  m_connection_attr["_os"] = get_os_version_info(platform);
  m_connection_attr["_platform"] = platform;
  m_connection_attr["_source_host"] =
      cdk::foundation::connection::get_local_hostname();
  m_connection_attr["_client_name"] = CONCPP_NAME;
  m_connection_attr["_client_version"] = CONCPP_VERSION;
  m_connection_attr["_client_license"] = CONCPP_LICENSE;
}

void mysqlx::common::Settings_impl::Data::clear_connection_attr()
{
  m_connection_attr.clear();
}

TCPIP_options::auth_method_t get_auth(unsigned m)
{
  using DevAPI_type = Settings_impl::Auth_method;
  using CDK_type = TCPIP_options::auth_method_t;

  switch (DevAPI_type(m))
  {
#define AUTH_TO_CDK(X,N) \
  case DevAPI_type::X: return CDK_type::X;

    AUTH_METHOD_LIST(AUTH_TO_CDK)

  default:
    // Note: caller should ensure that argument has correct value
    assert(false);
  }

  return CDK_type(0); // quiet compiler warnings
}

TCPIP_options::compression_mode_t get_compression(unsigned m)
{
  using DevAPI_type = Settings_impl::Compression_mode;
  using CDK_type = TCPIP_options::compression_mode_t;

  switch (DevAPI_type(m))
  {
#define COMPRESSION_TO_CDK(X,N) \
  case DevAPI_type::X: return CDK_type::X;

    COMPRESSION_MODE_LIST(COMPRESSION_TO_CDK)

  default:
    // Note: caller should ensure that argument has correct value
    assert(false);
  }

  return CDK_type(0); // quiet compiler warnings
}

TLS_options::SSL_MODE get_ssl_mode(unsigned m)
{
  using DevAPI_type = Settings_impl::SSL_mode;
  using CDK_type = TLS_options::SSL_MODE;

  switch (DevAPI_type(m))
  {
#define SSL_TO_CDK(X,N) \
  case DevAPI_type::X: return CDK_type::X;

    SSL_MODE_LIST(SSL_TO_CDK)

  default:
    // Note: caller should ensure that argument has correct value
    assert(false);
  }

  return CDK_type(0); // quiet compiler warnings
}


/*
  Initialize CDK connection options based on session settings.
  If socket is true, we are preparing options for a connection
  over Unix domain socket (and then encryption is not required by default).
*/

void prepare_options(
  Settings_impl &settings, bool socket, TCPIP_options &opts
)
{
  using Option = Settings_impl::Session_option_impl;
  using SSL_mode = Settings_impl::SSL_mode;

  if (!settings.has_option(Option::USER))
    throw_error("USER option not defined");

  opts = TCPIP_options(
    string(settings.get(Option::USER).get_string()),
    settings.has_option(Option::PWD)
      ? &settings.get(Option::PWD).get_string() : nullptr
  );

  if (settings.has_option(Option::CONNECT_TIMEOUT))
    opts.set_connection_timeout(settings.get(Option::CONNECT_TIMEOUT).
           get_uint() * 1000); // millisec to microsec
  else
    opts.set_connection_timeout(DEFAULT_CN_TIMEOUT_US);

  // Set basic options

  if (settings.has_option(Option::DB))
    opts.set_database(settings.get(Option::DB).get_string());

  // Set TLS options

  /*
    By default ssl-mode is REQUIRED. If ssl-mode was not explicitly set but
    ssl-ca was, then mode defaults to VERIFY_CA.
  */

  unsigned mode = unsigned(SSL_mode::REQUIRED);
  bool mode_set = false;

  if (settings.has_option(Option::SSL_MODE))
  {
    mode_set = true;
    mode = (unsigned)settings.get(Option::SSL_MODE).get_uint();
  }
  else if (settings.has_option(Option::SSL_CA))
  {
    mode_set = true;
    mode = unsigned(SSL_mode::VERIFY_CA);
  }

  if (socket && mode_set && mode >= unsigned(SSL_mode::REQUIRED))
  {
    throw_error("SSL connection over Unix domain socket requested.");
  }

  if (unsigned(SSL_mode::DISABLED) == mode)
  {
#ifdef WITH_SSL
    opts.set_tls(TLS_options::SSL_MODE::DISABLED);
#endif
  }
  else
#ifdef WITH_SSL
  {
    socket = true;  // so that PLAIN auth method is used below

    TLS_options tls_opt(get_ssl_mode(mode));

    for (const auto &opt_val : settings)
    {
      switch (opt_val.first)
      {
      case Option::TLS_VERSIONS:
        tls_opt.add_version(opt_val.second.get_string());
        break;
      case Option::TLS_CIPHERSUITES:
        tls_opt.add_ciphersuite(opt_val.second.get_string());
        break;
      default:
        break;
      }
    }

    /*
      Note: CDK will not report errors below if no versions or no ciphers
      were specified, because in that case CDK uses default lists.
    */

    if (
      settings.has_option(Option::TLS_VERSIONS)
      && tls_opt.get_tls_versions().empty()
    )
      throw cdk::Error(cdk::cdkerrc::tls_versions);

    if (
      settings.has_option(Option::TLS_CIPHERSUITES)
      && tls_opt.get_ciphersuites().empty()
    )
      throw cdk::Error(cdk::cdkerrc::tls_ciphers);

    if (settings.has_option(Option::SSL_CA))
      tls_opt.set_ca(settings.get(Option::SSL_CA).get_string());
    opts.set_tls(tls_opt);
  }
#endif

  // Set Connection Attributes

  settings.get_attributes(opts);

  // Set authentication options

  if (settings.has_option(Option::AUTH))
    opts.set_auth_method(get_auth(
      (unsigned)settings.get(Option::AUTH).get_uint()
    ));
  else
  {
    opts.set_auth_method(
      socket ? TCPIP_options::PLAIN : TCPIP_options::DEFAULT
    );
  }

  if (settings.has_option(Option::COMPRESSION))
  {
    opts.set_compression(get_compression(
      (unsigned)settings.get(Option::COMPRESSION).get_uint()));
  }

  // DNS+SRV

  if(settings.has_option(Option::DNS_SRV))
  {
    opts.set_dns_srv(settings.get(Option::DNS_SRV).get_bool());
  }

}


/*
  Initialize CDK data source based on collected settings.
*/

void Settings_impl::get_data_source(cdk::ds::Multi_source &src)
{
  cdk::ds::TCPIP::Options opts;

  /*
    A single-host connection over Unix domain socket is considered secure.
    Otherwise SSL connection will be configured by default.
  */
  bool socket = m_data.m_sock && (1 == m_data.m_host_cnt);

  prepare_options(*this, socket, opts);

  // Build the list of hosts based on current settings.

  src.clear();

  if (has_option(Session_option_impl::DNS_SRV))
  {
    /*
      Use DNS+SRV data source.

      Note: option consistency checks are done by Setter
    */

    assert(1 == m_data.m_host_cnt);

    cdk::ds::DNS_SRV_source dns_srv_src(
      get(Session_option_impl::HOST).get_string(), opts
    );

    /*
      Note: this assignment performs DNS lookup to populate the server list
      in src. If no hosts are returned, method get() throws error.
    */

    src = dns_srv_src.get();

    assert(src.size() > 0);
    return;
  }

  /*
    If DNS+SRV is not used, get list of hosts from the settings.
  */

  // if priorities were not set explicitly, assign decreasing starting from 100
  int prio = m_data.m_user_priorities ? -1 : 100;

  /*
    Look for a priority after host/socket setting. If explicit priorities
    are used, then we expect the priority setting to be present and we throw
    error if this is not the case. Otherwise the given defalut priority is
    not changed and only sanity checks are done.
  */

  auto check_prio = [this](iterator &it, int &prio) {

    if (m_data.m_user_priorities)
    {
      if (it == end() || Session_option_impl::PRIORITY != it->first)
        throw_error("No priority specified for host ...");
      // note: value of PRIORITY option is checked for validity
      prio = (int)it->second.get_uint();
      ++it;
    }

    assert(0 <= prio && prio <= 100);

    /*
      Convert from decreasing priorities to increasing priorities used
      by cdk::Multi_source.
    */
    prio = 100 - prio;

    /*
      If there are more options, there should be no PRIORITY option
      at this point.
    */
    assert(it == end() || Session_option_impl::PRIORITY != it->first);
  };

  /*
    This lambda is called when current option is HOST or PORT, to add (next)
    TCPIP host with optional priority to the data source.
  */

  auto add_host = [this, &src, &opts, check_prio]
    (iterator &it, int prio) {

    string host("localhost");
    unsigned short  port = DEFAULT_MYSQLX_PORT;

    if (Session_option_impl::PORT == it->first)
    {
      assert(0 == m_data.m_host_cnt);
    }
    else
    {
      assert(Session_option_impl::HOST == it->first);
      host = it->second.get_string();
      ++it;
    }

    // Look for PORT

    if (it != end() && Session_option_impl::PORT == it->first)
    {
      port = (unsigned short)it->second.get_uint();
      ++it;
    }

    check_prio(it, prio);

#ifdef WITH_SSL

    /*
      Set expected CN if ssl mode is VERIFY_IDENTITY. We expect CN to be
      the host name given by user when creating the session.
    */

    if (TLS_options::SSL_MODE::VERIFY_IDENTITY == opts.get_tls().ssl_mode())
    {
      TLS_options tls = opts.get_tls();

      tls.set_host_name(host);
      opts.set_tls(tls);
    }

#endif

    src.add_prio(cdk::ds::TCPIP(host, port), opts, (unsigned short)prio);
  };

  /*
    This lambda is called when current option is SOCKET to add Unix socket
    source to the list.
  */

#ifdef _WIN32
  auto add_socket = [](iterator, int) {
    throw_error("Unix socket connections not supported on Windows platform.");
  };
#else
  auto add_socket = [this, &src, &opts, check_prio](iterator &it, int prio) {

    assert(Session_option_impl::SOCKET == it->first);

    string socket_path = it->second.get_string();
    ++it;

    check_prio(it, prio);

    src.add_prio(cdk::ds::Unix_socket(socket_path),
      (cdk::ds::Unix_socket::Options&)opts,
      (unsigned short)prio);

  };
#endif


  // default prioirty of 1 is used if priorities are not explicitly specified
  static const int default_prio = 1;

  /*
    Go through options and look for ones which define connections.
  */

  for (auto it = begin(); it != end();)
  {
    switch (it->first)
    {
    case Session_option_impl::HOST:
      add_host(it, default_prio); break;

    case Session_option_impl::SOCKET:
      add_socket(it, default_prio); break;

    /*
      Note: if m_host_cnt > 0 then a HOST setting must be before PORT setting,
      so the case above should cover that HOST/PORT pair.
    */
    case Session_option_impl::PORT:
      assert(0 == m_data.m_host_cnt);
      add_host(it, default_prio);
      break;

    default:
      ++it;
    }
  }

  if (0 == src.size())
  {
    throw_error("No sources to connect");
  }

}


void Settings_impl::get_attributes(cdk::ds::Attr_processor &prc)
{
  for(auto &el : m_data.m_connection_attr)
  {
    prc.attr(el.first, el.second);
  }
}


// ---------------------------------------------------------------------------


void Session_impl::prepare_for_cmd()
{
  if (m_current_result)
  {
    m_current_result->store_all_results();
  }

  m_current_result = nullptr;
}


void Session_impl::release()
{
  // Clear up pending results before returning session to the pool
  cleanup();
  m_sess.release();
}


// ---------------------------------------------------------------------------


Pooled_session::Pooled_session(
  Session_pool_shared &pool, Session_cleanup *cleanup
)
  : m_sess_pool(pool), m_cleanup(cleanup)
{
  m_deadline = system_clock::now() + m_sess_pool->m_timeout;
  cont();
}


Pooled_session::Pooled_session(cdk::ds::Multi_source &ds)
{
  reset(new cdk::Session(ds));
}


Pooled_session::~Pooled_session()
{
  release();
}

void Pooled_session::release()
{
  if (*this)
  {
    if (m_sess_pool)
      m_sess_pool->release_session(*this);
    else
      (*this)->close();
  }

  //Session pool is no longer needed
  m_sess_pool.reset();
}


bool Pooled_session::is_completed() const
{
  return  nullptr != get();
}


bool Pooled_session::do_cont()
{
  if (get())
    return true;

  assert(m_sess_pool);

  // If session pool disabled, create session
  std::shared_ptr<cdk::Session>::operator=(
    m_sess_pool->get_session(m_cleanup)
  );

  if (get())
    return true;

  //Otherwise, continue trying and check timeout

  if (m_deadline < system_clock::now())
      throw_error("Timeout reached when getting session from pool");

  return false;

}


void Pooled_session::do_wait()
{
  //If session is/gets closed, do_cont() will throw error
  while(!do_cont())
  {
    //waiting untill someone releases a session
    std::unique_lock<std::mutex> lock(m_sess_pool->m_reelase_mutex);

    //prevent changing m_pool_closed before getting release condition signal
    if (!m_sess_pool->m_pool_closed &&
        m_sess_pool->m_release_cond.wait_until(lock, m_deadline)
        == std::cv_status::timeout)
    {
      throw_error("Timeout reached when getting session from pool");
    }
  }
}


void Pooled_session::do_cancel()
{

}


const cdk::foundation::api::Event_info* Pooled_session::get_event_info() const
{
  return nullptr;
}


// ---------------------------------------------------------------------------


Session_pool::Session_pool(cdk::ds::Multi_source &ds)
  : m_ds(ds)
{}


Session_pool::~Session_pool()
try {
  close();
}
catch (...)
{}


void Session_pool::close()
{
  lock_guard guard(m_pool_mutex);
  //First, close all sessions
  for(auto &el : m_pool)
  {
    try{
      // If there is a cleanup handler, call it before closing a session.
      if (el.second.m_cleanup)
        el.second.m_cleanup->cleanup();
      el.first->close();
    } catch(...)
    {}
  }
  m_pool.clear();

  //prevent changing m_pool_closed before getting release condition signal
  std::unique_lock<std::mutex> lock(m_reelase_mutex);
  m_pool_closed = true;

  //Will notify all because, since pool is now closed, waiting pooled sessions
  //will throw error!
  m_release_cond.notify_all();
}


void Session_pool::release_session(cdk::shared_ptr<cdk::Session> &sess)
{
  // Pool closed... nothing to do here!
  if (m_pool_closed)
    return;
  {
    lock_guard guard(m_pool_mutex);

    auto el = m_pool.find(sess);

    if (el != m_pool.end())
    {
      el->second.m_deadline = system_clock::now() + m_time_to_live;

      // Note: we assume that session returned to the pool is no longer
      // in use and does not need a cleanup handler.
      el->second.m_cleanup = nullptr;
    }

    try {
      //Reset session so that internal is unique!
      sess.reset();
    }
    catch (...) {
      try {
        //remove session, since we got error
        m_pool.erase(el);
      } catch (...)
      {}
    }

    time_to_live_cleanup();
  }

  //inform a session was released
  m_release_cond.notify_one();
}


std::shared_ptr<cdk::Session>
Session_pool::get_session(Session_cleanup *cleanup)
{
  lock_guard guard(m_pool_mutex);

  if (!m_pool_enable)
  {
    return std::shared_ptr<cdk::Session>(new cdk::Session(m_ds));
  }

  if (m_pool_closed)
    throw_error("Pool was closed!");

  time_to_live_cleanup();

  for(auto it = m_pool.begin();
      it != m_pool.end();
      ++it)
  {
    // Not in use
    if (it->first.unique())
    {
      try {
        it->first->reset();
        if(!it->first->is_valid())
        {
          throw "Remove this";
        }

      } catch (...) {
        m_pool.erase(it);
        break;
      }
      it->second.m_cleanup = cleanup;
      return it->first;
    }
  }

  // Need new connection
  if (m_pool.size() < m_max)
  {
    auto ret = m_pool.emplace(
      cdk::shared_ptr<cdk::Session>(new cdk::Session(m_ds)),
      Sess_data{ time_point::max(), cleanup }
    );
    return ret.first->first;
  }
  return nullptr;
}


void Session_pool::time_to_live_cleanup()
{
  lock_guard guard(m_pool_mutex);

  time_point current_time = system_clock::now();

  auto it = m_pool.begin();
  for(; it != m_pool.end(); )
  {
    if (it->first.unique() && it->second.m_deadline < current_time)
    {
      // Note: removed session is not active and does not need calling
      // of the cleanup handler.
      it = m_pool.erase(it);
    }
    else
    {
      ++it;
    }
  }
}


void Session_pool::set_pool_opts(Settings_impl &opts)
{
  if (opts.has_option(Settings_impl::Client_option_impl::POOLING))
  try{
    set_pooling(opts.get(Settings_impl::Client_option_impl::POOLING).get_bool());
  }catch(...)
  {
    throw_error("Invalid POOLING value");
  }


  if (opts.has_option(Settings_impl::Client_option_impl::POOL_MAX_SIZE))
  try{
    set_size(
          static_cast<size_t>(
            opts.get(Settings_impl::Client_option_impl::POOL_MAX_SIZE)
            .get_uint()));
  }catch(...)
  {
    throw_error("Invalid POOL_MAX_SIZE value");
  }


  if (opts.has_option(Settings_impl::Client_option_impl::POOL_QUEUE_TIMEOUT))
  try{
    set_timeout(opts.get(Settings_impl::Client_option_impl::POOL_QUEUE_TIMEOUT).get_uint());
  }catch(...)
  {
    throw_error("Invalid POOL_QUEUE_TIMEOUT value");
  }


  if (opts.has_option(Settings_impl::Client_option_impl::POOL_MAX_IDLE_TIME))
  try{
    set_time_to_live(
          opts.get(Settings_impl::Client_option_impl::POOL_MAX_IDLE_TIME)
          .get_uint());
  }catch(...)
  {
    throw_error("Invalid POOL_MAX_IDLE_TIME value");
  }
}

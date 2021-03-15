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

#include <mysql/cdk/foundation/common.h>
//include socket_detail.h before ssl.h because it includes winsock2.h
//which must be included before winsock.h
#include "socket_detail.h"
PUSH_SYS_WARNINGS_CDK
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <openssl/err.h>
#include <iostream>
#include <map>
POP_SYS_WARNINGS_CDK
#include <mysql/cdk/foundation/error.h>
#include <mysql/cdk/foundation/connection_openssl.h>
#include <mysql/cdk/foundation/opaque_impl.i>
#include "connection_tcpip_base.h"

/*
  On Windows, external dependencies can be declared using
  #pragma comment directive.
*/

#ifdef _WIN32
  #pragma comment(lib,"ws2_32")
  #if defined(WITH_SSL)
    #if OPENSSL_VERSION_NUMBER < 0x10100000L
      #pragma comment(lib,"ssleay32")
      #pragma comment(lib,"libeay32")
    #else
      #pragma comment(lib,"libssl")
      #pragma comment(lib,"libcrypto")
    #endif
  #endif
#endif


/*
  Valid TLS versions with a mapping to OpenSSL version constant and
  major/minor version number.

  Note: Even if OpenSSL we are using does not support TLSv1.3, we still
  recognize it as a valid version and define TLS1_3_VERSION although this
  constant won't be used in that scenario.
*/

#ifndef TLS1_3_VERSION
#define TLS1_3_VERSION 0
#endif

// Note: this list must be in increasing order.

#define TLS_VERSIONS(X) \
  X("TLSv1",   TLS1_VERSION,   1,0) \
  X("TLSv1.1", TLS1_1_VERSION, 1,1) \
  X("TLSv1.2", TLS1_2_VERSION, 1,2) \
  X("TLSv1.3", TLS1_3_VERSION, 1,3) \


/*
  Default list of ciphers. By default we allow only ciphers that are approved
  by the OSSA page (the link below). Lists of mandatory and approved ciphers
  defined below should be kept in sync with requirements on this
  page.

  https://confluence.oraclecorp.com/confluence/display/GPS/Approved+Security+Technologies%3A+Standards+-+TLS+Ciphers+and+Versions
*/

#define TLS_CIPHERS_MANDATORY(X) \
  X("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",  "ECDHE-ECDSA-AES128-GCM-SHA256") \
  X("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",  "ECDHE-ECDSA-AES256-GCM-SHA384") \
  X("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",    "ECDHE-RSA-AES128-GCM-SHA256") \

/*
  Note: Empty OpenSSL name means TLSv1.3+ cipher suite which is handled
  differently from pre-TLSv1.3 suites that have OpenSSL specific names.
*/

#define TLS_CIPHERS_APPROVED1(X) \
  X("TLS_AES_128_GCM_SHA256", "") \
  X("TLS_AES_256_GCM_SHA384", "") \
  X("TLS_CHACHA20_POLY1305_SHA256", "") \
  X("TLS_AES_128_CCM_SHA256", "") \
  X("TLS_AES_128_CCM_8_SHA256", "") \
  X("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384", "ECDHE-RSA-AES256-GCM-SHA384") \
  X("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384", "ECDHE-ECDSA-AES256-SHA384") \
  X("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384", "ECDHE-RSA-AES256-SHA384") \
  X("TLS_DHE_RSA_WITH_AES_128_GCM_SHA256", "DHE-RSA-AES128-GCM-SHA256") \
  X("TLS_DHE_DSS_WITH_AES_128_GCM_SHA256", "DHE-DSS-AES128-GCM-SHA256") \
  X("TLS_DHE_RSA_WITH_AES_128_CBC_SHA256", "DHE-RSA-AES128-SHA256") \
  X("TLS_DHE_DSS_WITH_AES_128_CBC_SHA256", "DHE-DSS-AES128-SHA256") \
  X("TLS_DHE_DSS_WITH_AES_256_GCM_SHA384", "DHE-DSS-AES256-GCM-SHA384") \
  X("TLS_DHE_RSA_WITH_AES_256_GCM_SHA384", "DHE-RSA-AES256-GCM-SHA384") \
  X("TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256", "ECDHE-ECDSA-CHACHA20-POLY1305") \
  X("TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256", "ECDHE-RSA-CHACHA20-POLY1305") \


#define TLS_CIPHERS_APPROVED2(X) \
  X("TLS_DH_DSS_WITH_AES_128_GCM_SHA256", "DH-DSS-AES128-GCM-SHA256") \
  X("TLS_ECDH_ECDSA_WITH_AES_128_GCM_SHA256","ECDH-ECDSA-AES128-GCM-SHA256") \
  X("TLS_DH_DSS_WITH_AES_256_GCM_SHA384","DH-DSS-AES256-GCM-SHA384") \
  X("TLS_ECDH_ECDSA_WITH_AES_256_GCM_SHA384","ECDH-ECDSA-AES256-GCM-SHA384") \
  X("TLS_DH_RSA_WITH_AES_128_GCM_SHA256","DH-RSA-AES128-GCM-SHA256") \
  X("TLS_ECDH_RSA_WITH_AES_128_GCM_SHA256","ECDH-RSA-AES128-GCM-SHA256") \
  X("TLS_DH_RSA_WITH_AES_256_GCM_SHA384","DH-RSA-AES256-GCM-SHA384") \
  X("TLS_ECDH_RSA_WITH_AES_256_GCM_SHA384","ECDH-RSA-AES256-GCM-SHA384") \


// Note: these deprecated ciphers are temporarily allowed to make it possible
// to connect to old servers based on YaSSL.

#define TLS_CIPHERS_COMPAT(X) \
  X("TLS_DHE_RSA_WITH_AES_256_CBC_SHA", "DHE-RSA-AES256-SHA") \
  X("TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "DHE-RSA-AES128-SHA") \
  X("TLS_RSA_WITH_AES_256_CBC_SHA", "AES256-SHA")


#define TLS_CIPHERS_DEFAULT(X) \
  TLS_CIPHERS_MANDATORY(X) \
  TLS_CIPHERS_APPROVED1(X) \
  TLS_CIPHERS_APPROVED2(X) \
  TLS_CIPHERS_COMPAT(X) \



/*
  TLS_CIPHERS() list is used to translate IANA cipher names to OpenSSL ones.

  In principle CDK could/should support any cipher that is known to OpenSSL,
  but it is not so easy to find complete data for IANA -> OpenSSL translation.

  Since X DevAPI is restricting allowed ciphers to the ones used in the default
  list, the easy solution for now is to have translation only for these allowed
  ciphers (as other ciphers should trigger error anyway). If needed, CDK can
  be extended in the future to cover other ciphers known to OpenSSL.

  Note: some sources of data for IANA -> OpenSSL name translation:
  - https://confluence.oraclecorp.com/confluence/display/GPS/Approved+Security+Technologies%3A+Standards+-+TLS+Ciphers+and+Versions
  - https://www.openssl.org/docs/man1.1.1/man1/ciphers.html
  - https://testssl.sh/openssl-iana.mapping.html
  - https://ciphersuite.info/
*/

#define TLS_CIPHERS(X) \
  TLS_CIPHERS_DEFAULT(X)



using namespace cdk::foundation;

/*
  Handling SSL layer errors.
*/

static void throw_openssl_error_msg(const char* msg)
{
  throw cdk::foundation::Error(cdk::foundation::cdkerrc::tls_error,
                               std::string("OpenSSL: ")
                               + msg);
}

static void throw_openssl_error()
{
  char buffer[512];

  ERR_error_string_n(ERR_get_error(), buffer, sizeof(buffer));

  throw_openssl_error_msg(buffer);
}

/*
  Function should be called after SSL_read/SSL_write returns error (<=0).
  It will get ssl error and throw it if needed.
  Will return normally if the error can be continued.
*/
static void throw_ssl_error(SSL* tls, int err)
{
  switch(SSL_get_error(tls, err))
  {
  case SSL_ERROR_WANT_READ:
  case SSL_ERROR_WANT_WRITE:
#ifndef WITH_SSL_YASSL
  case SSL_ERROR_WANT_CONNECT:
  case SSL_ERROR_WANT_ACCEPT:
  case SSL_ERROR_WANT_X509_LOOKUP:
# if OPENSSL_VERSION_NUMBER >= 0x10100000L
  case SSL_ERROR_WANT_ASYNC:
  case SSL_ERROR_WANT_ASYNC_JOB:
# endif
#endif
    //Will not throw anything, so function that calls this, will continue.
    break;
  case SSL_ERROR_ZERO_RETURN:
    throw connection::Error_eos();
  case SSL_ERROR_SYSCALL:
    cdk::foundation::throw_posix_error();
  case SSL_ERROR_SSL:
    throw_openssl_error();
  default:
    {
      char buffer[512];
      ERR_error_string_n(static_cast<unsigned long>(SSL_get_error(tls, err)), buffer, sizeof(buffer));
      throw_openssl_error_msg(buffer);
    }
  }
}


/*
  TLS_version
*/


connection::TLS::Options::TLS_version::TLS_version(const std::string &ver)
{
#define TLS_VERSION_GET(V,N,X,Y) \
  if (ver == V) { m_major = X; m_minor = Y; return; }

  TLS_VERSIONS(TLS_VERSION_GET)
  throw Error(ver);
}


/*
  Implementation of TLS connection class.
*/


class connection_TLS_impl
  : public connection::Socket_base::Impl
{
public:

  /*
    Note: Once created, the TLS object takes ownership of the plain tcpip
    connection object (which is assumed to be dynamically allocated).
  */

  connection_TLS_impl(connection::Socket_base* tcpip,
                      connection::TLS::Options options)
    : m_tcpip(tcpip)
    , m_tls(NULL)
    , m_tls_ctx(NULL)
    , m_options(options)
  {}

  ~connection_TLS_impl()
  {
    if (m_tls)
    {
      SSL_shutdown(m_tls);
      SSL_free(m_tls);
    }

    if (m_tls_ctx)
      SSL_CTX_free(m_tls_ctx);

    delete m_tcpip;
  }

  void do_connect();

  void verify_server_cert();

  connection::Socket_base* m_tcpip;
  SSL* m_tls;
  SSL_CTX* m_tls_ctx;
  connection::TLS::Options m_options;
};


/*
  Helper class to configure allowed TLS versions and ciphers.
*/

struct TLS_helper
{
  using Versions_list = connection::TLS::Options::TLS_versions_list;
  using Ciphers_list =  connection::TLS::Options::TLS_ciphersuites_list;

  TLS_helper()
  {
    // sets default ciphers
    set_ciphers({
#define CIPHER_NAME(A,B) A,
      TLS_CIPHERS_DEFAULT(CIPHER_NAME)
    });
  }

  void setup(SSL_CTX*);
  void set_versions(const Versions_list&);
  void set_ciphers(const Ciphers_list&);

  int m_ver_min = TLS1_VERSION;
  int m_ver_max = 0;
  unsigned long m_ver_mask = SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3;

  std::string m_cipher_list;
  std::string m_cipher_list_13;

  static TLS_helper m_instance;
};

/*
  Note: This static instance is used to quickly get default settings without
  processing cipher lists each time. The default list of ciphers is stored
  in this static instance and will be used if user does not override defaults.
*/

TLS_helper TLS_helper::m_instance;


void TLS_helper::setup(SSL_CTX *ctx)
{
  // Configure allowed TLS versions

  SSL_CTX_clear_options(
    ctx,
    SSL_OP_NO_TLSv1 |
    SSL_OP_NO_TLSv1_1 |
    SSL_OP_NO_TLSv1_2
  );

#if OPENSSL_VERSION_NUMBER >= 0x10100000L

  if (m_ver_min)
  {
    if (1 != SSL_CTX_set_min_proto_version(ctx, m_ver_min))
      throw_openssl_error();
  }

  if (m_ver_max)
  {
    if (1 != SSL_CTX_set_max_proto_version(ctx, m_ver_max))
      throw_openssl_error();
  }

#endif

  long result_mask = SSL_CTX_set_options(ctx, m_ver_mask);

  if ((result_mask & m_ver_mask) == 0)
    throw_openssl_error();

  /*
    Configure allowed TLS ciphers. First check if we have any valid ciphers
    configured.
  */

  if (
    m_cipher_list.empty()
#if OPENSSL_VERSION_NUMBER>=0x1010100fL
    && m_cipher_list_13.empty()
#endif
  )
  {
    throw Error(cdkerrc::tls_ciphers);
  }

  SSL_CTX_set_cipher_list(ctx, m_cipher_list.c_str());

#if OPENSSL_VERSION_NUMBER>=0x1010100fL

  /*
    Note: If TLSv1.3 is not enabled, there is no need to restrict
    1.3 ciphers as they won't be used anyway. Also, it turns out
    that setting any 1.3 ciphers while TLSv1.3 is not disabled breaks
    connections that otherwise could down-grade to TLSv1.2. As if
    calling SSL_CTX_set_ciphersuites() in this situation would set
    minimum TLS version to TLSv1.3.

    Note: m_ver_max == 0 means that there is no limit.
  */

  if (!m_ver_max || m_ver_max > TLS1_2_VERSION)
  {
    SSL_CTX_set_ciphersuites(ctx, m_cipher_list_13.c_str());
  }

#endif

}


void TLS_helper::set_versions(const Versions_list &list)
{
  using TLS_version = connection::TLS::Options::TLS_version;
  bool no_versions = true;  // Note: used to check if any version was set

  m_ver_min = m_ver_max = 0;
  m_ver_mask = SSL_OP_NO_SSLv2 |
        SSL_OP_NO_SSLv3 |
        SSL_OP_NO_TLSv1 |
        SSL_OP_NO_TLSv1_1 |
        SSL_OP_NO_TLSv1_2;

  auto process_version = [&](const char *, int val, TLS_version ver)
  {
    if (0 == list.count(ver))
      return;

    // val is 0 for TLS versions that are valid but not supported by
    // OpenSSL. We skip them here.

    if (!val)
      return;

    no_versions = false;

    if (0 == m_ver_min)
      m_ver_min = val;
    m_ver_max = val;

    // Currently we only have TLSv1.x versions.
    assert(1 == ver.m_major);

    switch(ver.m_minor)
    {
    case 0:
      m_ver_mask &= ~SSL_OP_NO_TLSv1;
      break;
    case 1:
      m_ver_mask &= ~SSL_OP_NO_TLSv1_1;
      break;
    case 2:
      m_ver_mask &= ~SSL_OP_NO_TLSv1_2;
      break;
#if TLS1_3_VERSION
    case 3:
      // Note: Exclude mask works only up to version TLSv1.2 but exclustion
      // of TLSv1.3 (if requested) is handled by m_ver_max
      break;
#endif
    default:
      // We should not have any other versions, when they appear this code
      // needs to be modified.
      assert(false);
    }

  };

#define PROCESS_VERSION(V,N,X,Y) process_version(V,N,{X,Y});
  TLS_VERSIONS(PROCESS_VERSION)

  if (no_versions)
    throw Error(cdkerrc::tls_versions);

}


void TLS_helper::set_ciphers(const Ciphers_list &list)
{
  /*
    Note: This function is written so that only one iteration through
    the list is needed. We avoid iterating over all default ciphers,
    because in most cases the list of default ciphers will be much longer
    than the list of ciphers specified by the user.
  */

  m_cipher_list.clear();
  m_cipher_list_13.clear();

  auto add_cipher = [](std::string &list, const std::string &name)
  {
    if (!list.empty())
      list.append(":");
    list.append(name);
  };

  /*
    Mapping from IANA cipher names to OpenSSL names and priorities.
  */

  static std::map<std::string, std::pair<std::string, unsigned>> cipher_name_map =
  {
#define TLS_CIPHER_MAP0(A,B)  {A,{B,0}},
#define TLS_CIPHER_MAP1(A,B)  {A,{B,1}},
#define TLS_CIPHER_MAP2(A,B)  {A,{B,2}},
#define TLS_CIPHER_MAP3(A,B)  {A,{B,3}},

    TLS_CIPHERS_MANDATORY(TLS_CIPHER_MAP0)
    TLS_CIPHERS_APPROVED1(TLS_CIPHER_MAP1)
    TLS_CIPHERS_APPROVED2(TLS_CIPHER_MAP2)
    TLS_CIPHERS_COMPAT(TLS_CIPHER_MAP3)
  };

  /*
    For each priority, store a separate list of ciphers of that priority
    to later combine them in the correct priority order.
  */

  std::string cipher_list[4];

  for (const std::string &cipher : list)
  {
    try {
      const auto &name_prio = cipher_name_map.at(cipher);

      /*
        Empty name means that this is TLSv1.3+ cipher. Otherwise append the
        openssl name to the correct priority list.
      */

      if (name_prio.first.empty())
        add_cipher(m_cipher_list_13, cipher);
      else
        add_cipher(cipher_list[name_prio.second], name_prio.first);
    }
    catch (const std::out_of_range&)
    {
      /*
        We silently ignore unkown ciphers -- if no know cipher is configured,
        error will be thrown in setup().
      */
    }
  }

  // Build final list of ciphers taking priorities into account.

  m_cipher_list = cipher_list[0]
    + ":" + cipher_list[1]
    + ":" + cipher_list[2]
    + ":" + cipher_list[3];

}


void connection_TLS_impl::do_connect()
{
  if (m_tcpip->is_closed())
    m_tcpip->connect();

  if (m_tls || m_tls_ctx)
  {
    // TLS handshake already established, exit.
    return;
  }

  try
  {
    const SSL_METHOD* method = SSLv23_client_method();

    if (!method)
      throw_openssl_error();

    m_tls_ctx = SSL_CTX_new(method);
    if (!m_tls_ctx)
      throw_openssl_error();

    // Set allowed TLS protocol versions and ciphers

    {
      // Note: copy defaults from static instance
      TLS_helper helper(TLS_helper::m_instance);

      auto vlist = m_options.get_tls_versions();
      if (!vlist.empty())
        helper.set_versions(vlist);

      auto clist = m_options.get_ciphersuites();
      if (!clist.empty())
        helper.set_ciphers(clist);

      helper.setup(m_tls_ctx);
    }


    // Certificate data, if requested.

    if (
      m_options.ssl_mode()
      >=
      cdk::foundation::connection::TLS::Options::SSL_MODE::VERIFY_CA
    )
    {
      /*
        Warnings must be disabled because of a bug in Visual Studio 2017 compiler:
        https://developercommunity.visualstudio.com/content/problem/130244/c-warning-c5039-reported-for-nullptr-argument.html
      */
      SSL_CTX_set_verify(m_tls_ctx, SSL_VERIFY_PEER, nullptr);

      if (SSL_CTX_load_verify_locations(
            m_tls_ctx,
            m_options.get_ca().c_str(),
            m_options.get_ca_path().empty()
            ? NULL : m_options.get_ca_path().c_str()) == 0)
        throw_openssl_error();
    }
    else
    {
      SSL_CTX_set_verify(m_tls_ctx, SSL_VERIFY_NONE, nullptr);
    }

    // Establish TLS connection

    m_tls = SSL_new(m_tls_ctx);
    if (!m_tls)
      throw_openssl_error();

    unsigned int fd = m_tcpip->get_fd();

    cdk::foundation::connection::detail::set_nonblocking(fd, false);

    SSL_set_fd(m_tls, static_cast<int>(fd));

#ifdef HAVE_REQUIRED_X509_FUNCTIONS
    /*
      The new way of server certificate verification
      (OpenSSL version >= 1.0.2)
      sets the verification options before a connection is established
    */
    verify_server_cert();
#endif

    if(SSL_connect(m_tls) != 1)
      throw_openssl_error();

#ifndef HAVE_REQUIRED_X509_FUNCTIONS
    /*
      The old way of server certificate verification
      (OpenSSL version < 1.0.2)
      can be only done after a connection is established
    */
    verify_server_cert();
#endif


  }
  catch (...)
  {
    if (m_tls)
    {
      SSL_shutdown(m_tls);
      SSL_free(m_tls);
      m_tls = NULL;
    }

    if (m_tls_ctx)
    {
      SSL_CTX_free(m_tls_ctx);
      m_tls_ctx = NULL;
    }

    throw;
  }
}


/*
  Class used to safely delete allocated X509 objects.
  This way, no need to test cert on each possible return/throw.
*/
template <typename X>
class safe_X509
{
  X* m_X509;

public:
  safe_X509(X *obj = NULL)
    : m_X509(obj)
  {}

  ~safe_X509()
  {
    if (std::is_same<X, X509>::value)
    {
      X509_free((X509*)m_X509);
    }
    else if (std::is_same<X, X509_VERIFY_PARAM>::value)
    {
      // for X509_VERIFY_PARAM* it must not be freed by a caller
      // X509_VERIFY_PARAM_free((X509_VERIFY_PARAM*)m_X509);
    }
  }

  operator bool()
  {
    return m_X509 != NULL;
  }

  operator X*() const
  {
    return m_X509;
  }
};

const unsigned char * get_cn(ASN1_STRING *cn_asn1)
{
  const unsigned char *cn = NULL;
#if OPENSSL_VERSION_NUMBER > 0x10100000L
  cn = ASN1_STRING_get0_data(cn_asn1);
#else
  cn = (const unsigned char*)(ASN1_STRING_data(cn_asn1));
#endif

  // There should not be any NULL embedded in the CN
  if ((size_t)ASN1_STRING_length(cn_asn1) != strlen(reinterpret_cast<const char*>(cn)))
    return NULL;

  return cn;
}

bool matches_common_name(const std::string &host_name, const X509 *server_cert)
{
  const unsigned char *cn = NULL;
  int cn_loc = -1;
  ASN1_STRING *cn_asn1 = NULL;
  X509_NAME_ENTRY *cn_entry = NULL;
  X509_NAME *subject = NULL;

  subject = X509_get_subject_name((X509 *)server_cert);
  // Find the CN location in the subject
  cn_loc = X509_NAME_get_index_by_NID(subject, NID_commonName, -1);

  if (cn_loc < 0)
  {
    throw_openssl_error_msg("SSL certificate validation failure");
  }

  // Get the CN entry for given location
  cn_entry = X509_NAME_get_entry(subject, cn_loc);
  if (cn_entry == NULL)
  {
    throw_openssl_error_msg("Failed to get CN entry using CN location");
  }

  // Get CN from common name entry
  cn_asn1 = X509_NAME_ENTRY_get_data(cn_entry);
  if (cn_asn1 == NULL)
  {
    throw_openssl_error_msg("Failed to get CN from CN entry");
  }

  cn = get_cn(cn_asn1);
  // There should not be any NULL embedded in the CN
  if (cn == NULL)
  {
    throw_openssl_error_msg("NULL embedded in the certificate CN");
  }

  std::string s_cn = reinterpret_cast<const char*>(cn);
  if (host_name == s_cn)
  {
    return true;
  }

  return false;
}


bool matches_alt_name(const std::string &host_name, const X509 *server_cert)
{
  int i, alt_names_num;
  STACK_OF(GENERAL_NAME) *alt_names;
  bool result = false;

  // Extract names from Subject Alternative Name extension (SAN)
  alt_names = (STACK_OF(GENERAL_NAME)*)
                X509_get_ext_d2i((X509*)server_cert,
                                 NID_subject_alt_name,
                                 NULL, NULL);
  if (alt_names == NULL)
    return false;  // No SAN is present

  alt_names_num = sk_GENERAL_NAME_num(alt_names);
  for (i = 0; i < alt_names_num; ++i)
  {
    GENERAL_NAME *gen_name = sk_GENERAL_NAME_value(alt_names, i);
    if (gen_name->type == GEN_DNS)
    {
      const unsigned char* dns_name;

      dns_name = get_cn(gen_name->d.dNSName);

      // There should not be any NULL embedded in the CN
      if (dns_name == NULL)
      {
        result = false; // Exit the loop, wrong length
        break;
      }

      std::string s_dns_name = reinterpret_cast<const char*>(dns_name);
      if (host_name == s_dns_name)
      {
        result = true;
        break;
      }
    }
  }

  sk_GENERAL_NAME_pop_free(alt_names, GENERAL_NAME_free);
  return result;
}


void connection_TLS_impl::verify_server_cert()
{
  if (cdk::foundation::connection::TLS::Options::SSL_MODE::VERIFY_IDENTITY ==
      m_options.ssl_mode())
  {

#ifdef HAVE_REQUIRED_X509_FUNCTIONS
    safe_X509<X509_VERIFY_PARAM> safe_param(SSL_get0_param(m_tls));

    X509_VERIFY_PARAM_set_hostflags(safe_param, X509_CHECK_FLAG_NO_WILDCARDS);

    if (X509_VERIFY_PARAM_set1_host(safe_param, m_options.get_host_name().c_str(),
                                     m_options.get_host_name().length()) != 1)
    {
      throw_openssl_error_msg("Could not verify the server certificate");
    }
    SSL_set_verify(m_tls, SSL_VERIFY_PEER, NULL);
#else
    safe_X509<X509> server_cert(SSL_get_peer_certificate(m_tls));

    if (!server_cert)
    {
      throw_openssl_error_msg("Could not get server certificate");
    }

    if (X509_V_OK != SSL_get_verify_result(m_tls))
    {
      throw_openssl_error_msg("Failed to verify the server certificate");
    }

    if (!matches_alt_name(m_options.get_host_name(), server_cert) &&
        !matches_common_name(m_options.get_host_name(), server_cert))
    {
      throw_openssl_error_msg("Could not verify the server certificate");
    }
#endif
  }
}


IMPL_TYPE(cdk::foundation::connection::TLS, connection_TLS_impl);
IMPL_PLAIN(cdk::foundation::connection::TLS);


namespace cdk {
namespace foundation {
namespace connection {


TLS::TLS(Socket_base* tcpip,
         const TLS::Options &options)
  : opaque_impl<TLS>(NULL, tcpip, options)
{}


Socket_base::Impl& TLS::get_base_impl()
{
  return get_impl();
}


TLS::Read_op::Read_op(TLS &conn, const buffers &bufs, time_t deadline)
  : IO_op(conn, bufs, deadline)
  , m_tls(conn)
  , m_currentBufferIdx(0)
  , m_currentBufferOffset(0)
{
  connection_TLS_impl& impl = m_tls.get_impl();

  if (!impl.m_tcpip->get_base_impl().is_open())
    throw Error_eos();
}


bool TLS::Read_op::do_cont()
{
  return common_read();
}


void TLS::Read_op::do_wait()
{
  while (!is_completed())
    common_read();
}


bool TLS::Read_op::common_read()
{
  if (is_completed())
    return true;

  connection_TLS_impl& impl = m_tls.get_impl();

  const bytes& buffer = m_bufs.get_buffer(m_currentBufferIdx);
  byte* data =buffer.begin() + m_currentBufferOffset;
  int buffer_size = static_cast<int>(buffer.size() - m_currentBufferOffset);

  int result = SSL_read(impl.m_tls, data, buffer_size);

  if (result <= 0)
  {
    throw_ssl_error(impl.m_tls, result);
  }

  if (result > 0)
  {
    m_currentBufferOffset += result;

    if (m_currentBufferOffset == buffer.size())
    {
      ++m_currentBufferIdx;

      if (m_currentBufferIdx == m_bufs.buf_count())
      {
        set_completed(m_bufs.length());
        return true;
      }
    }
  }

  return false;
}


TLS::Read_some_op::Read_some_op(TLS &conn, const buffers &bufs, time_t deadline)
  : IO_op(conn, bufs, deadline)
  , m_tls(conn)
{
  connection_TLS_impl& impl = m_tls.get_impl();

  if (!impl.m_tcpip->get_base_impl().is_open())
    throw Error_eos();
}


bool TLS::Read_some_op::do_cont()
{
  return common_read();
}


void TLS::Read_some_op::do_wait()
{
  while (!is_completed())
    common_read();
}


bool TLS::Read_some_op::common_read()
{
  if (is_completed())
    return true;

  connection_TLS_impl& impl = m_tls.get_impl();

  const bytes& buffer = m_bufs.get_buffer(0);

  int result = SSL_read(impl.m_tls, buffer.begin(), (int)buffer.size());

  if (result <= 0)
  {
    throw_ssl_error(impl.m_tls, result);
  }

  if (result > 0)
  {
    set_completed(static_cast<size_t>(result));
    return true;
  }

  return false;
}


TLS::Write_op::Write_op(TLS &conn, const buffers &bufs, time_t deadline)
  : IO_op(conn, bufs, deadline)
  , m_tls(conn)
  , m_currentBufferIdx(0)
  , m_currentBufferOffset(0)
{
  connection_TLS_impl& impl = m_tls.get_impl();

  if (!impl.m_tcpip->get_base_impl().is_open())
    throw Error_no_connection();
}


bool TLS::Write_op::do_cont()
{
  return common_write();
}


void TLS::Write_op::do_wait()
{
  while (!is_completed())
    common_write();
}


bool TLS::Write_op::common_write()
{
  if (is_completed())
    return true;

  connection_TLS_impl& impl = m_tls.get_impl();

  const bytes& buffer = m_bufs.get_buffer(m_currentBufferIdx);
  byte* data = buffer.begin() + m_currentBufferOffset;
  int buffer_size = static_cast<int>(buffer.size() - m_currentBufferOffset);

  int result = SSL_write(impl.m_tls, data, buffer_size);

  if (result <= 0)
  {
    throw_ssl_error(impl.m_tls, result);
  }

  if (result > 0)
  {
    m_currentBufferOffset += result;

    if (m_currentBufferOffset == buffer.size())
    {
      ++m_currentBufferIdx;

      if (m_currentBufferIdx == m_bufs.buf_count())
      {
        set_completed(m_bufs.length());
        return true;
      }
    }
  }

  return false;
}


TLS::Write_some_op::Write_some_op(TLS &conn, const buffers &bufs, time_t deadline)
  : IO_op(conn, bufs, deadline)
  , m_tls(conn)
{
  connection_TLS_impl& impl = m_tls.get_impl();

  if (!impl.m_tcpip->get_base_impl().is_open())
    throw Error_no_connection();
}


bool TLS::Write_some_op::do_cont()
{
  return common_write();
}


void TLS::Write_some_op::do_wait()
{
  while (!is_completed())
    common_write();
}


bool TLS::Write_some_op::common_write()
{
  if (is_completed())
    return true;

  connection_TLS_impl& impl = m_tls.get_impl();

  const bytes& buffer = m_bufs.get_buffer(0);

  int result = SSL_write(impl.m_tls, buffer.begin(), (int)buffer.size());

  if (result <= 0)
  {
    throw_ssl_error(impl.m_tls, result);
  }

  if (result > 0)
  {
    set_completed(static_cast<size_t>(result));
    return true;
  }

  return false;
}


}  // namespace connection
}  // namespace foundation
}  // namespace cdk

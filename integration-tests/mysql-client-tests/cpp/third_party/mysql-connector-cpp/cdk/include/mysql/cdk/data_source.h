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

#ifndef CDK_DATA_SOURCE_H
#define CDK_DATA_SOURCE_H

#include <mysql/cdk/foundation.h>

PUSH_SYS_WARNINGS_CDK
#include <functional>
#include <algorithm>
#include <set>
#include <random>
#include "api/expression.h"
POP_SYS_WARNINGS_CDK


namespace cdk {

// Data source

namespace ds {


struct Attr_processor
{
  virtual ~Attr_processor() {}
  virtual void attr(const string &key, const string &val)=0;
};

class Session_attributes
  : public cdk::api::Expr_base<Attr_processor>
{};


/*
 * Generic session options which are valid for any data source.
 */



template <class Base>
class Options
    : public Base
    , public Session_attributes
    , public Attr_processor
{
public:

  Options()
    : m_usr("root"), m_has_pwd(false), m_has_db(false)
  {
  }

  Options(const string &usr, const std::string *pwd =NULL)
    : m_usr(usr), m_has_pwd(false), m_has_db(false)
  {
    if (pwd)
    {
      m_has_pwd = true;
      m_pwd= *pwd;
    }
  }

  virtual ~Options() {}

  virtual const string& user() const { return m_usr; }
  virtual const std::string* password() const
  { return m_has_pwd ? &m_pwd : NULL; }


  virtual const string* database() const
  {
    return m_has_db ? &m_db : NULL;
  }

  void set_database(const string &db)
  {
    m_db = db;
    m_has_db = true;
  }

  void set_attributes(std::map<std::string,std::string> &connection_attr)
  {
    m_connection_attr = connection_attr;
  }

  const Session_attributes* attributes() const
  {
    if (m_connection_attr.empty())
      return nullptr;
    return this;
  }

  void process(Processor &prc) const override
  {
    for (auto &el :  m_connection_attr)
    {
      prc.attr(el.first, el.second);
    }
  }

  void attr(const string &key, const string &val) override
  {
    m_connection_attr[key]=val;
  }

protected:

  string m_usr;
  bool   m_has_pwd;
  std::string m_pwd;

  bool    m_has_db;
  string  m_db;
  std::map<std::string,std::string> m_connection_attr;

};


namespace mysqlx {

/*
 * A TCPIP data source represents a MySQL server accessible via TCP/IP
 * connection using the X Protocol.
 */

class TCPIP
{
protected:
  unsigned short m_port;
  std::string m_host;

public:

  class Options;


  TCPIP(const std::string &_host="localhost", unsigned short _port =33060)
  : m_port(_port), m_host(_host)
  {
    if (_host.empty() || 0 == _host.length())
      throw_error("invalid empty host name");
  }

  virtual ~TCPIP() {}

  virtual unsigned short port() const { return m_port; }
  virtual const std::string& host() const { return m_host; }

};


class Protocol_options
{

  public:

  enum auth_method_t {
    DEFAULT,
    PLAIN,
    MYSQL41,
    EXTERNAL,
    SHA256_MEMORY
  };

  enum compression_mode_t {
    DISABLED,
    PREFERRED,
    REQUIRED
  };

  virtual auth_method_t auth_method() const = 0;
  virtual compression_mode_t compression() const = 0;

};


class Options
  : public ds::Options<Protocol_options>,
    public foundation::connection::Socket_base::Options
{
protected:

  auth_method_t m_auth_method = DEFAULT;
  compression_mode_t m_compression = DISABLED;

public:

  Options()
  {}

  Options(const string &usr, const std::string *pwd =NULL)
    : ds::Options<Protocol_options>(usr, pwd)
  {}

  void set_auth_method(auth_method_t auth_method)
  {
    m_auth_method = auth_method;
  }

  auth_method_t auth_method() const
  {
    return m_auth_method;
  }

  void set_compression(compression_mode_t val)
  {
    m_compression = val;
  }

  compression_mode_t compression() const
  {
    return m_compression;
  }

};


class TCPIP::Options
  : public ds::mysqlx::Options
{
public:

  typedef cdk::connection::TLS::Options  TLS_options;

private:

#ifdef WITH_SSL
  cdk::connection::TLS::Options m_tls_options;
#endif

  bool m_dns_srv = false;

public:

  Options()
  {}

  Options(const string &usr, const std::string *pwd =NULL)
    : ds::mysqlx::Options(usr, pwd)
  {}

#ifdef WITH_SSL

  void set_tls(const TLS_options& options)
  {
    m_tls_options = options;
  }

  const TLS_options& get_tls() const
  {
    return m_tls_options;
  }

  bool get_dns_srv() const
  {
    return m_dns_srv;
  }

  void set_dns_srv(bool dns_srv)
  {
    m_dns_srv = dns_srv;
  }

#endif

};


#ifndef _WIN32
class Unix_socket
{
protected:
  std::string m_path;

public:

  class Options;

  Unix_socket(const std::string &path)
    : m_path(path)
  {
    if (path.empty() || 0 == path.length())
      throw_error("invalid empty socket path");
  }

  virtual ~Unix_socket() {}

  virtual const std::string& path() const { return m_path; }
};

class Unix_socket::Options
  : public ds::mysqlx::Options
{
  public:

  Options()
  {}

  Options(const string &usr, const std::string *pwd = NULL)
    : ds::mysqlx::Options(usr, pwd)
  {}

};
#endif //_WIN32

} // mysqlx


namespace mysql {

/*
 * Future Session with MYSQL over legacy protocol.
 */

class Protocol_options
{};

class TCPIP : public cdk::ds::mysqlx::TCPIP
{
public:

  TCPIP(const std::string &_host="localhost", unsigned short _port =3306)
  : cdk::ds::mysqlx::TCPIP(_host, _port)
  {}

  virtual ~TCPIP() {}

  typedef ds::Options<Protocol_options> Options;
};

} //mysql

}  // ds


//TCPIP defaults to mysqlx::TCPIP
namespace ds {

  typedef mysqlx::TCPIP TCPIP;
#ifndef _WIN32
  typedef mysqlx::Unix_socket Unix_socket;
#endif //_WIN32
  typedef mysql::TCPIP TCPIP_old;


  template <typename DS_t, typename DS_opt>
  struct DS_pair : public std::pair<DS_t, DS_opt>
  {
    DS_pair(const DS_pair&) = default;
#ifdef HAVE_MOVE_CTORS
    DS_pair(DS_pair&&) = default;
#endif
    DS_pair(const DS_t &ds, const DS_opt &opt) : std::pair<DS_t, DS_opt>(ds, opt)
    {}
  };


  /*
    A data source which encapsulates several other data sources (all of which
    are assumed to hold the same data).

    When adding data sources to a multi source, a priority can be specified.
    When a visitor is visiting the multi source, the data sources it contains
    are presented to the visitor in increasing priority order. If several data
    sources have the same priority, they are presented in random order. If
    no priorities were specified, then data sources are presented in the order
    in which they were added.

    If priorities are specified, they must be specified for all data sources
    that are added to the multi source.
  */

  class Multi_source
  {

  private:

    typedef cdk::foundation::variant <
      DS_pair<cdk::ds::TCPIP, cdk::ds::TCPIP::Options>
#ifndef _WIN32
      ,DS_pair<cdk::ds::Unix_socket, cdk::ds::Unix_socket::Options>
#endif //_WIN32
      ,DS_pair<cdk::ds::TCPIP_old, cdk::ds::TCPIP_old::Options>
    >
    DS_variant;

    bool m_is_prioritized = false;
    unsigned short m_counter = 0;

    struct Prio
    {
      unsigned short prio;
      uint16_t weight;
      operator unsigned short() const
      {
        return prio;
      }

      bool operator < (const Prio &other) const
      {
        return prio < other.prio;
      }
    };

    typedef std::multimap<Prio, DS_variant, std::less<Prio>> DS_list;
    DS_list m_ds_list;
    uint32_t m_total_weight = 0;

  public:

    // Add data source without explicit priority.

    template <class DS_t, class DS_opt>
    void add(const DS_t& ds, const DS_opt& opt, uint16_t weight = 1)
    {
      if (m_is_prioritized)
      {
        throw_error(
          "Adding un-prioritized items to prioritized list is not allowed"
        );
      }

      m_ds_list.emplace(Prio{ m_counter++, weight }, DS_pair<DS_t, DS_opt>{ ds, opt });
    }

    // Add data source with priority.

    template <class DS_t, class DS_opt>
    void add_prio(const DS_t &ds, const DS_opt &opt, unsigned short prio, uint16_t weight = 1)
    {
      if (m_ds_list.size() == 0)
        m_is_prioritized = true;

      if (!m_is_prioritized)
      {
        throw_error(
          "Adding prioritized items to un-prioritized list is not allowed"
        );
      }

      m_ds_list.emplace(Prio{ prio, weight }, DS_pair<DS_t, DS_opt>{ ds, opt });
    }

  private:

    template <typename Visitor>
    struct Variant_visitor
    {
      Visitor *vis = nullptr;
      bool stop_processing = false;

      template <class DS_t, class DS_opt>
      void operator () (const DS_pair<DS_t, DS_opt> &ds_pair)
      {
        assert(vis);
        stop_processing = (bool)(*vis)(ds_pair.first, ds_pair.second);
      }
    };

  public:

    /*
      Call visitor(ds,opts) for each data source ds with options
      opts in the list. Do it in decreasing priority order, choosing
      randomly among data sources with the same priority.
      If visitor(...) call returns true, stop the process.
    */

    template <class Visitor>
    void visit(Visitor &visitor)
    {
      Variant_visitor<Visitor> variant_visitor;
      variant_visitor.vis = &visitor;

      std::random_device rnd;
      bool stop_processing = false;
      std::vector<uint16_t> weights;
      std::set<DS_variant*> same_prio;

      for (auto it = m_ds_list.begin(); !stop_processing;)
      {
        if (it == m_ds_list.end())
          break;

        assert(same_prio.empty());

        {
          //  Get items with the same priority and store them in same_prio set

          auto same_range = m_ds_list.equal_range(it->first);
          it = same_range.second;  // move it to the first element after the range
          unsigned total_weight = 0;

          for (auto it1 = same_range.first; it1 != same_range.second; ++it1)
          {
            same_prio.insert(&(it1->second));
            weights.push_back(it1->first.weight);
            total_weight += it1->first.weight;
          }

          /*
            If all weights are 0 then all servers should be picked with the
            same probability. Set the weights to 1 because discrete_distribiton<>
            does not work when all weights are 0.
          */

          if (0 == total_weight)
          {
            for (auto& w : weights)
              w = 1;
          }
        }

        for (size_t size = same_prio.size(); size > 0; size = same_prio.size())
        {
          auto el = same_prio.begin();
          size_t pos = 0;

          if (size > 1)
          {
            /*
              Note: std::discrete_distribution will never pick hosts that have
              0 weight. But according to the DNS+SRV RFC [*], there should be
              a small probablity that they are picked. For now we leave it
              as is, as this is a corner case (normally weights should be > 0).
              We might consider improving the implementation later.

              Also note that we separately handle the case of all hosts having
              0 weight - in this case we pick them randomly with equal
              probability, as expected.

              [*] https://tools.ietf.org/html/rfc2782
            */

            std::discrete_distribution<int> distr(
              weights.begin(), weights.end()
            );
            pos = distr(rnd);
            std::advance(el, pos);
          }

          (*el)->visit(variant_visitor);
          stop_processing = variant_visitor.stop_processing;

          if (stop_processing)
            break;

          same_prio.erase(el);
          weights.erase(weights.begin() + pos);
        }

      } // for m_ds_llist
    }

    void clear()
    {
      m_ds_list.clear();
      m_is_prioritized = false;
      m_total_weight = 0;
    }

    size_t size()
    {
      return m_ds_list.size();
    }

    struct Access;
    friend Access;
  };


  /*
    A data source which takes data from one of the hosts obtained from
    a DNS+SRV query.

    Method get() issues a DNS+SRV query and returns its result as
    a Multi_source which contains a list of TCPIP data sources for the
    hosts returned from the query. Also the weights and priorites
    obtained from the DNS+SRV query are used.

    Example usage:

      DNS_SRV_source dns_srv(name, opts);
      Multi_source   src = dns_srv.get();

    Note: Each call to get() issues new DNS query and can result in
    different list of sources.
  */

  class DNS_SRV_source
  {
  public:

    using Options = TCPIP::Options;

    /*
      Create DNS+SRV data source for the given DNS name and session options.

      The DNS name is used to query DNS server when getting list of hosts.
      Given session options are used for each host obtained from the DNS+SRV
      query.
    */

    DNS_SRV_source(const std::string& host, const Options &opts)
      : m_host(host), m_opts(opts)
    {}

    /*
      Query DNS and return results as a Multi_source.
    */

    Multi_source get()
    {
      Multi_source src;

      auto list = cdk::foundation::connection::srv_list(m_host);

      if (list.empty())
      {
        std::string err = "Unable to locate any hosts for " + m_host;
        throw_error(err.c_str());
      }

      for (auto& el : list)
      {
        Options opt1(m_opts);
        Options::TLS_options tls(m_opts.get_tls());
        tls.set_host_name(el.name);
        opt1.set_tls(tls);
        src.add_prio(ds::TCPIP(el.name, el.port), opt1, el.prio, el.weight);
      }

      return src;
    }

  protected:

    std::string m_host;
    Options     m_opts;

  };

}


} // cdk

#endif // CDK_DATA_SOURCE_H

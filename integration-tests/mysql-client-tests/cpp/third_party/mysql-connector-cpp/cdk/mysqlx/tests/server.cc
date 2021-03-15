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

#include <mysql/cdk.h>
#include <mysql/cdk/protocol/mysqlx_protocol.h>
#include "socket.h"
#include <iostream>


using namespace std;
using namespace cdk::foundation;
using namespace cdk::protocol::mysqlx;


class Session
  : public Init_processor
  , public Cmd_processor
{
public:

  Session(cdk::api::Connection &conn)
    : m_proto(conn), m_closed(false)
  {
    cout <<"Waiting for initial message ..." <<endl;
    m_proto.rcv_initMessage(*this).wait();
    cout <<"Authentication using method: " <<m_auth <<endl;

    // bad auth test
    if (m_auth == "plain" &&
        m_user == "bad_user" &&
        m_pass == "bad_password")
    {
      byte welcome[] = "Invalid User or password!";
      m_proto.snd_authenticateFail(bytes(welcome, sizeof(welcome)-1)).wait();
    }
    else
    {
      byte welcome[] = "Welcome!";
      m_proto.snd_authenticateOK(bytes(welcome, sizeof(welcome)-1)).wait();
    }
  }

  ~Session()
  {
    abort();
  }

  void process_requests();
  void abort() {}

private:

  Protocol_server m_proto;
  string m_auth;
  string m_user;
  string m_pass;
  bool   m_closed;

  void authenticateStart(const char *mech, bytes data, bytes response)
  {
    m_auth= mech;
    m_user= string(data.begin(),data.end());
    m_pass= string(response.begin(),response.end());
  }

  void authenticateContinue(bytes data)
  {}

  void close()
  {
    cout <<"Client closed connection" <<endl;
    m_closed= true;
  }

  void unknownMessage(msg_type_t type, bytes msg)
  {
    cout <<"Got message of type " <<type <<" and length " <<msg.size() <<endl;
  }

};


void Session::process_requests()
{
  while (!m_closed)
  {
    m_proto.rcv_command(*this).wait();
    if (m_closed)
      break;
    m_proto.snd_Error(1, "Not implemented").wait();
  }
}


int main(int argc, char* argv[])
try {

  short unsigned port = 0;

  if (argc > 1)
    port = atoi(argv[1]);
  if (0 == port)
    port = DEFAULT_PORT;


  Socket sock(port);

  cout <<"Waiting for connection on port " <<port <<" ..." <<endl;
  Socket::Connection conn(sock);

  cout <<"New connection, starting session ..." <<endl;
  Session sess(conn);

  cout <<"Session accepted, serving requests ..." <<endl;
  sess.process_requests();

  cout <<"Done!" <<endl;
}
catch (cdk::Error &e)
{
  cout <<"CDK ERROR: " <<e <<endl;
  exit(1);
}
catch (std::exception &e)
{
  cout <<"std exception: " <<e.what() <<endl;
  exit(1);
}
catch (const char *e)
{
  cout <<"ERROR :" <<e <<endl;
  exit(1);
}

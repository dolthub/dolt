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
 * which is part of <MySQL Product>, is also subject to the
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

/*
  Mockup server that listens on MySQLx port 33060 and serves basic requests
  issued using the new X protocol.

  When run, server accepts port number to listen to as the first argument.

  Currently server accepts only one connection and exits when session is terminated.
*/

#include <mysql/cdk/protocol/mysqlx.h>
#include <mysql/cdk/foundation/socket.h>
#include <iostream>
#include <stdlib.h>  // for atoi()

using namespace std;
using namespace cdk;
using namespace cdk::protocol::mysqlx;

using foundation::Socket;
using std::string;

/*
  Instance of Session class creates and handles single session on an
  incoming connection. After constructing Session instance it is ready
  to handle client requests using process_reqeuests() method. Authentication
  is handled inside the constructor an throws error in case valid session
  can not be established.
*/

class Session
  : public Init_processor
  , public Cmd_processor
{
public:

  Session(Socket::Connection &conn);

  ~Session()
  {
    abort();
  }

  void process_requests();
  void abort()
  {
    m_closed= true;
  }

private:

  Protocol_server m_proto;
  std::string m_auth;
  std::string m_user;
  std::string m_pass;
  bool   m_closed;

  void auth_start(const char *mech, bytes data, bytes response)
  {
    m_auth= mech;
    m_user= std::string(data.begin(),data.end());
    m_pass= std::string(response.begin(),response.end());
  }

  void auth_continue(bytes /*data*/)
  {}

  void close()
  {
    cout <<"Client closed connection" <<endl;
    m_closed= true;
  }

  // TODO: make it work with current protocol API
  void unknownMessage(msg_type_t type, bytes msg)
  {
    cout <<"Got message of type " <<type <<" and length " <<msg.size() <<endl;
  }

};


/*
  Create a valid session. No real authentication is performed and all
  incoming connections are accepted.

  For testing purposes some authentication methods trigger special behavior:

  :interrupt: Causes Session constructor to throw error after which server
              interrupts the connection and exits.

  :close: Causes server to close the session immediately after the handshake.
*/

Session::Session(Socket::Connection &conn)
  : m_proto(conn), m_closed(false)
{
  cout <<"Waiting for initial message ..." <<endl;
  m_proto.rcv_InitMessage(*this).wait();
  cout <<"Authentication using method: " <<m_auth <<endl;

  if (m_auth == "interrupt")
    throw "Interrupting authentication";

  if (m_auth == "plain" &&
      m_user == "bad_user" &&
      m_pass == "bad_password")
  {
    byte welcome[] = "Invalid User or password!";
    m_proto.snd_Error(2, L"Invalid User or pasword!");
  }
  else
  {
    byte welcome[] = "Welcome!";
    m_proto.snd_AuthenticateOK(bytes(welcome, sizeof(welcome)-1)).wait();
  }

  if (m_auth == "close")
    abort();
}


void Session::process_requests()
{
  while (!m_closed)
  {
    m_proto.rcv_Command(*this).wait();
    if (m_closed)
      break;
    m_proto.snd_Error(1, L"Not implemented").wait();
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
  conn.wait();

  cout <<"New connection, starting session ..." <<endl;
  Session sess(conn);

  cout <<"Session accepted, serving requests ..." <<endl;
  sess.process_requests();

  cout <<"Done!" <<endl;
}
catch (cdk::Error &e)
{
  cout <<"CDK ERROR: " <<e <<endl;
}
catch (std::exception &e)
{
  cout <<"std exception: " <<e.what() <<endl;
}
catch (const char *e)
{
  cout <<"ERROR: " <<e <<endl;
}

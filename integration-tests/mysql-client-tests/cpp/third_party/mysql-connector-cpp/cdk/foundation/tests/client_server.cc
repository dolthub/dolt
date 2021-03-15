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

#include "test.h"
#include <iostream>
#include <asio.h>
#include <mysql/cdk/foundation/socket.h>

using namespace ::std;

#define PORT 9876

TESTS_INIT() { return 0; }


using namespace cdk::foundation;

class Listener : public Socket_read::Listener
{
  void data(size_t len, byte *data)
  {
    data[len] = '\0';
    cout <<"Received " <<len <<" bytes: " <<data <<endl;
  }

  void eos()
  {
    cout <<"End of stream while reading socket" <<endl;
  }

  void error(size_t pos, Error&)
  {
    cout <<"Socket reading error after " <<pos <<" bytes" <<endl;
  }
};


TEST(Foundation_socket, client)
try {

  io_service io;

  cout <<"Connecting to port " <<PORT <<" ..." <<endl;

  Socket sock(io, PORT);
  sock.connect();

  cout <<"Connected, sending greeting ..." <<endl;

  byte output[]= "Hello World!";

  Output_stream &outs = sock.get_output_stream();
  size_t howmuch= outs.write((byte*)output, sizeof(output));

  cout <<"Wrote " <<howmuch <<" bytes, waiting for reply ..." <<endl;

  Listener listener;
  Socket_read rd(sock, listener);

  while (!rd.is_completed())
  {
    rd.cont();
    cout <<"Waiting for read to complete..." <<endl;
  }

  cout <<"Done!" <<endl;
}
catch (const char *e)
{
  cout <<"ERROR: " <<e <<endl;
}
catch(std::exception &e)
{
  std::cout <<"EXCEPTION: " <<e.what() <<std::endl;
}


#if 0

TEST(Foundation_socket, DISABLED_server)
{
  using namespace ::boost::asio;
  using namespace ::boost::asio::ip;

  ip::address localhost(ip::address_v4::loopback());
  tcp::endpoint endp(localhost, PORT);

  io_service io;

  tcp::acceptor accept(io, endp);
  tcp::socket   sock(io);

  cout <<"Waiting for connection on port " <<PORT <<" ..." <<endl;

  accept.accept(sock);

  cout <<"Connected, waiting for data ..." <<endl;

  char input[128];
  size_t howmuch= sock.receive(buffer(input,sizeof(input)-1));
  input[howmuch]= '\0';

  cout <<"Received " <<howmuch <<" bytes: " <<input <<endl;

  cout <<"Sending back ..." <<endl;

  write(sock, buffer(input, howmuch+1));

  cout <<"Done!" <<endl;
}

#endif

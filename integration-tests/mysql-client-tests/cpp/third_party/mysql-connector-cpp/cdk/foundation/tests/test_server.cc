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
  Simple echo server for testing socket connections.

  Server listens on port given by PORT macro and accepts single
  connection. Then it reads a message from the connection and
  sends it back to the client.
*/

#include <mysql/cdk/foundation/socket.h>
#include <iostream>
#include <exception>

using namespace ::std;
using namespace ::cdk::foundation;

typedef Socket::Connection   Connection;
typedef Socket::Connection::Read_some_op  Rd_op;
typedef Socket::Connection::Write_op      Wr_op;

int main()
{
  try
  {
    Socket sock(PORT);

    cout <<"Waiting for connection on port " <<PORT <<" ..." <<endl;

    Connection conn(sock);
    conn.wait();

    cout <<"Connected, waiting for data ..." <<endl;

    char input[128];

    Rd_op read(conn, buffers((byte*)input,sizeof(input)-1));
    read.wait();
    size_t howmuch= read.get_result();
    input[howmuch]= '\0';

    cout <<"Received " <<howmuch <<" bytes: " <<input <<endl;

    cout <<"Sending back ..." <<endl;

    Wr_op write(conn, buffers((byte*)input, howmuch+1));
    write.wait();

    cout <<"Done!" <<endl;
  }
  catch (std::exception& e)
  {
    cout <<"Test server exit with exception: " <<e.what() <<endl;
  }
  catch (...)
  {
    cout <<"Test server exit with unknown exception." <<endl;
  }
}

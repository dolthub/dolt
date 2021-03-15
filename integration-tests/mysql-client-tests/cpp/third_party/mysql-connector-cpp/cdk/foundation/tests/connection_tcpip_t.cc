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


/**
  Unit tests for sdk::foundation::connection::TCPIP class.
*/

/*
  To use getenv() on Windows, which warns that it is not safe
*/
#undef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS 1

#include "test.h"
#include <process_launcher.h>
#include <exception.h>
#include <iostream>
#include <mysql/cdk/foundation/connection_tcpip.h>
#include <mysql/cdk/foundation/error.h>

#define PORT 9876

using namespace ::std;
using namespace cdk::foundation;

class Foundation_connection_tcpip : public ::testing::Test
{

 protected:
  // Per-test-case set-up.
  // Called before the first test in this test case.
  // Can be omitted if not needed.
  static void SetUpTestCase()
  {

  }

  // Per-test-case tear-down.
  // Called after the last test in this test case.
  // Can be omitted if not needed.
  static void TearDownTestCase()
  {

  }

  // You can define per-test set-up and tear-down logic as usual.
  virtual void SetUp()
  {
    const char *server = getenv("FOUNDATION_TEST_SERVER");
    if (!server)
      server= "foundation/tests/test_server";

    pl = NULL; // in case constructor fails
    try
    {
      const char* args[] = { server, NULL };
      pl = new ngcommon::Process_launcher(server, args);
      pl->read_one_char();

      //Sleep 3sec to allow server to open tcp port.
      cdk::foundation::sleep(3000);
    }
    catch (ngcommon::Exception &e)
    {
      FAIL() <<"Could not launch test server (" <<server <<"): " <<e.what();
    }
  }

  virtual void TearDown()
  {
    delete pl;
  }

  // Some expensive resource shared by all tests.
  //boost::shared_ptr<ngcommon::Process_launcher> pl;
  ngcommon::Process_launcher *pl;

};


/*
  Basic test that connects to the test server, sends a message and
  reads server's reply.

  Note: Test server should be started before running this test.
*/


TEST_F(Foundation_connection_tcpip, basic)
{
  using cdk::foundation::byte;
  using connection::TCPIP;

  char inbuf_raw[13];
  buffers inbuf((byte*)inbuf_raw, sizeof(inbuf_raw)-1);

  cout << "Connecting to wrong port " << 17757 << " ..." << endl;

  TCPIP wrong_conn("localhost", 17757);

  try {
    wrong_conn.connect();
    FAIL() << "Connection attempt should fail." << endl;
  }
  catch (Error &e)
  {
    if (e != cdk::foundation::errc::connection_refused)
      FAIL() << "Received error does not match expected error: " << e << endl;
    cout << "Expected connection error: " << e << endl;
  }

  cout << "Connecting to port " << PORT << " ..." << endl;

  TCPIP conn("localhost", PORT);

  try
  {
    conn.flush();
  }
  catch (connection::Error_no_connection &e)
  {
    cout << "Expected exception: " << e << endl;
  }
  catch (...)
  {
    FAIL() << "Unexpected exception" << endl;
  }


  try {
    conn.connect();
  }
  catch (Error &e)
  {
    cout <<"Connection error: " <<e << endl;
    FAIL() << "Connection error: " << e << endl;
    //if (stderrc::connection_refused == e)
    //  FAIL() << "Is the test server running?";
    //else
    //  FAIL() << "Failed to connect to test server";
  }

  cout <<"Connected, sending greeting ..." <<endl;

  // Get output stream and write to it synchronously

  byte output[]= "Hello World!";
  buffers bufs(output, sizeof(output));
  TCPIP::Write_op write_op(conn, bufs);
  write_op.wait();

  cout << "Wrote " << write_op.get_result() << " bytes, waiting for reply ..."
    << endl;

  cout << "Reading from server ..." << endl;

  // Get input stream and read till the end

  TCPIP::Read_op read_op(conn, inbuf);
  read_op.wait();

  inbuf_raw[read_op.get_result()]= 0;
  cout << "Read " << read_op.get_result() << " bytes: " << inbuf_raw << endl;

  cout <<"Done!" <<endl;
}


/*
  IPv4 connection test.

  Note: Test server should be started before running this test.
*/


TEST_F(Foundation_connection_tcpip, IPv4_connection)
{
  using cdk::foundation::byte;
  using connection::TCPIP;

  TCPIP conn("127.0.0.1", PORT);

  try
  {
    conn.connect();
  }
  catch (Error& e)
  {
    FAIL() << "IPv4 connection failed: " << e.what() << endl;
  }
  catch (...)
  {
    FAIL() << "IPv4 connection failed" << endl;
  }
}


/*
  IPv6 connection test.

  Note: Test server should be started before running this test.
*/


TEST_F(Foundation_connection_tcpip, DISABLED_IPv6_connection)
{
  using cdk::foundation::byte;
  using connection::TCPIP;

  TCPIP conn("::1", PORT);

  try
  {
    conn.connect();
  }
  catch (Error& e)
  {
    FAIL() << "IPv6 connection failed: " << e.what() << endl;
  }
  catch (...)
  {
    FAIL() << "IPv6 connection failed" << endl;
  }
}


/*
  Basic test that connects to the test server, sends a message and
  reads server's reply. Using async API to wait for IO operations.

  Note: Test server should be started before running this test.

  FIXME: (MYC-137) Remove sporadic failures on Win.
*/


TEST_F(Foundation_connection_tcpip, DISABLED_basic_async)
{
  using cdk::foundation::byte;
  using connection::TCPIP;

  char inbuf_raw[13];
  buffers inbuf((byte*)inbuf_raw, sizeof(inbuf_raw) - 1);

  cout << "Connecting to port " << PORT << " ..." << endl;

  TCPIP conn("localhost", PORT);

  try {
    conn.connect();
  }
  catch (Error &e)
  {
    cout << "Connection error: " << e << endl;
    FAIL() << "Connection error: " << e << endl;
  }

  cout << "Connected, sending greeting ..." << endl;

  // Create write operation and use it asynchronously

  byte output[]= "Hello World!";
  buffers bufs(output, sizeof(output));
  TCPIP::Write_op write_op(conn, bufs);
  while (!write_op.cont())
  {
    cout << "Sending bytes to server..." << endl;
  }

  cout << "Wrote " << write_op.get_result() << " bytes, waiting for reply ..."
    << endl;

  cout << "Reading from server ..." << endl;

  // Create read operation and read fixed count of bytes asynchonously

  TCPIP::Read_op read_op(conn, inbuf);
  while (!read_op.cont())
  {
    cout << "Waiting for reply from server..." << endl;
  }

  inbuf_raw[read_op.get_result()]= 0;
  cout << "Read " << read_op.get_result() << " bytes: " << inbuf_raw << endl;

  cout << "Done!" << endl;
}


/*
  Test that connects to the test server, sends a message and
  reads server's reply. But server closes connection before all
  requested bytes are read.

  Note: Test server should be started before running this test.
*/


TEST_F(Foundation_connection_tcpip, sudden_close)
{
  using cdk::foundation::byte;
  using connection::TCPIP;

  char inbuf_raw[100];
  buffers inbuf((byte*)inbuf_raw, sizeof(inbuf_raw) - 1);

  cout << "Connecting to port " << PORT << " ..." << endl;

  TCPIP conn("localhost", PORT);

  try {
    conn.connect();
  }
  catch (Error &e)
  {
    cout << "Connection error: " << e << endl;
    FAIL() << "Connection error: " << e << endl;
  }

  cout << "Connected, sending greeting ..." << endl;

  // Create write operation and use it asynchronously

  byte output[]= "Hello World!";
  buffers bufs(output, sizeof(output));
  TCPIP::Write_op write_op(conn, bufs);
  while (!write_op.cont())
  {
    cout << "Sending bytes to server..." << endl;
  }

  cout << "Wrote " << write_op.get_result() << " bytes, waiting for reply ..."
    << endl;

  cout << "Reading from server ..." << endl;

  // Create read operation and read fixed count of bytes asynchonously
  // Server closes connection after sending limited number of bytes equal
  // to the length of the received message.
  // This causes error because we request to fill our input buffer (inbuf
  // variable) which is few times longer than sent message.

  try
  {
    TCPIP::Read_op read_op(conn, inbuf);

    cout << "Waiting for reply from server..." << endl;
    while (!read_op.cont())
    {
      cdk::foundation::sleep(10);  // 10ms
    }

    inbuf_raw[read_op.get_result()]= 0;
    cout << "Read " << read_op.get_result() << " bytes: " << inbuf_raw << endl;
  }
  catch (connection::Error_eos &e)
  {
    cout << "Expected exception: " << e << endl;
  }
  catch (Error &e)
  {
    FAIL() << "Received error does not match expected error: " << e << endl;
  }
  catch (...)
  {
    FAIL() << "Unexpected exception" << endl;
  }

  cout << "Done!" << endl;
}


/*
  Testing behavior of APIs when there is no connection.
  Stage 1: calling APIs on fresh TCPIP instance without connection
  Stage 2: establish connection and close it
  Stage 3: repeat API calls with closed connection

  Note: Test server should be started before running this test.
*/


TEST_F(Foundation_connection_tcpip, closed)
{
  using cdk::foundation::byte;
  using connection::TCPIP;

  byte buf_raw[100];
  buffers bufs(buf_raw, sizeof(buf_raw));

  TCPIP conn("localhost", PORT);

  // Testing API calls without established connection

  // Stage 1: calling APIs on fresh TCPIP instance without connection

  // close() should do nothing
  EXPECT_NO_THROW(conn.close());

  EXPECT_TRUE(conn.is_ended());
  EXPECT_TRUE(conn.eos());
  EXPECT_TRUE(conn.is_closed());
  EXPECT_FALSE(conn.has_space());
  EXPECT_FALSE(conn.has_bytes());

  EXPECT_THROW(conn.flush(), connection::Error_no_connection);
  EXPECT_THROW(TCPIP::Read_op(conn, bufs), connection::Error_eos);
  EXPECT_THROW(TCPIP::Read_some_op(conn, bufs), connection::Error_eos);
  EXPECT_THROW(TCPIP::Write_op(conn, bufs), connection::Error_no_connection);
  EXPECT_THROW(TCPIP::Write_some_op(conn, bufs), connection::Error_no_connection);


  // Stage 2: establish connection

  try {
    conn.connect();
  }
  catch (Error &e)
  {
    cout << "Connection error: " << e << endl;
    FAIL() << "Connection error: " << e << endl;
  }

  cout << "Connected, sending greeting ..." << endl;

  // Create write operation and use it asynchronously

  strcpy((char*)buf_raw, "Hello World!");
  TCPIP::Write_op write_op(conn, bufs);
  while (!write_op.cont())
  {
    cout << "Sending bytes to server..." << endl;
  }

  cout << "Wrote " << write_op.get_result() << " bytes, waiting for reply ..."
    << endl;

  cout << "Bytes available: " << (conn.has_bytes() ? "yes" : "no") << endl;
  cout << "Has space: " << (conn.has_space() ? "yes" : "no") << endl;

  // Close connection

  conn.close();

  // Stage 3: repeat API calls with closed connection

  // close() should be no-op for closed connection
  EXPECT_NO_THROW(conn.close());

  EXPECT_TRUE(conn.is_ended());
  EXPECT_TRUE(conn.eos());
  EXPECT_TRUE(conn.is_closed());
  EXPECT_FALSE(conn.has_space());
  EXPECT_FALSE(conn.has_bytes());

  EXPECT_THROW(conn.flush(), connection::Error_no_connection);
  EXPECT_THROW(TCPIP::Read_op(conn, bufs), connection::Error_eos);
  EXPECT_THROW(TCPIP::Read_some_op(conn, bufs), connection::Error_eos);
  EXPECT_THROW(TCPIP::Write_op(conn, bufs), connection::Error_no_connection);
  EXPECT_THROW(TCPIP::Write_some_op(conn, bufs), connection::Error_no_connection);

}



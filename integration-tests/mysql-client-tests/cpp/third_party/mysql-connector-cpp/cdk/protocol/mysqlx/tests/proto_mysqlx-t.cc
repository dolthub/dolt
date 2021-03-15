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

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <stdexcept>


#include <mysql/cdk.h>
#include <mysql/cdk/protocol/mysqlx.h>

#include <cdk_test.h>
#include <gtest/gtest.h>


using namespace cdk;
using namespace cdk::protocol::mysqlx;
//using namespace google::protobuf;

using std::cout;
using std::endl;
//using cdk::string;



TEST(Protocol_mysqlx, io_buffers)
{
  typedef foundation::test::Mem_stream<16*1024*1024> Stream;

  try {

    /*
      Note: objects of large size can not be allocated on stack.
      Hence we use heap allocated objects.
    */

    scoped_ptr<Stream> conn(new Stream());

    Protocol proto(*conn);
    Protocol_server srv(*conn);

    // Send large packet

    std::string buf;
    buf.reserve(12*1024*1024);

    bytes data((byte*)buf.data(), buf.capacity());

    cout <<"Sending AuthStart message with " <<buf.capacity()
         <<" bytes of auth data" <<endl;

    proto.snd_AuthenticateStart("test", data, bytes("")).wait();

    // read it from the other end

    struct : public Init_processor
    {
      size_t auth_size;

      void auth_start(const char *mech, bytes data, bytes resp)
      {
        cout <<"Got AuthStart message for " <<mech;
        cout <<" with " <<data.size() <<" bytes of auth data" <<endl;
        EXPECT_EQ(auth_size, data.size());
      }

      void auth_continue(bytes data)
      {}

    } m_iproc;

    m_iproc.auth_size= data.size();
    srv.rcv_InitMessage(m_iproc).wait();

    cout <<"Done!" <<endl;
  }
  CATCH_TEST_GENERIC;
}

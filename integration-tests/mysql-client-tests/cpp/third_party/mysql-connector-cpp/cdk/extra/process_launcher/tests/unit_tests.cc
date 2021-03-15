/*
 * Copyright (c) 2014, 2018, Oracle and/or its affiliates. All rights reserved.
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

#ifdef WIN32
#  include <windows.h>
#else
#  include <sys/time.h>
#endif

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <boost/shared_ptr.hpp>

#include "process_launcher.h"
#include "gtest/gtest.h"

#ifndef PROCESS_LAUNCHER_TESTS_DIR
#error Please rerun the cmake to have the available definition #define PROJECT_SOURCE_DIR "${PROCESS_LAUNCHER_TESTS_DIR}/tests/scripts"
#endif

using namespace ngcommon;

// The unit tests assume that Python is in the path, as well as MySql CLI tools.
// And a server running in localhost:3305.

namespace ngcommon
{
namespace tests
{
  const int port = 3305;

  const std::string *get_path(const char *filename)
  {
    std::string *path = new std::string(PROCESS_LAUNCHER_TESTS_DIR);
    *path += "/";
    *path += filename;
    return path;
  }

  const std::string *get_file_contents(const char *filename)
  {
    std::string path = std::string(PROCESS_LAUNCHER_TESTS_DIR);
    path += "/";
    path += filename;
    std::ifstream iff(path.c_str(), std::ios::in);

    if (iff)
    {
      std::string *s = new std::string();
      iff.seekg(0, iff.end);
      s->resize(static_cast<unsigned int>(iff.tellg()));
      iff.seekg(0, std::ios::beg);
      iff.read(&(*s)[0], s->size());
      iff.close();
      return s;
    }
    else
    {
      return NULL;
    }
  }

  // Scenario 1 tests the scenario
  //   spawn
  //   wait
  // A python script that sleep 5 seconds is launched and waited for.
  TEST(Scenario1, Simple)
  {
    const std::string *path_script = get_path("long_test.py");
    const char *args[] = { "python", path_script->c_str(), NULL };
    Process_launcher p("python", args);

#ifdef WIN32
    LARGE_INTEGER start, end, freq;
    double elapsed_time;

    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start);
#else
    struct timeval start, end;
    double elapsed_time;

    gettimeofday(&start, NULL);
#endif

    int result = p.wait();

#ifdef WIN32
    QueryPerformanceCounter(&end);
    elapsed_time = (end.QuadPart - start.QuadPart) * 1000.0 / freq.QuadPart;

    if(elapsed_time < 5000)
      EXPECT_TRUE(5000 - elapsed_time < 1000);
    else
      EXPECT_TRUE(elapsed_time - 5000 < 1000);
#else
    gettimeofday(&end, NULL);
    elapsed_time = (end.tv_sec - start.tv_sec) * 1000.0;
    elapsed_time += (end.tv_usec - start.tv_usec) / 1000.0;

    if(elapsed_time < 5000)
      EXPECT_TRUE(5000 - elapsed_time < 1000);
    else
      EXPECT_TRUE(elapsed_time - 5000 < 1000);
#endif

    delete path_script;
  }

  // Same than scenario one, but this time the child process is killed instead of waiting for it.
  TEST(Scenario1, WithKill)
  {
    const std::string *path_script = get_path("long_test.py");
    const char *args[] = { "python", path_script->c_str(), NULL };
    Process_launcher p("python", args);

    p.kill();

#ifdef WIN32
    LARGE_INTEGER start, end, freq;
    double elapsed_time;

    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start);
#else
    struct timeval start, end;
    double elapsed_time;

    gettimeofday(&start, NULL);
#endif

    p.wait();

#ifdef WIN32
    QueryPerformanceCounter(&end);
    elapsed_time = (end.QuadPart - start.QuadPart) * 1000.0 / freq.QuadPart;

    if (elapsed_time < 1000)
      EXPECT_TRUE(1000 - elapsed_time < 1000);
    else
      EXPECT_TRUE(elapsed_time - 1000 < 1000);
#else
    gettimeofday(&end, NULL);
    elapsed_time = (end.tv_sec - start.tv_sec) * 1000.0;
    elapsed_time += (end.tv_usec - start.tv_usec) / 1000.0;

    if (elapsed_time < 1000)
      EXPECT_TRUE(1000 - elapsed_time < 1000);
    else
      EXPECT_TRUE(elapsed_time - 1000 < 1000);
#endif

    delete path_script;
  }

  // spawn
  // while not eof :
  //   stdout.read
  // wait
  TEST(Scenario2, MySqlDump)
  {
    char buf[80];
    int i = 0, cnt;
    // NOTE: Windows allows this syntax (but linux's execvp does not)
    //const char *args[] = { "mysqldump", "-u root --port=3305 --protocol=TCP --databases sakila", NULL };
    // so the portable way is
    const char *args[] = { "mysqldump", "-u", "root", "--port=3305", "--protocol=TCP", "--databases", "sakila", NULL };
#ifdef WIN32
    Process_launcher p("mysqldump", args);
#else
    Process_launcher p("/usr/local/mysql/bin/mysqldump", args);
#endif

    while ((cnt = p.read(buf, sizeof(buf))) > 0)
    {
      i += cnt;
    }

    p.wait();

#ifdef WIN32
    EXPECT_EQ(3356773, i);
#else
    EXPECT_EQ(3349805, i);
#endif
  }

  //
  // spawn
  // stdin.write
  // while not eof :
  //   stdout.read
  // wait
  TEST(Scenario3, PrintHelloNTimes)
  {
    const std::string *path_script = get_path("printn.py");
    const char *args[] = { "python", path_script->c_str(), NULL };
    Process_launcher p("python", args);
    std::string buf;
    char c, c1;
    int i = 1;

    p.write("3\n", 2);
    while (p.read(&c, 1) > 0)
    {
      buf += c;
      if (c == '\n')
      {
        c1 = '0' + i;
        std::string data("hello");
        data.append(&c1, 1);
#ifdef WIN32
        data += "\r\n";
#else
  data += "\n";
#endif
        EXPECT_STREQ(data.c_str(), buf.c_str());
        i++;
        buf = "";
      }
    }
    EXPECT_EQ(4, i);
    p.wait();
    delete path_script;
  }
}
}

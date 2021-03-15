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

#ifndef SDK_FOUNDATION_CDK_TIME_H
#define SDK_FOUNDATION_CDK_TIME_H

#include "common.h"

PUSH_SYS_WARNINGS_CDK
#include <time.h>
POP_SYS_WARNINGS_CDK

namespace cdk {
namespace foundation {

// In CDK, time points are expressed in milliseconds since epoch

typedef  long long  time_t;


// Get the current time point

inline
time_t  get_time()
{
  ::time_t   now_sec;
  long int   now_ms;

#if defined(_WIN32)

  SYSTEMTIME now;
  GetSystemTime(&now);
  now_ms = now.wMilliseconds;

#elif defined(__APPLE__)

  struct timeval now;
  gettimeofday(&now, NULL);
  now_ms = now.tv_usec / 1000;

#else

  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  now_ms = now.tv_nsec / 1000000;

#endif

  ::time(&now_sec);

  return 1000U*now_sec + now_ms;
}


// Sleep for given number of milliseconds

inline
void sleep(time_t howlong)
{
#if defined(_WIN32)

  Sleep((DWORD)howlong);

#else

  struct timespec time;
  time.tv_sec = howlong/1000;
  time.tv_nsec = (howlong%1000)*1000000;

  while (!nanosleep(&time, &time))
  {
    if (EINTR != errno)
      break;
  }

#endif
}


}}  // cdk::foundation

#endif

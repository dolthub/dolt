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
  Unit tests for CDK error handling infrastructure.
*/

#include "test.h"
#include <iostream>
#include <stdexcept>
#include <mysql/cdk/foundation/error.h>

using ::std::cout;
using ::std::endl;
using namespace cdk::foundation;


/*
  Throwing and re-throwing errors
  ===============================
*/

TEST(Errors, basic)
{
  try {
    throw_error("Test error");
  }
  CATCH_AND_PRINT({
    cout <<"Error description: " <<error.description() <<endl;
  })

  try {
    throw_error("Test error");
  }
  catch (const std::exception &e)
  {
    cout <<e.what() <<endl;
  }

  try {
    try {
      try {
        throw_error("Base error");
      }
      catch (...)
      {
        rethrow_error("First layer");
      }
    }
    catch (...)
    {
      rethrow_error("Second layer");
    }
  }
  catch (const Error &e)
  {
    e.describe(cout);
    cout <<endl;
  }
  //CATCH_AND_PRINT({})

  cout <<"Done!" <<endl;
}


#define RETHROW(E) \
  try { \
    try { throw (E); } catch (...) { rethrow_error("Wrapped"); } \
  } CATCH_AND_PRINT({})

TEST(Errors, wrap)
{
  RETHROW(std::runtime_error("standard exception"));
  RETHROW("string exception");
}


TEST(Errors, categories)
{
  error_code ec1(0, generic_error_category());
  cout <<"error in generic category: " <<ec1 <<": " <<ec1.message() <<endl;

  error_code ec2(0, system_error_category());
  cout <<"error in system category: " <<ec2 <<": " <<ec2.message() <<endl;

  error_code ec3(0, posix_error_category());
  cout <<"error in posix category: " <<ec3 <<": " <<ec3.message() <<endl;

  error_code ec4(0, std_error_category());
  cout <<"error in standard category: " <<ec4 <<": " <<ec4.message() <<endl;
}


TEST(Errors, conditions)
{
  error_condition ec1(cdkerrc::generic_error);
  cout <<"error condition " <<ec1 <<": " <<ec1.message() <<endl;

  error_condition ec2(errc::io_error);
  cout <<"error condition " <<ec2 <<": " <<ec2.message() <<endl;

  error_condition ec3(22);
  cout <<"error condition " <<ec3 <<": " <<ec3.message() <<endl;
}


TEST(Errors, posix)
{
  try {
    errno = errc::file_too_large;
    throw_posix_error();
    FAIL() <<"Should throw error";
  }
  CATCH_AND_PRINT({})

  try {
    errno = errc::bad_file_descriptor;
    throw_posix_error("Prefix");
    FAIL() <<"Should throw error";
  }
  CATCH_AND_PRINT({})

  try {
    errno = 0;
    throw_posix_error("Prefix");
  }
  CATCH_AND_PRINT({
    FAIL() << "Should be no error if errno is zero";
  })

}


TEST(Errors, system)
{
  try {

#ifdef _WIN32
    SetLastError(ERROR_FILE_NOT_FOUND);
#else
    errno = errc::file_exists;
#endif

    throw_system_error();
    FAIL() <<"Should throw error";
  }
  CATCH_AND_PRINT({})

  try {

#ifdef _WIN32
    SetLastError(DNS_ERROR_RCODE_NAME_ERROR);
#else
    errno = errc::bad_file_descriptor;
#endif

    // Note: This fails because string conversion clears windows
    // error set above.

    throw_system_error("Prefix");
    FAIL() <<"Should throw error";
  }
  CATCH_AND_PRINT({})

  try {

#ifdef _WIN32
    SetLastError(0);
#else
    errno = 0;
#endif

    throw_system_error("Prefix");
  }
  CATCH_AND_PRINT({
    FAIL() << "Should be no error if errno is zero";
  })

}

/*
  Defining error category
  =======================
  See cdk/foundation/error_category.h for more information.
*/

#define EC_test_ERRORS(X) \
  CDK_ERROR(X, FIRST,  1, "First test error") \
  CDK_ERROR(X, SECOND, 2, "Second test error")

CDK_ERROR_CATEGORY(test, test_errc)


cdk::foundation::error_condition
error_category_test::do_default_error_condition(int) const
{
  throw "not implemented";
}

bool
error_category_test::do_equivalent(int code, const cdk::foundation::error_condition &ec) const
{
  if (generic_error_category() != ec.category())
    return false;

  switch (code)
  {
  case test_errc::FIRST:
    return (int)cdk::foundation::cdkerrc::generic_error == ec.value();
  default: return false;
  }
}


TEST(Errors, category)
{
  try {
    throw_error(test_error(test_errc::FIRST));
  }
  CATCH_AND_PRINT({})

  // comparing with error conditions

  try {
    throw_error(test_error(test_errc::FIRST));
    FAIL() << "First error not thrown!";
  }
  catch (const Error &e)
  {
    cout << "First error: " << e << endl;
    EXPECT_EQ(e, cdk::foundation::cdkerrc::generic_error);
    EXPECT_NE(e, cdk::foundation::cdkerrc::standard_exception);
  }

  try {
    throw_error(test_error(test_errc::SECOND));
    FAIL() << "Second error not thrown!";
  }
  catch (const Error &e)
  {
    cout << "Second error: " << e << endl;
    EXPECT_NE(e, cdk::foundation::cdkerrc::generic_error);
  }

  // extending

  try {
    throw_error(test_error(test_errc::SECOND), "With prefix");
  }
  CATCH_AND_PRINT({})

  try {
    try {
      throw_error(test_errc::FIRST, test_error_category());
    }
    catch (...)
    {
      rethrow_error("Extended");
    }
  }
  CATCH_AND_PRINT({})
}


/*
  Defining custom errors
  ======================
*/

class Test_error : public Error_class<Test_error>
{
  typedef Error_class<Test_error> Base;
  using string = std::string;

  string m_name;
  int m_num;

public:

  Test_error(const string &name, int num)
    : Base(NULL, test_error(test_errc::SECOND))
    , m_name(name)
    , m_num(num)
  {}

  virtual ~Test_error() throw() {}

  // Custom prefix

  bool add_prefix(std::ostream &out) const
  {
    out <<"Test error " <<m_name <<"#" <<m_num;
    return true;
  }

  // Custom description

  void describe(std::ostream &out) const
  {
    add_prefix(out);
    out <<" [" <<code().message() <<"] (" <<code() <<")";
  }
};


TEST(Errors, custom)
{
  try {
    throw Test_error("foo",7);
  }
  CATCH_AND_PRINT({})

  try {
    try {
      throw Test_error("bar",0);
    }
    catch (...)
    {
      rethrow_error("Extended");
    }
  }
  CATCH_AND_PRINT({})
}


TEST(Errors, rethrow)
{
  std::string description;
  Error  *eptr;

  try {
    throw Test_error("baz", 8);
  }
  catch (Error &e)
  {
    cout <<e <<endl;
    eptr= e.clone();
    description= e.description();

    try {
      /*
        Note: throw *eptr will not work because error type information
        is lost.
      */
      eptr->rethrow();
    }
    CATCH_AND_PRINT({
      EXPECT_EQ(description, (std::string)error.description());
    })

    delete eptr;
  }
}


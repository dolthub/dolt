/*
 * Copyright (c) 2008, 2019, Oracle and/or its affiliates. All rights reserved.
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



/*
   Basic example of an application using JDBC API of Connector/C++
*/

/* Standard C++ includes */

#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <stdexcept>

/*
  Note: Boost must be in the include path to build code which uses the
  JDBC API.
*/

#include <boost/scoped_ptr.hpp>

#include <mysql/jdbc.h>

#define DEFAULT_URI "tcp://127.0.0.1"
#define EXAMPLE_USER "root"
#define EXAMPLE_PASS ""
#define EXAMPLE_DB "test"

using namespace std;

/*
  Usage example for Driver, Connection, (simple) Statement, ResultSet
*/

int main(int argc, const char **argv)
{
  const char   *url = (argc > 1 ? argv[1] : DEFAULT_URI);
  const string user(argc >= 3 ? argv[2] : EXAMPLE_USER);
  const string pass(argc >= 4 ? argv[3] : EXAMPLE_PASS);
  const string database(argc >= 5 ? argv[4] : EXAMPLE_DB);

  cout << endl;
  cout << "Connector/C++ standalone program example..." << endl;
  cout << endl;

  try {

    sql::Driver * driver = sql::mysql::get_driver_instance();

    /* Using the Driver to create a connection */

    cout << "Creating session on " << url << " ..."
         << endl << endl;

    boost::scoped_ptr< sql::Connection >
      con(driver->connect(url, user, pass));
    con->setSchema(database);

    boost::scoped_ptr< sql::Statement > stmt(con->createStatement());
    boost::scoped_ptr< sql::ResultSet >
      res(stmt->executeQuery("SELECT 'Welcome to Connector/C++' AS _message"));
    cout << "\t... running 'SELECT 'Welcome to Connector/C++' AS _message'"
         << endl;

    while (res->next())
    {
      cout << "\t... MySQL replies: " << res->getString("_message") << endl;
      cout << "\t... say it again, MySQL" << endl;
      cout << "\t....MySQL replies: " << res->getString(1) << endl;
    }

  }
  catch (sql::SQLException &e)
  {
    /*
      The JDBC API throws three different exceptions:

    - sql::MethodNotImplementedException (derived from sql::SQLException)
    - sql::InvalidArgumentException (derived from sql::SQLException)
    - sql::SQLException (derived from std::runtime_error)
    */

    cout << "# ERR: SQLException in " << __FILE__;
    cout << "(" << "EXAMPLE_FUNCTION" << ") on line " << __LINE__ << endl;

    /* Use what() (derived from std::runtime_error) to fetch the error message */

    cout << "# ERR: " << e.what();
    cout << " (MySQL error code: " << e.getErrorCode();
    cout << ", SQLState: " << e.getSQLState() << " )" << endl;

    return EXIT_FAILURE;
  }

  cout << endl;
  cout << "... find more at http://www.mysql.com" << endl;
  cout << endl;
  return EXIT_SUCCESS;
}

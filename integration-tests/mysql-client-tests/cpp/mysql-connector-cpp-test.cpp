#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <stdexcept>


#include "mysql_driver.h"
#include "mysql_connection.h"
#include "mysql_error.h"
#include "cppconn/connection.h"
#include "cppconn/build_config.h"
#include "cppconn/resultset.h"
#include "cppconn/sqlstring.h"
#include "cppconn/config.h"
#include "cppconn/datatype.h"
#include "cppconn/version_info.h"
#include "cppconn/driver.h"
#include "cppconn/statement.h"
#include "cppconn/metadata.h"
#include "cppconn/warning.h"
#include "cppconn/resultset_metadata.h"
#include "cppconn/parameter_metadata.h"
#include "cppconn/exception.h"
#include "cppconn/prepared_statement.h"
#include "cppconn/variant.h"

#define QUERIES_SIZE 14

std::string queries[QUERIES_SIZE] =
  {
   "create table test (pk int, `value` int, primary key(pk))",
   "describe test",
   "select * from test",
   "insert into test (pk, `value`) values (0,0)",
   "select * from test",
   "select dolt_add('-A');",
   "select dolt_commit('-m', 'my commit')",
   "select COUNT(*) FROM dolt_log",
   "select dolt_checkout('-b', 'mybranch')",
   "insert into test (pk, `value`) values (1,1)",
   "select dolt_commit('-a', '-m', 'my commit2')",
   "select dolt_checkout('main')",
   "select dolt_merge('mybranch')",
   "select COUNT(*) FROM dolt_log",
  };

int is_update[QUERIES_SIZE] = {1,0,0,1,0,0,0,0,0,1,0,0,0,0};

int main(int argc, char **argv) {
  std::string user = argv[1];
  std::string port = argv[2];
  std::string db   = argv[3];
  
  sql::mysql::MySQL_Driver *driver;

  sql::Connection *con;
  
  driver = sql::mysql::get_mysql_driver_instance();
  con = driver->connect("tcp://127.0.0.1:" + port, user, "");
  con->setSchema(db);
  
  for ( int i = 0; i < QUERIES_SIZE; i++ ) {
    try {
      sql::Statement *stmt = con->createStatement();

      if ( is_update[i] ) {
	int affected_rows = stmt->executeUpdate(queries[i]);
      } else {
	sql::ResultSet *res = stmt->executeQuery(queries[i]);
	delete res;
      }
    
      delete stmt;
    } catch (sql::SQLException &e) {
      std::cout << "QUERY: " << queries[i] << std::endl;
      std::cout << "# ERR: " << e.what();
      std::cout << " (MySQL error code: " << e.getErrorCode();
      std::cout << ", SQLState: " << e.getSQLState() << " )" << std::endl;
      return 1;
    }
  }
  
  delete con;

  return 0;
}

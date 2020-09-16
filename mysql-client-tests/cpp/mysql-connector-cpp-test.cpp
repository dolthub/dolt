#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <stdexcept>


#include "mysql_driver.h"
#include <mysql/jdbc.h>

#define QUERIES_SIZE 5

std::string queries[QUERIES_SIZE] =
  {
   "create table test (pk int, value int, primary key(pk))",
   "describe test",
   "select * from test",
   "insert into test (pk, value) values (0,0)", 
   "select * from test"
  };

int is_update[QUERIES_SIZE] = {0,0,0,1,0};

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

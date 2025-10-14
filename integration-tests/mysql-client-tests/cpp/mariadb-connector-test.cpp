#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <memory>
#include <mariadb/conncpp.hpp>

#define QUERIES_SIZE 14

std::string queries[QUERIES_SIZE] =
  {
   "create table test (pk int, `value` int, primary key(pk))",
   "describe test",
   "select * from test",
   "insert into test (pk, `value`) values (0,0)",
   "select * from test",
   "call dolt_add('-A');",
   "call dolt_commit('-m', 'my commit')",
   "select COUNT(*) FROM dolt_log",
   "call dolt_checkout('-b', 'mybranch')",
   "insert into test (pk, `value`) values (1,1)",
   "call dolt_commit('-a', '-m', 'my commit2')",
   "call dolt_checkout('main')",
   "call dolt_merge('mybranch')",
   "select COUNT(*) FROM dolt_log",
  };

int is_update[QUERIES_SIZE] = {1,0,0,1,0,0,0,0,0,1,0,0,0,0};

int main(int argc, char **argv) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <user> <port> <database>" << std::endl;
        return 1;
    }

  std::string user = argv[1];
  std::string port = argv[2];
  std::string db   = argv[3];
  
  try {
    // Get the driver instance
    sql::Driver* driver = sql::mariadb::get_driver_instance();
    
    // Create connection properties
    sql::SQLString url("jdbc:mariadb://127.0.0.1:" + port + "/" + db);
    sql::Properties properties({{"user", user}, {"password", ""}});
    
    // Establish connection
    std::unique_ptr<sql::Connection> con(driver->connect(url, properties));
    
    for ( int i = 0; i < QUERIES_SIZE; i++ ) {
      try {
        std::unique_ptr<sql::Statement> stmt(con->createStatement());

        if ( is_update[i] ) {
          stmt->executeUpdate(queries[i]);
        } else {
          std::unique_ptr<sql::ResultSet> res(stmt->executeQuery(queries[i]));

          // Assert that all columns have column name metadata populated
          sql::ResultSetMetaData* metadata = res->getMetaData();
          const uint32_t columnCount = metadata->getColumnCount();
          for (uint32_t columnIndex = 1; columnIndex <= columnCount; ++columnIndex) {
              sql::SQLString columnName = metadata->getColumnName(columnIndex);
              if (columnName.length() == 0) {
                  std::cerr << "Column name is empty at index " << columnIndex << std::endl;
                  return 1;
              }
          }
        }
      } catch (sql::SQLException &e) {
        std::cout << "QUERY: " << queries[i] << std::endl;
        std::cout << "# ERR: " << e.what();
        std::cout << " (MariaDB error code: " << e.getErrorCode();
        std::cout << ", SQLState: " << e.getSQLState() << " )" << std::endl;
        return 1;
      }
    }
    
    return 0;
  } catch (sql::SQLException &e) {
    std::cerr << "Connection error: " << e.what() << std::endl;
    std::cerr << " (MariaDB error code: " << e.getErrorCode();
    std::cerr << ", SQLState: " << e.getSQLState() << " )" << std::endl;
    return 1;
  }
}


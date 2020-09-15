#include <iostream>

#include "mysql_driver.h"

int main(int argc, char **argv) {
  std::string user = argv[1];
  std::string port = argv[2];
  std::string db   = argv[3];
  
  sql::mysql::MySQL_Driver *driver;
  sql::Connection *con;
  
  driver = sql::mysql::get_mysql_driver_instance();
  con = driver->connect("tcp://127.0.0.1:" + port, user, "");
  
  delete con;
}

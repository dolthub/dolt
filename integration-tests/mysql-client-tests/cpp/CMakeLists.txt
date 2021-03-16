cmake_minimum_required(VERSION 3.10)

project(DoltCxxConnectorTest
	VERSION 0.1
	DESCRIPTION "A smoke test for mysql-connector-c++ connecting to Dolt"
	LANGUAGES CXX)

add_executable(test_mysql_connector_cxx mysql-connector-cpp-test.cpp)
set_property(TARGET test_mysql_connector_cxx PROPERTY CXX_STANDARD 11)

if(WITH_JDBC)
  add_subdirectory(third_party/mysql-connector-cpp EXCLUDE_FROM_ALL)
  target_link_libraries(test_mysql_connector_cxx connector-jdbc)
else()
  find_library(LIBMYSQLCPPCONN "mysqlcppconn")
  target_link_libraries(test_mysql_connector_cxx "${LIBMYSQLCPPCONN}")
endif()

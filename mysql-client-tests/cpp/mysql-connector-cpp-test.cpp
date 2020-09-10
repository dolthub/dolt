#include <stdio.h>
#include <string>

#include <mysqlx/xdevapi.h>

#define QUERIES_SIZE 5

using namespace ::mysqlx;

const char *queries[QUERIES_SIZE] =
  {
   "create table test (pk int, value int, primary key(pk))",
   "describe test",
   "select * from test",
   "insert into test (pk, value) values (0,0)", 
   "select * from test"
  };

int main(int argc, char **argv) { 

  std::string user = argv[1];
  std::string port = argv[2];
  std::string db   = argv[3];

  std::string url = "mysqlx://" + user + "@127.0.0.1:" + port;

  Session sess(url);
  
  return 0;
}

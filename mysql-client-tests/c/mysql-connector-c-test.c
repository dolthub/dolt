#include <stdio.h>
#include <mysql.h>

#define QUERIES_SIZE 5

MYSQL mysql;
MYSQL *conn;

char *queries[QUERIES_SIZE] =
  {
   "create table test (pk int, value int, primary key(pk))",
   "describe test",
   "select * from test",
   "insert into test (pk, value) values (0,0)", 
   "select * from test"
  };

int main(int argc, char **argv) { 

  char* user = argv[1];
  int   port = atoi(argv[2]);
  char* db   = argv[3];
  
  MYSQL *con = mysql_init(NULL);

  if ( con == NULL ) {
      fprintf(stderr, "%s\n", mysql_error(con));
      exit(1);
  }

  if ( mysql_real_connect(con,
			 "127.0.0.1",
			 user,
			 "",
			 db,
			 port,
			 NULL,
			 0 ) == NULL) {
    fprintf(stderr, "%s\n", mysql_error(con));
    mysql_close(con);
    exit(1);
  }

  for ( int i = 0; i < QUERIES_SIZE; i++ ) {
    if ( mysql_query(con, queries[i]) ) {
      printf("QUERY FAILED: %s\n", queries[i]);
      fprintf(stderr, "%s\n", mysql_error(con));
      mysql_close(con);
      exit(1);
    } else {
      // Not checking validity of results for now
      MYSQL_RES* result = mysql_use_result(con);
      mysql_free_result(result);
    }
  }

  mysql_close(con);
  
  return 0;
}

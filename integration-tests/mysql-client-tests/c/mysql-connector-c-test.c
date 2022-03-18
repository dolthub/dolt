#include <stdio.h>
#include <string.h>
#include <memory.h>
#include <mysql.h>

#define QUERIES_SIZE 14

char *queries[QUERIES_SIZE] =
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
   "insert into test (pk, `value`) values (10,10)",
   "select dolt_commit('-a', '-m', 'my commit2')",
   "select dolt_checkout('main')",
   "select dolt_merge('mybranch')",
   "select COUNT(*) FROM dolt_log",
  };

typedef struct statement_t {
  char *query;
  MYSQL_BIND bind[10];
  int expect_prepare_error;
  int expect_exec_error;
  int expect_result_metadata;
} statement;

void test_statement(MYSQL *con, statement *stmt) {
  MYSQL_STMT *mstmt = mysql_stmt_init(con);
  if (!mstmt) {
    fprintf(stderr, "failed to init stmt: %s\n", mysql_error(con));
    exit(1);
  }
  if ( mysql_stmt_prepare(mstmt, stmt->query, strlen(stmt->query)) ) {
    if ( !stmt->expect_prepare_error) {
      fprintf(stderr, "failed to prepare stmt: %s: %s\n", stmt->query, mysql_stmt_error(mstmt));
      exit(1);
    } else {
      goto close;
    }
  }
  if ( mysql_stmt_bind_param(mstmt, stmt->bind) ) {
    fprintf(stderr, "failed to bind stmt: %s: %s\n", stmt->query, mysql_stmt_error(mstmt));
    exit(1);
  }
  MYSQL_RES *metadata = mysql_stmt_result_metadata(mstmt);
  if (stmt->expect_result_metadata && metadata == NULL) {
    fprintf(stderr, "result metadata was unexpectedly NULL: %s\n", stmt->query);
    exit(1);
  } else if (!stmt->expect_result_metadata && metadata != NULL) {
    fprintf(stderr, "result metadata was unexpectedly non-NULL: %s\n", stmt->query);
    exit(1);
  }
  if ( mysql_stmt_execute(mstmt) ) {
    if ( !stmt->expect_exec_error) {
      fprintf(stderr, "failed to execute stmt: %s: %s\n", stmt->query, mysql_stmt_error(mstmt));
      exit(1);
    }
  }
  // TODO: Add test for mysql_stmt_store_result when supported
close:
  if ( mysql_stmt_close(mstmt) ) {
    fprintf(stderr, "failed to close stmt: %s: %s\n", stmt->query, mysql_error(con));
    exit(1);
  }
}

statement LAST_STATEMENT = {
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

  int pk = 1;
  int value = 12;
  unsigned long string_len = 16;
  statement statements[] = {
    {
      .query = "select * from test where pk = ?",
      .bind = {
        [0] = {
         .buffer_type = MYSQL_TYPE_LONG,
         .buffer = (void *)(&pk),
         .buffer_length = sizeof(pk),
        },
      },
      .expect_result_metadata = 1,
    },
    {
      .query = "select * from test where pk = ?",
      .bind = {
        [0] = {
         .buffer_type = MYSQL_TYPE_LONG,
         .buffer = (void *)(&pk),
         .buffer_length = sizeof(pk),
         .is_unsigned = 1,
        },
      },
      .expect_result_metadata = 1,
    },
    {
      .query = "insert into test values (?, ?)",
      .bind = {
        [0] = {
         .buffer_type = MYSQL_TYPE_LONG,
         .buffer = (void *)(&pk),
         .buffer_length = sizeof(pk),
        },
        [1] = {
         .buffer_type = MYSQL_TYPE_LONG,
         .buffer = (void *)(&value),
         .buffer_length = sizeof(value),
        },
      },
      .expect_result_metadata = 0,
    },
    {
      .query = "update test set `value` = ?",
      .bind = {
        [0] = {
         .buffer_type = MYSQL_TYPE_STRING,
         .buffer = (void *)"test string here",
         .buffer_length = string_len,
         .length = &string_len,
        },
      },
      .expect_exec_error = 1,
      .expect_result_metadata = 0,
    },
    {
      .query = "select * from test SYNTAX ERROR where pk = ?",
      .bind = {
        [0] = {
         .buffer_type = MYSQL_TYPE_LONG,
         .buffer = (void *)(&pk),
         .buffer_length = sizeof(pk),
        },
      },
      .expect_prepare_error = 1,
    },
    LAST_STATEMENT,
  };

  for (int i = 0; statements[i].query; i++) {
    test_statement(con, &statements[i]);
  }

  mysql_close(con);
  
  return 0;
}

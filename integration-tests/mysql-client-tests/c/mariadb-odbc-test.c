#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>

#define QUERIES_SIZE 14

char *queries[QUERIES_SIZE] = {
    "create table test (pk int, `value` int, primary key(pk))",
    "describe test",
    "select * from test",
    "insert into test (pk, `value`) values (0,0)",
    "select * from test",
    "call dolt_add('-A')",
    "call dolt_commit('-m', 'my commit')",
    "select COUNT(*) FROM dolt_log",
    "call dolt_checkout('-b', 'mybranch')",
    "insert into test (pk, `value`) values (10,10)",
    "call dolt_commit('-a', '-m', 'my commit2')",
    "call dolt_checkout('main')",
    "call dolt_merge('mybranch')",
    "select COUNT(*) FROM dolt_log",
};

void check_error(SQLRETURN ret, SQLHANDLE handle, SQLSMALLINT type, const char *msg) {
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        SQLCHAR sqlstate[6];
        SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
        SQLINTEGER native_error;
        SQLSMALLINT text_length;
        
        SQLGetDiagRec(type, handle, 1, sqlstate, &native_error, message, sizeof(message), &text_length);
        fprintf(stderr, "%s\nSQLSTATE: %s\nMessage: %s\n", msg, sqlstate, message);
        exit(1);
    }
}

typedef struct prepared_statement_t {
    char *query;
    int num_params;
    int pk_param;
    int value_param;
    int expect_prepare_error;
    int expect_exec_error;
} prepared_statement;

void test_prepared_statement(SQLHDBC dbc, prepared_statement *pstmt) {
    SQLHSTMT stmt;
    SQLRETURN ret;
    
    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    check_error(ret, dbc, SQL_HANDLE_DBC, "Failed to allocate statement handle");
    
    ret = SQLPrepare(stmt, (SQLCHAR *)pstmt->query, SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        if (!pstmt->expect_prepare_error) {
            check_error(ret, stmt, SQL_HANDLE_STMT, "Failed to prepare statement");
        } else {
            SQLFreeHandle(SQL_HANDLE_STMT, stmt);
            return;
        }
    }
    
    if (pstmt->num_params > 0) {
        if (pstmt->pk_param) {
            SQLINTEGER pk = 1;
            ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, &pk, 0, NULL);
            check_error(ret, stmt, SQL_HANDLE_STMT, "Failed to bind pk parameter");
        }
        if (pstmt->num_params > 1 && pstmt->value_param) {
            SQLINTEGER value = 12;
            ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, &value, 0, NULL);
            check_error(ret, stmt, SQL_HANDLE_STMT, "Failed to bind value parameter");
        }
    }
    
    ret = SQLExecute(stmt);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        if (!pstmt->expect_exec_error) {
            check_error(ret, stmt, SQL_HANDLE_STMT, "Failed to execute statement");
        }
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <user> <port> <database>\n", argv[0]);
        return 1;
    }
    
    char *user = argv[1];
    int port = atoi(argv[2]);
    char *db = argv[3];
    
    SQLHENV env;
    SQLHDBC dbc;
    SQLHSTMT stmt;
    SQLRETURN ret;
    
    // Allocate environment handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    check_error(ret, env, SQL_HANDLE_ENV, "Failed to allocate environment handle");
    
    // Set ODBC version
    ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
    check_error(ret, env, SQL_HANDLE_ENV, "Failed to set ODBC version");
    
    // Allocate connection handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    check_error(ret, env, SQL_HANDLE_ENV, "Failed to allocate connection handle");
    
    // Build connection string
    char connStr[512];
    snprintf(connStr, sizeof(connStr), 
             "DRIVER=MariaDB ODBC 3.2 Driver;SERVER=127.0.0.1;PORT=%d;DATABASE=%s;UID=%s;PWD=;",
             port, db, user);
    
    // Connect to database
    ret = SQLDriverConnect(dbc, NULL, (SQLCHAR *)connStr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    check_error(ret, dbc, SQL_HANDLE_DBC, "Failed to connect to database");
    
    printf("Connected to database successfully\n");
    
    // Execute test queries
    for (int i = 0; i < QUERIES_SIZE; i++) {
        ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
        check_error(ret, dbc, SQL_HANDLE_DBC, "Failed to allocate statement handle");
        
        ret = SQLExecDirect(stmt, (SQLCHAR *)queries[i], SQL_NTS);
        if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
            printf("QUERY FAILED: %s\n", queries[i]);
            check_error(ret, stmt, SQL_HANDLE_STMT, "Query execution failed");
        }
        
        // Fetch and discard results
        while (SQLFetch(stmt) == SQL_SUCCESS) {
            // Just consume the results
        }
        
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }
    
    // Test prepared statements
    prepared_statement statements[] = {
        {
            .query = "select * from test where pk = ?",
            .num_params = 1,
            .pk_param = 1,
            .expect_prepare_error = 0,
            .expect_exec_error = 0,
        },
        {
            .query = "insert into test values (?, ?)",
            .num_params = 2,
            .pk_param = 1,
            .value_param = 1,
            .expect_prepare_error = 0,
            .expect_exec_error = 0,
        },
        {
            .query = "select * from test SYNTAX ERROR where pk = ?",
            .num_params = 1,
            .pk_param = 1,
            .expect_prepare_error = 1,
            .expect_exec_error = 0,
        },
        {NULL, 0, 0, 0, 0, 0}, // Sentinel
    };
    
    for (int i = 0; statements[i].query; i++) {
        test_prepared_statement(dbc, &statements[i]);
    }
    
    printf("All tests passed\n");
    
    // Cleanup
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    
    return 0;
}


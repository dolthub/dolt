CC = gcc

CFLAGS_MYSQL = -I/usr/include/mysql -Wall -O2
CFLAGS_MARIADB = -I/usr/include/mariadb -Wall -O2
CFLAGS_ODBC = -Wall -O2

LDFLAGS_MYSQL = -lmysqlclient
LDFLAGS_MARIADB = -lmariadb
LDFLAGS_ODBC = -lodbc

TARGET_MYSQL = /build/bin/mysql-client-test
TARGET_MARIADB = /build/bin/mariadb-client-test
TARGET_ODBC = /build/bin/mariadb-odbc-test

SRCS_MYSQL = mysql-connector-test.c
SRCS_MARIADB = mariadb-connector-test.c
SRCS_ODBC = mariadb-odbc-test.c

all: $(TARGET_MYSQL) $(TARGET_MARIADB) $(TARGET_ODBC)

$(TARGET_MYSQL): $(SRCS_MYSQL)
	@mkdir -p /build/bin
	$(CC) $(CFLAGS_MYSQL) -o $@ $^ $(LDFLAGS_MYSQL)

$(TARGET_MARIADB): $(SRCS_MARIADB)
	@mkdir -p /build/bin
	$(CC) $(CFLAGS_MARIADB) -o $@ $^ $(LDFLAGS_MARIADB)

$(TARGET_ODBC): $(SRCS_ODBC)
	@mkdir -p /build/bin
	$(CC) $(CFLAGS_ODBC) -o $@ $^ $(LDFLAGS_ODBC)

clean:
	rm -f $(TARGET_MYSQL) $(TARGET_MARIADB) $(TARGET_ODBC)

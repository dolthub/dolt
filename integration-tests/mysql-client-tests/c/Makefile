CC = gcc

CFLAGS_MYSQL = -I/usr/include/mysql -Wall -O2
CFLAGS_MARIADB = -I/usr/include/mariadb -Wall -O2

LDFLAGS_MYSQL = -lmysqlclient
LDFLAGS_MARIADB = -lmariadb

TARGET_MYSQL = /build/bin/c-mysql-client-test
TARGET_MARIADB = /build/bin/c-mariadb-client-test

SRCS_MYSQL = mysql-connector-c-test.c
SRCS_MARIADB = mariadb-connector-c-test.c

all: $(TARGET_MYSQL) $(TARGET_MARIADB)

$(TARGET_MYSQL): $(SRCS_MYSQL)
	@mkdir -p /build/bin
	$(CC) $(CFLAGS_MYSQL) -o $@ $^ $(LDFLAGS_MYSQL)

$(TARGET_MARIADB): $(SRCS_MARIADB)
	@mkdir -p /build/bin
	$(CC) $(CFLAGS_MARIADB) -o $@ $^ $(LDFLAGS_MARIADB)

clean:
	rm -f $(TARGET_MYSQL) $(TARGET_MARIADB)

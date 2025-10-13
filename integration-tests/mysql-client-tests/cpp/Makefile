CXX = g++

CXXFLAGS_MYSQL = -I/usr/include/cppconn -std=c++11 -Wall -O2
CXXFLAGS_MARIADB = -I/usr/include/mariadb -std=c++11 -Wall -O2

LDFLAGS_MYSQL = -lmysqlcppconn
LDFLAGS_MARIADB = -lmariadbcpp

TARGET_MYSQL = /build/bin/mysql-connector-test
TARGET_MARIADB = /build/bin/mariadb-connector-test

SRCS_MYSQL = mysql-connector-test.cpp
SRCS_MARIADB = mariadb-connector-test.cpp

all: $(TARGET_MYSQL) $(TARGET_MARIADB)

$(TARGET_MYSQL): $(SRCS_MYSQL)
	@mkdir -p /build/bin
	$(CXX) $(CXXFLAGS_MYSQL) -o $@ $^ $(LDFLAGS_MYSQL)

$(TARGET_MARIADB): $(SRCS_MARIADB)
	@mkdir -p /build/bin
	$(CXX) $(CXXFLAGS_MARIADB) -o $@ $^ $(LDFLAGS_MARIADB)

clean:
	rm -f $(TARGET_MYSQL) $(TARGET_MARIADB)

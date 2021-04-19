MYSQL_CONCPP_DIR = /usr/local/Cellar/mysql-connector-c++/8.0.21
CPPFLAGS = -I $(MYSQL_CONCPP_DIR)/include -L $(MYSQL_CONCPP_DIR)/lib64
LDLIBS = -lmysqlcppconn8
CXX = clang++ -stdlib=libc++
CXXFLAGS = -std=c++11

all: mysql-connector-cpp-test

mysql-connector-cpp-test: mysql-connector-cpp-test.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o $@ $^ $(LDLIBS)

.PHONY: clean
clean:
	rm -f mysql-connector-cpp-test

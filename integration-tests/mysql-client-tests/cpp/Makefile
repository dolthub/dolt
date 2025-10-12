CXX = g++
CXXFLAGS = -I/usr/include/cppconn -std=c++11 -Wall -O2
LDFLAGS = -lmysqlcppconn
TARGET = /build/bin/cpp-mysql-connector-test
SRCS = mysql-connector-cpp-test.cpp

all: $(TARGET)

$(TARGET): $(SRCS)
	@mkdir -p /build/bin
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(TARGET)

CC = gcc

CFLAGS = -I/usr/include/mysql -Wall -O2

LDFLAGS = -lmysqlclient

TARGET = /build/bin/c-mysql-client-test

SRCS = mysql-connector-c-test.c

all: $(TARGET)

$(TARGET): $(SRCS)
	@mkdir -p /build/bin
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f /build/bin/c-mysql-client-test

CFLAGS := $(shell pkg-config --cflags mysqlclient)
LDFLAGS := $(shell pkg-config --libs mysqlclient)

all: mysql-connector-c-test

mysql-connector-c-test: mysql-connector-c-test.c
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

.PHONY: clean
clean:
	rm -f mysql-connector-c-test

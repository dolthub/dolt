CFLAGS := $(shell pkg-config --cflags mysqlclient)
LDFLAGS := $(shell pkg-config --libs mysqlclient)

.PHONY: clean
clean:
	rm -f mysql-connector-c-test

all: mysql-connector-c-test

mysql-connector-c-test: mysql-connector-c-test.c
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

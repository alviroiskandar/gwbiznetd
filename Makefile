
CC = gcc
CFLAGS = -Wall -Wextra -O2 -ggdb3
LDLIBS = -lcurl -lpthread

all: gwbiznetd

gwbiznetd: gwbiznetd.c

clean:
	rm -f gwbiznetd

.PHONY: all clean

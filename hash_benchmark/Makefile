
CFLAGS = -Wall -Wextra -Wpedantic

.PHONY: all
all: test
	./test

test: test.cpp ../hash/sha-256.c

.PHONY: clean
clean:
	rm test *.o

CFLAGS = -Wall -Wextra -Wpedantic
CCOMPILE=g++
CPPFLAGS=-std=c++11

.PHONY: all
all: test

test: test.cpp ../hash/sha-256.c ../hash/xxhash.o

.PHONY: clean
clean:
	rm test *.o
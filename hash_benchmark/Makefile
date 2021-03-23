
CFLAGS = -Wall -Wextra -Wpedantic
CCOMPILE=g++
CPPFLAGS=-std=c++11 -O3

.PHONY: all
all: test

test: test.cpp ../hash/sha-256.c ../hash/xxhash.o ../hash/md5.cpp ../hash/murmurhash.cc ../hash/Crc32.cpp ../hash/city.cc
	$(CCOMPILE) test.cpp ../hash/sha-256.c ../hash/xxhash.o ../hash/md5.cpp ../hash/murmurhash.cc ../hash/Crc32.cpp ../hash/city.cc $(CPPFLAGS) $(LDFLAGS) -o test

.PHONY: clean
clean:
	rm test 

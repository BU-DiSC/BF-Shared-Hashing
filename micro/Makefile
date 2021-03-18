
CCOMPILE=g++
CPPFLAGS=-std=c++11 -I
LDFLAGS= -Wno-deprecated
pf_LDFLAGS= -Wno-deprecated


all: micro1 micro2

micro1: micro1.cpp ../BF_bit.cpp ../hash/md5.cpp ../hash/murmurhash.cc ../hash/sha-256.c ../hash/xxhash.c ../hash/xxhash.o ../hash/Crc32.cpp
	$(CCOMPILE) micro1.cpp ../BF_bit.cpp ../hash/md5.cpp ../hash/murmurhash.cc ../hash/sha-256.c ../hash/xxhash.o ../hash/Crc32.cpp $(CPPFLAGS) $(LDFLAGS) -o micro1

micro2: micro2.cpp ../BF_bit.cpp ../hash/md5.cpp ../hash/murmurhash.cc ../hash/sha-256.c ../hash/xxhash.c ../hash/xxhash.o ../hash/Crc32.cpp
	$(CCOMPILE) micro2.cpp ../BF_bit.cpp ../hash/md5.cpp ../hash/murmurhash.cc ../hash/sha-256.c ../hash/xxhash.o ../hash/Crc32.cpp $(CPPFLAGS) $(LDFLAGS) -o micro2
clean:
	-rm micro1 micro2

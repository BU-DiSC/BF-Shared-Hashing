
CCOMPILE=g++
CPPFLAGS=-std=c++11 -I -msse4.2 -O3
LDFLAGS= -Wno-deprecated
pf_LDFLAGS= -Wno-deprecated 


all: micro1 micro2


micro1: micro1.cpp ../BF_bit.cpp ../hash/md5.cpp ../hash/murmurhash.cc ../hash/sha-256.c ../hash/xxhash.cc ../hash/xxhash.o ../hash/Crc32.cpp ../hash/city.cc
	$(CCOMPILE)  micro1.cpp ../BF_bit.cpp ../hash/md5.cpp ../hash/murmurhash.cc ../hash/sha-256.c ../hash/xxhash.o ../hash/Crc32.cpp ../hash/city.cc $(CPPFLAGS) $(LDFLAGS) -o micro1

micro2: micro2.cpp ../BF_bit.cpp ../hash/md5.cpp ../hash/murmurhash.cc ../hash/sha-256.c ../hash/xxhash.cc ../hash/xxhash.o ../hash/Crc32.cpp ../hash/city.cc
	$(CCOMPILE) -g micro2.cpp ../BF_bit.cpp ../hash/md5.cpp ../hash/murmurhash.cc ../hash/sha-256.c ../hash/xxhash.o ../hash/Crc32.cpp ../hash/city.cc $(CPPFLAGS) $(LDFLAGS) -o micro2
clean:
	-rm micro1 micro2

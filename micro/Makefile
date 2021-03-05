
CCOMPILE=g++
CPPFLAGS=-std=c++11 -I -g
LDFLAGS= -Wno-deprecated
pf_LDFLAGS= -Wno-deprecated -O3 -fprefetch-loop-arrays


all: micro

micro: main.cpp ../BF_bit.cpp ../hash/md5.cpp ../hash/murmurhash.cc ../hash/sha-256.c ../hash/xxhash.c ../hash/xxhash.o ../hash/Crc32.cpp
	$(CCOMPILE) -g main.cpp ../BF_bit.cpp ../hash/md5.cpp ../hash/murmurhash.cc ../hash/sha-256.c ../hash/xxhash.o ../hash/Crc32.cpp $(CPPFLAGS) $(LDFLAGS) -o micro

clean:
	-rm micro

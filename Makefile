
CCOMPILE=g++
CPPFLAGS=-std=c++11 -I -msse4.2 
LDFLAGS= -Wno-deprecated
#pf_LDFLAGS= -Wno-deprecated -O3 -fprefetch-loop-arrays
pf_LDFLAGS= -Wno-deprecated


all: lsm_emu

lsm_emu: BF_bit.cpp lsm-emulation.cpp options.cpp db.cpp hash/md5.cpp hash/murmurhash.cc hash/sha-256.c hash/xxhash.cc hash/xxhash.o hash/Crc32.cpp hash/city.cc
	$(CCOMPILE) lsm-emulation.cpp db.cpp BF_bit.cpp options.cpp hash/md5.cpp hash/murmurhash.cc hash/sha-256.c hash/xxhash.o hash/Crc32.cpp hash/city.cc $(CPPFLAGS) $(LDFLAGS) -o lsm-emu

clean:
	-rm lsm-emu

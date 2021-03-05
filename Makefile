
CCOMPILE=g++
CPPFLAGS=-std=c++11 -I -g
LDFLAGS= -Wno-deprecated
pf_LDFLAGS= -Wno-deprecated -O3 -fprefetch-loop-arrays


all: lsm_emu

lsm_emu: BF_bit.cpp lsm-emulation.cpp options.cpp db.cpp hash/md5.cpp hash/murmurhash.cc hash/sha-256.c hash/xxhash.c hash/xxhash.o
	$(CCOMPILE) -g lsm-emulation.cpp db.cpp BF_bit.cpp options.cpp hash/md5.cpp hash/murmurhash.cc hash/sha-256.c hash/xxhash.o $(CPPFLAGS) $(LDFLAGS) -o lsm-emu

clean:
	-rm lsm-emu

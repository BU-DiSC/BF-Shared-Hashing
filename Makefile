
CCOMPILE=g++
CPPFLAGS=-std=c++11 -I 
LDFLAGS= -Wno-deprecated -O2
#pf_LDFLAGS= -Wno-deprecated -O3 -fprefetch-loop-arrays
pf_LDFLAGS= -Wno-deprecated -Ofast


all: lsm_emu

lsm_emu: BF_bit.cpp lsm-emulation.cpp options.cpp db.cpp hash/md5.cpp hash/murmurhash.cc hash/sha-256.c hash/xxhash.c hash/xxhash.o
	$(CCOMPILE) lsm-emulation.cpp db.cpp BF_bit.cpp options.cpp hash/md5.cpp hash/murmurhash.cc hash/sha-256.c hash/xxhash.o $(CPPFLAGS) $(LDFLAGS) -o lsm-emu

clean:
	-rm lsm-emu

#pragma once
using namespace std;

#define WORD 8

enum HashType {
    MD5 = 0x0,
    SHA2 = 0x1,
    MurMurhash = 0x2,
    MurMur64 = 0x3,
    XXhash = 0x4,
    CRC = 0x5,
    CITY = 0x6,
    Default = 0x7
};

class BFHash {
public:
    static bool share_hash_across_levels_;
    static int share_hash_across_filter_units_; // 0 -> k, 1 -> 1, 2 -> 2
    static int num_filter_units_; 
    static int num_hash_indexes_;
    static bool reset;
    static HashType ht_;
    string key_;
    static vector<uint64_t>* hash_digests_;
    static vector<HashType> filter_unit_hash_funcs_;
    static vector<uint32_t> filter_unit_seeds_;
    BFHash(string & key){
        key_ = key;
        hash_digests_ = getLevelwiseHashDigest(-1);  
    } 
    static void prepareHashFuncs(HashType ht); 
    vector<uint64_t>* getLevelwiseHashDigest(int level);
    uint64_t getFilterUnitwiseHashDigest(int filter_unit_no);
    static uint64_t get_hash_digest(string & key, HashType ht, uint32_t seed);
};




unsigned int bf_mem_access( unsigned char* BF, int index );
void get_index( uint64_t hash_digest, int BF_index, int BF_size, int *index);



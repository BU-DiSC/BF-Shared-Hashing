#pragma once
using namespace std;

#define WORD 8

enum HashType {
    MD5 = 0x0,
    SHA2 = 0x1,
    MurMurhash = 0x2,
    XXhash = 0x3,
    CRC = 0x4,
    Default = 0x5
};

class BFHash {
public:
    static bool share_hash_across_levels_;
    static int share_hash_across_filter_units_; // 0 -> k, 1 -> 1, 2 -> 2
    static int num_filter_units_; 
    static int num_hash_indexes_;
    static HashType ht_;
    string key_;
    static vector<string> hash_digests_;
    static vector<HashType> filter_unit_hash_funcs_;
    static vector<uint32_t> filter_unit_seeds_;
    BFHash(string & key){
        key_ = key;
        getLevelwiseHashDigest(0, hash_digests_);  
    } 
    static void prepareHashFuncs(); 
    static fsec hash_duration;
    void getLevelwiseHashDigest(int level, vector<string> & hash_digests);
    string getFilterUnitwiseHashDigest(int filter_unit_no);
    static string get_hash_digest(string & key, HashType ht, uint32_t seed);
};




unsigned int bf_mem_access( vector<unsigned char> & BF, int index );
void get_index( string hash_digest, int BF_index, int BF_size, int *index);
void pgm_BF( string key, int level, int filter_unit_idx, int BF_size, int BF_index, vector<unsigned char>* BF );



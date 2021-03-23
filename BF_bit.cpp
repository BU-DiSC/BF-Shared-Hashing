using namespace std;

#include "stdafx.h"
#include "BF_bit.h"
#include "hash/md5.h"
#include "hash/murmurhash.h"
#include "hash/Crc32.h"
#include "hash/sha-256.h"
#include "hash/xxhash.h"
#include "hash/citycrc.h"
#include <functional>
#include <string>

bool BFHash::share_hash_across_levels_ = true;
int BFHash::share_hash_across_filter_units_ = 1;
int BFHash::num_filter_units_ = 2;
int BFHash::num_hash_indexes_ = 6;
bool BFHash::reset = true;
HashType BFHash::ht_ = MurMur64;
vector<HashType> BFHash::filter_unit_hash_funcs_ = vector<HashType> ();
vector<uint32_t> BFHash::filter_unit_seeds_ = vector<uint32_t> ();
vector<uint64_t>* BFHash::hash_digests_ = nullptr;

void BFHash:: prepareHashFuncs(HashType ht){
   ht_ = ht;
   filter_unit_hash_funcs_ = vector<HashType> (num_filter_units_,  ht);
   hash_digests_ = new vector<uint64_t> (num_filter_units_, 0);
   filter_unit_seeds_ = vector<uint32_t> (1, 0xbc9f1d34);
   uint32_t last_seed = 0xbc9f1d34;
   for(int i = 1; i < num_filter_units_; i++){
      last_seed = (last_seed << 17 | last_seed >> 15) + last_seed;
      filter_unit_seeds_.push_back(last_seed);
   }
}

uint64_t BFHash::getFilterUnitwiseHashDigest(int filter_unit_no){
   return get_hash_digest(key_, filter_unit_hash_funcs_[filter_unit_no], filter_unit_seeds_[filter_unit_no]);
}

void hash_to_string(char string[65], const uint8_t hash[32])
{
	size_t i;
	for (i = 0; i < 32; i++) {
		string += sprintf(string, "%02x", hash[i]);
	}
}
uint64_t BFHash::get_hash_digest(string & key, HashType ht, uint32_t seed){
    uint64_t result = 0;
    switch(ht){
        case MD5:
            memcpy(&result, md5(key).c_str(), sizeof(result));
            break;
        case SHA2:{
            uint8_t hash[32];
            const uint8_t * a = (const uint8_t*)key.c_str();
            calc_sha_256(hash, a, key.length());
	    for(int i = 0; i < 32; i++){
               result = (result << 1) + (hash[i]&0x1);
	    }
            break;
        }
            
        case MurMurhash: {
            result = MurmurHash2(key.c_str(), key.size(), seed);
            break;
	}
	case MurMur64: {
	    result = MurmurHash64A( key.c_str(), key.size(), seed);
	    break;
	}
        case XXhash:
        {
            // result = MurmurHash2(key.c_str(), key.size(), seed);
            XXH64_hash_t const p = seed;
            const void * key_void = key.c_str();
            XXH64_hash_t const h = XXH64(key_void, key.size(), p);
            result = h;
            break;
        }
        case CRC:{
            const void * key_void = key.c_str();
            result = crc32_fast( key_void, (unsigned long)key.size(), seed );
            break;
        }
        case CITY:{
            const char * key_void = key.c_str();
            result = CityHash64WithSeed( key_void, (unsigned long)key.size(), (unsigned long) seed);
	    break;
        }
	default:
            result = MurmurHash2(key.c_str(), key.size(), seed);
            break;
    }
    
    return result;
}

vector<uint64_t>* BFHash::getLevelwiseHashDigest(int level){
        if((!share_hash_across_levels_ && level != -1)|| hash_digests_->size() == 0 || reset){
	    if(hash_digests_ == nullptr){
		hash_digests_ = new vector<uint64_t>(num_filter_units_, 0);
	    }
	    if(share_hash_across_filter_units_ == 0){
                for(int i = 0; i < num_filter_units_; i++){
                    hash_digests_->at(i) = getFilterUnitwiseHashDigest(i);
                }
            }else if(share_hash_across_filter_units_ == 1){
                uint64_t last_hash_digest = getFilterUnitwiseHashDigest(0);
                hash_digests_->at(0) = last_hash_digest;
		uint64_t delta = last_hash_digest << 17 | last_hash_digest >> 15;
                for(int i = 1; i < num_filter_units_; i++){
                   last_hash_digest = last_hash_digest + delta; 
                   hash_digests_->at(i) = last_hash_digest;
                }
            }else{
                uint64_t hash_digest1 = getFilterUnitwiseHashDigest(0);
                if(num_filter_units_ == 1){
                   hash_digests_->at(0) = hash_digest1;
                }else{
		   HashType ht = filter_unit_hash_funcs_[0];
		   HashType ht2 = MD5;
                   for(auto t_ht: { MD5, MurMurhash, CRC, XXhash, MurMur64, SHA2}){
                      if(t_ht != ht){
                          ht2 = t_ht;
			  break;
                      }
                   }
                   uint32_t seed = filter_unit_seeds_[0];
	           uint64_t hash_digest2 = get_hash_digest(key_, ht2, seed + (seed << 17 | seed >> 15));
                   for(int i = 0; i < num_filter_units_; i++){
                       hash_digests_->at(i) = hash_digest1 + i*hash_digest2;
                   }
                }
                
            }
	    reset = false;
	}
	return hash_digests_;
}


void get_index( uint64_t hash_digest, int BF_index, int BF_size, int *index )
{

  uint64_t m_int_hash = hash_digest;
  index[0] = m_int_hash % BF_size;
  // get BF indices
  uint64_t delta = (m_int_hash << 17 | m_int_hash >> 15);
  for( uint32_t n=1 ; n<BF_index ; n++ ){
    m_int_hash = m_int_hash + delta; 
    index[n] =  m_int_hash % BF_size;
  }

  return;
}



unsigned int bf_mem_access(unsigned char* BF, int index )
{
	unsigned char mask[WORD] = {0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80};
	
	unsigned int refBit = BF[index/WORD] & mask[index%WORD];
	//printf("Q: %d %02hhx %02hhx\n", refBit, BF[index/WORD], mask[index%WORD]);

	return refBit;
}



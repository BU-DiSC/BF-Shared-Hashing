using namespace std;

#include "stdafx.h"
#include "BF_bit.h"
#include "hash/md5.h"
#include "hash/murmurhash.h"
#include "hash/Crc32.h"
#include "hash/sha-256.h"
#include "hash/xxhash.h"
#include <functional>
#include <string>

bool BFHash::share_hash_across_levels_ = true;
int BFHash::share_hash_across_filter_units_ = 1;
int BFHash::num_filter_units_ = 2;
int BFHash::num_hash_indexes_ = 6;
bool BFHash::reset = true;
HashType BFHash::ht_ = MD5;
fsec BFHash::hash_duration = std::chrono::microseconds::zero();
vector<HashType> BFHash::filter_unit_hash_funcs_ = vector<HashType> ();
vector<uint32_t> BFHash::filter_unit_seeds_ = vector<uint32_t> ();
vector<uint64_t> BFHash::hash_digests_ = vector<uint64_t> ();

void BFHash:: prepareHashFuncs(){
   filter_unit_hash_funcs_ = vector<HashType> (num_filter_units_, MurMurhash);
   hash_digests_ = vector<uint64_t> (num_filter_units_, 0);
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
    auto hash_start = high_resolution_clock::now();
    switch(ht){
        case MD5:
            memcpy(&result, md5(key).c_str(), sizeof(result));
            break;
        case SHA2:{
            uint8_t hash[32];
            const uint8_t * a = (const uint8_t*)key.c_str();
            calc_sha_256(hash, a, key.length());
	    result = ((hash[0] << 24) + (hash[1] << 16) + (hash[2] << 8) + hash[3]) << 32 + (hash[4] << 24) + (hash[5] << 16) + (hash[6] << 8) + hash[7];
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
            printf("hash '%s': %d \n", key.c_str(), h);
            break;
        }
        case CRC:{
            const void * key_void = key.c_str();
            result = crc32_fast( key_void, (unsigned long)key.size(), seed );
            break;
        }
	default:
            result = MurmurHash2(key.c_str(), key.size(), seed);
            break;
    }
    
    auto hash_end   = high_resolution_clock::now();
    hash_duration += duration_cast<microseconds>(hash_end - hash_start);	
    return result;
}

void BFHash::getLevelwiseHashDigest(int level, vector<uint64_t> & hash_digests){
        if(!share_hash_across_levels_ || hash_digests_.size() == 0 || reset){
	    if(share_hash_across_filter_units_ == 0){
                for(int i = 0; i < num_filter_units_; i++){
                    hash_digests[i] = getFilterUnitwiseHashDigest(i);
                }
            }else if(share_hash_across_filter_units_ == 1){
                hash_digests[0] = getFilterUnitwiseHashDigest(0);
                uint64_t last_hash_digest = hash_digests[0];
                for(int i = 1; i < num_filter_units_; i++){
                   last_hash_digest = last_hash_digest + (last_hash_digest << 17 | last_hash_digest >> 15); 
                   hash_digests[i] = last_hash_digest;
                }
            }else{
                uint64_t hash_digest1 = getFilterUnitwiseHashDigest(0);
                if(num_filter_units_ == 1){
                   hash_digests[0] = hash_digest1;
                }else{
		   HashType ht = filter_unit_hash_funcs_[0];
		   HashType ht2 = MD5;
                   for(auto ht_: { MD5, MurMurhash, CRC, XXhash, SHA2}){
                      if(ht_ != ht){
                          ht2 = ht_;
                      }
                   }
                   uint32_t seed = filter_unit_seeds_[0];
	           uint64_t hash_digest2 = get_hash_digest(key_, ht2, seed + (seed << 17 | seed >> 15));
                   for(int i = 0; i < num_filter_units_; i++){
                       hash_digests[i] = hash_digest1 + i*hash_digest2;
                   }
                }
                
            }
            hash_digests_ = hash_digests;
	    reset = false;
	}else{
            hash_digests = hash_digests_;
        }
}


void get_index( uint64_t hash_digest, int BF_index, int BF_size, int *index )
{

  uint64_t m_int_hash = hash_digest;
  index[0] = m_int_hash % BF_size;
  // get BF indices
  for( uint32_t n=1 ; n<BF_index ; n++ ){
    m_int_hash = m_int_hash + (m_int_hash << 17 | m_int_hash >> 15); 
    index[n] =  m_int_hash % BF_size;
  }

  return;
}


void pgm_BF( string key, int level, int filter_unit_idx, int BF_size, int BF_index, unsigned char* BF )
{
	constexpr unsigned char mask[WORD] = {0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80};

	int * ind_dec;
	ind_dec = (int * )malloc( BF_index  * sizeof(int));

    cout << "check" << endl;
	if ( key.size() == 0)
		return;

	BFHash bfHash (key);	
    cout << "check" << endl;
	vector<uint64_t> hash_digests = vector<uint64_t> (BFHash::num_filter_units_, 0);
	bfHash.getLevelwiseHashDigest(level, hash_digests);

	get_index(hash_digests[filter_unit_idx], BF_index, BF_size, ind_dec );

	for(int j=0; j<BF_index; j++)
	{
		unsigned int ind_byte = ind_dec[j]/WORD;
		unsigned char ref = BF[ind_byte];
		BF[ind_byte] = ref | mask[ind_dec[j]%WORD];
		//printf("P: %d %02hhx %02hhx\n", ref, mask[ind_dec[j]%WORD], BF->at(ind_byte) );
	}

	free(ind_dec);
	return;
}


unsigned int bf_mem_access(unsigned char* BF, int index )
{
	unsigned char mask[WORD] = {0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80};

	unsigned int refBit = BF[index/WORD] & mask[index%WORD];
	//printf("Q: %d %02hhx %02hhx\n", refBit, BF[index/WORD], mask[index%WORD]);

	return refBit;
}



using namespace std;

#include "stdafx.h"
#include "BF_bit.h"
#include "hash/md5.h"
#include "hash/murmurhash.h"
//#include "hash/crc32c.h"
#include <functional>
#include <string>

bool BFHash::share_hash_across_levels_ = true;
int BFHash::share_hash_across_filter_units_ = 1;
int BFHash::num_filter_units_ = 2;
int BFHash::num_hash_indexes_ = 6;
HashType BFHash::ht_ = MurMurhash;
fsec BFHash::hash_duration = std::chrono::microseconds::zero();
vector<HashType> BFHash::filter_unit_hash_funcs_ = vector<HashType> ();
vector<uint32_t> BFHash::filter_unit_seeds_ = vector<uint32_t> ();
vector<string> BFHash::hash_digests_ = vector<string> ();

void BFHash:: prepareHashFuncs(){
   filter_unit_hash_funcs_ = vector<HashType> (num_filter_units_, MurMurhash);
   hash_digests_ = vector<string> (num_filter_units_, "");
   filter_unit_seeds_ = vector<uint32_t> (1, 0xbc9f1d34);
   uint32_t last_seed = 0xbc9f1d34;
   for(int i = 1; i < num_filter_units_; i++){
      last_seed = (last_seed << 17 | last_seed >> 15) + last_seed;
      filter_unit_seeds_.push_back(last_seed);
   }
}

string BFHash::getFilterUnitwiseHashDigest(int filter_unit_no){
   return get_hash_digest(key_, filter_unit_hash_funcs_[filter_unit_no], filter_unit_seeds_[filter_unit_no]);
}

string BFHash::get_hash_digest(string & key, HashType ht, uint32_t seed){
    char buffer[32];
    memset(buffer, 0, 32);
    string result = key;
    auto hash_start = high_resolution_clock::now();
    switch(ht){
        case MD5:
            result = md5(key);
            break;
        case SHA2:
            result = md5(key);
            break;
        case MurMurhash:
            sprintf(buffer, "%x",MurmurHash2(key.c_str(), key.size(), seed));
            result = buffer;
            break;
        case XXhash:
            result = md5(key);
            break;
        case CRC:
            //sprintf(buffer, "%x",rocksdb::crc32c::Value(key.c_str(),key.size())); 
	    //result = buffer;
            result = md5(key);
            break;
	default:
	    std::hash<std::string> dft_hash;
	    result = dft_hash(key);
            break;
    }
    
    auto hash_end   = high_resolution_clock::now();
    hash_duration += duration_cast<microseconds>(hash_end - hash_start);	
    return result;
}

void BFHash::getLevelwiseHashDigest(int level, vector<string> & hash_digests){
        if(!share_hash_across_levels_ || hash_digests_.size() == 0 || hash_digests_[0].compare("") == 0){
	    if(share_hash_across_filter_units_ == 0){
                for(int i = 0; i < num_filter_units_; i++){
                    hash_digests.push_back(getFilterUnitwiseHashDigest(i));
                }
            }else if(share_hash_across_filter_units_ == 1){
                hash_digests.push_back(getFilterUnitwiseHashDigest(0));
                uint32_t last_hash_digest = stoul(hash_digests_[0], NULL, 16);
                for(int i = 1; i < num_filter_units_; i++){
                   last_hash_digest = last_hash_digest + (last_hash_digest << 17 | last_hash_digest >> 15); 
                   hash_digests.push_back(to_string(last_hash_digest));
                }
            }else{
                string hash_digest1 = getFilterUnitwiseHashDigest(0);
                uint32_t hash_digest_number_1 = stoul(hash_digest1, NULL, 16);
                if(num_filter_units_ == 1){
                   hash_digests.push_back(hash_digest1);
                }else{
		   HashType ht = filter_unit_hash_funcs_[0];
		   HashType ht2 = MD5;
                   for(auto ht_: { MD5, MurMurhash, CRC, XXhash, SHA2}){
                      if(ht_ != ht){
                          ht2 = ht_;
                      }
                   }
                   uint32_t seed = filter_unit_seeds_[0];
	           string hash_digest2 = get_hash_digest(key_, ht2, seed + (seed << 17 | seed >> 15));
                   uint32_t hash_digest_number_2 = stoul(hash_digest2, NULL, 16);
                   for(int i = 0; i < num_filter_units_; i++){
                       hash_digests.push_back(to_string(hash_digest_number_1 + i*hash_digest_number_2)); 
                   }
                }
                
            }
            hash_digests_ = hash_digests;
	}else{
            hash_digests = hash_digests_;
        }
}


void get_index( string hash_digest, int BF_index, int BF_size, int *index )
{

  uint64_t m_int_hash = stoul ( hash_digest, NULL, 16);
  index[0] = m_int_hash % BF_size;
  // get BF indices
  for( uint32_t n=1 ; n<BF_index ; n++ ){
    m_int_hash = m_int_hash + (m_int_hash << 17 | m_int_hash >> 15); 
    index[n] =  m_int_hash % BF_size;
  }

  return;
}


void pgm_BF( string key, int level, int filter_unit_idx, int BF_size, int BF_index, vector<unsigned char>* BF )
{
	constexpr unsigned char mask[WORD] = {0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80};

	int * ind_dec;
	ind_dec = (int * )malloc( BF_index  * sizeof(int));

	if ( key.size() == 0)
		return;

	BFHash bfHash (key);	
	vector<string> hash_digests;
	bfHash.getLevelwiseHashDigest(level, hash_digests);

	get_index(hash_digests[filter_unit_idx], BF_index, BF_size, ind_dec );

	for(int j=0; j<BF_index; j++)
	{
		unsigned int ind_byte = ind_dec[j]/WORD;
		unsigned char ref = BF->at( ind_byte );
		BF->at( ind_byte ) = ref | mask[ind_dec[j]%WORD];
		//printf("P: %d %02hhx %02hhx\n", ref, mask[ind_dec[j]%WORD], BF->at(ind_byte) );
	}

	free(ind_dec);
	return;
}


unsigned int bf_mem_access(vector<unsigned char> & BF, int index )
{
	unsigned char mask[WORD] = {0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80};

	unsigned int refBit = BF[index/WORD] & mask[index%WORD];
	//printf("Q: %d %02hhx %02hhx\n", refBit, BF[index/WORD], mask[index%WORD]);

	return refBit;
}



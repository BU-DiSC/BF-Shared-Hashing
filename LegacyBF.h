#include "bloom_impl.h"
#include "hash/rocksdb_hash.h"
#include <memory>
#include <array>
#include <cstring>
#include <cassert>
#include <deque>

#define CACHE_LINE_SIZE 64U

inline void EncodeFixed32(char* buf, uint32_t value) {
    memcpy(buf, &value, sizeof(value));
  
}



using LegacyBloomImpl = LegacyLocalityBloomImpl</*ExtraRotates*/ false>;

class LegacyBF {
 public:
  int bits_per_key_;
  int num_probes_;
  int num_lines_;
  int space_;
  std::vector<uint32_t> hash_entries_;
  char* data_; 
  explicit LegacyBF(const int bits_per_key){
      bits_per_key_ = bits_per_key;
      num_probes_ = LegacyNoLocalityBloomImpl::ChooseNumProbes(bits_per_key_); 
  }

  void AddKey(string & key, uint64_t hash) ;

  void Finish() ;

  int CalculateNumEntry(const uint32_t bytes);

  uint32_t CalculateSpace(const int num_entry) {
    uint32_t dont_care1;
    uint32_t dont_care2;
    return CalculateSpace(num_entry, &dont_care1, &dont_care2);
  }

  double EstimatedFpRate(size_t keys, size_t bytes) {
    return LegacyBloomImpl::EstimatedFpRate(keys, bytes - /*metadata*/ 5,
                                            num_probes_);
  }


  // Get totalbits that optimized for cpu cache line
  uint32_t GetTotalBitsForLocality(uint32_t total_bits);


  // Reserve space for new filter
  char* ReserveSpace(const int num_entry, uint32_t* total_bits,
                     uint32_t* num_lines);

  // Implementation-specific variant of public CalculateSpace
  uint32_t CalculateSpace(const int num_entry, uint32_t* total_bits,
                          uint32_t* num_lines);

  // Assuming single threaded access to this function.
  void AddHash(uint32_t h, char* data, uint32_t num_lines, uint32_t total_bits);

  static bool MayMatch(uint64_t hash_digest, int num_lines, int num_probes, char* data) {  // originally BloomHash is used, to make it compatible with sharing hashing, change it with GetSliceHash64
    uint32_t hash = Lower32of64(hash_digest);
    //std::cout << Lower32of64(hash_digest) << std::endl;
    //std::cout << hash << std::endl;
    uint32_t byte_offset;
    LegacyBloomImpl::PrepareHashMayMatch(
        hash, num_lines, data, /*out*/ &byte_offset, 6);
    return LegacyBloomImpl::HashMayMatchPrepared(
        hash, num_probes, data + byte_offset, 6);
  }


};

void LegacyBF::AddKey(string & key, uint64_t hash) { // originally BloomHash is used, to make it compatible with sharing hashing, change it with GetSliceHash64
  if (hash_entries_.size() == 0 || hash != hash_entries_.back()) {
    hash_entries_.push_back(hash);
  }
}

void LegacyBF::Finish() {
  uint32_t total_bits, num_lines;
  size_t num_entries = hash_entries_.size();
  char* data =
      ReserveSpace(static_cast<int>(num_entries), &total_bits, &num_lines);
  data_ = data;
  assert(data);
  if (total_bits != 0 && num_lines != 0) {
    for (auto h : hash_entries_) {
      AddHash(h, data, num_lines, total_bits);
    }

    // Check for excessive entries for 32-bit hash function
    if (num_entries >= /* minimum of 3 million */ 3000000U) {
      // More specifically, we can detect that the 32-bit hash function
      // is causing significant increase in FP rate by comparing current
      // estimated FP rate to what we would get with a normal number of
      // keys at same memory ratio.
      double est_fp_rate = LegacyBloomImpl::EstimatedFpRate(
          num_entries, total_bits / 8, num_probes_);
      double vs_fp_rate = LegacyBloomImpl::EstimatedFpRate(
          1U << 16, (1U << 16) * bits_per_key_ / 8, num_probes_);

      if (est_fp_rate >= 1.50 * vs_fp_rate) {
        // For more details, see
        // https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter
        printf(
            "Using legacy SST/BBT Bloom filter with excessive key count "
            "(%.1fM @ %dbpk), causing estimated %.1fx higher filter FP rate. "
            "Consider using new Bloom with format_version>=5, smaller SST "
            "file size, or partitioned filters.",
            num_entries / 1000000.0, bits_per_key_, est_fp_rate / vs_fp_rate);
      }
    }
  }
  // See BloomFilterPolicy::GetFilterBitsReader for metadata
  data[total_bits / 8] = static_cast<char>(num_probes_);
  EncodeFixed32(data + total_bits / 8 + 1, static_cast<uint32_t>(num_lines));
  space_ = (total_bits >> 3) + 4;


  hash_entries_.clear();

}

uint32_t LegacyBF::GetTotalBitsForLocality(uint32_t total_bits) {
  uint32_t num_lines =
      (total_bits + CACHE_LINE_SIZE * 8 - 1) / (CACHE_LINE_SIZE * 8);

  // Make num_lines an odd number to make sure more bits are involved
  // when determining which block.
  if (num_lines % 2 == 0) {
    num_lines++;
  }
  return num_lines * (CACHE_LINE_SIZE * 8);
}

uint32_t LegacyBF::CalculateSpace(const int num_entry,
                                                uint32_t* total_bits,
                                                uint32_t* num_lines) {
  assert(bits_per_key_);
  if (num_entry != 0) {
    uint32_t total_bits_tmp = static_cast<uint32_t>(num_entry * bits_per_key_);

    *total_bits = GetTotalBitsForLocality(total_bits_tmp);
    *num_lines = *total_bits / (CACHE_LINE_SIZE * 8);
    num_lines_ = *num_lines;
    assert(*total_bits > 0 && *total_bits % 8 == 0);
  } else {
    // filter is empty, just leave space for metadata
    *total_bits = 0;
    *num_lines = 0;
  }

  // Reserve space for Filter
  uint32_t sz = *total_bits / 8;
  sz += 5;  // 4 bytes for num_lines, 1 byte for num_probes
  return sz;
}

char* LegacyBF::ReserveSpace(const int num_entry,
                                           uint32_t* total_bits,
                                           uint32_t* num_lines) {
  uint32_t sz = CalculateSpace(num_entry, total_bits, num_lines);
  char* data = new char[sz];
  memset(data, 0, sz);
  return data;
}

int LegacyBF::CalculateNumEntry(const uint32_t bytes) {
  assert(bits_per_key_);
  assert(bytes > 0);
  int high = static_cast<int>(bytes * 8 / bits_per_key_ + 1);
  int low = 1;
  int n = high;
  for (; n >= low; n--) {
    if (CalculateSpace(n) <= bytes) {
      break;
    }
  }
  assert(n < high);  // High should be an overestimation
  return n;
}

inline void LegacyBF::AddHash(uint32_t h, char* data,
                                            uint32_t num_lines,
                                            uint32_t total_bits) {
#ifdef NDEBUG
  static_cast<void>(total_bits);
#endif
  assert(num_lines > 0 && total_bits > 0);

  LegacyBloomImpl::AddHash(h, num_lines, num_probes_, data,
                           6);
}



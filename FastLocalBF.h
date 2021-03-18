#include "bloom_impl.h"
#include "hash/rocksdb_hash.h"
#include <memory>
#include <iostream>
#include <array>
#include <cstring>
#include <cassert>
#include <deque>


class FastLocalBF {

public:
  int millibits_per_key_;
  int num_probes_;
  int num_lines_;
  char* data_;
  uint32_t space_;
  // A deque avoids unnecessary copying of already-saved values
  // and has near-minimal peak memory use.
  std::deque<uint64_t> hash_entries_;

  explicit FastLocalBF(const int bits_per_key){
    millibits_per_key_ = bits_per_key*1000;
    num_probes_ = FastLocalBloomImpl::ChooseNumProbes(millibits_per_key_);
    assert(millibits_per_key_ >= 1000);
  }

  ~FastLocalBF(){};

  void AddKey(const string & key, uint64_t hash) {
    hash_entries_.push_back(hash);
  }

   void Finish()  {
    uint32_t len_with_metadata =
        CalculateSpace(static_cast<uint32_t>(hash_entries_.size()));
    space_ = len_with_metadata;
    char* data = new char[len_with_metadata];
    data_ = data;
    memset(data, 0, len_with_metadata);

    assert(data);
    assert(len_with_metadata >= 5);

    uint32_t len = len_with_metadata - 5;
    if (len > 0) {
      AddAllEntries(data, len);
    }

    // See BloomFilterPolicy::GetBloomBitsReader re: metadata
    // -1 = Marker for newer Bloom implementations
    data[len] = static_cast<char>(-1);
    // 0 = Marker for this sub-implementation
    data[len + 1] = static_cast<char>(0);
    // num_probes (and 0 in upper bits for 64-byte block size)
    data[len + 2] = static_cast<char>(num_probes_);
    // rest of metadata stays zero

    hash_entries_.clear();
    assert(hash_entries_.empty());

  }

  int CalculateNumEntry(const uint32_t bytes) {
    uint32_t bytes_no_meta = bytes >= 5u ? bytes - 5u : 0;
    return static_cast<int>(uint64_t{8000} * bytes_no_meta /
                            millibits_per_key_);
  }

  uint32_t CalculateSpace(const int num_entry)  {
    uint32_t num_cache_lines = 0;
    if (millibits_per_key_ > 0 && num_entry > 0) {
      num_cache_lines = static_cast<uint32_t>(
          (int64_t{num_entry} * millibits_per_key_ + 511999) / 512000);
    }
    return num_cache_lines * 64 + /*metadata*/ 5;
  }

  double EstimatedFpRate(size_t keys, size_t bytes) {
    return FastLocalBloomImpl::EstimatedFpRate(keys, bytes - /*metadata*/ 5,
                                               num_probes_, /*hash bits*/ 64);
  }
  static bool MayMatch(uint64_t h, int len_bytes, int  num_probes, const char* data) {
    
    uint32_t byte_offset;
    FastLocalBloomImpl::PrepareHash(Lower32of64(h), len_bytes, data,
                                    /*out*/ &byte_offset);
    //if(h%10 == 0) cout << byte_offset << endl;
    return FastLocalBloomImpl::HashMayMatchPrepared(Upper32of64(h), num_probes,
                                                    data + byte_offset);
  }
   
  private:
  void AddAllEntries(char* data, uint32_t len) {
    // Simple version without prefetching:
    //
    // for (auto h : hash_entries_) {
    //   FastLocalBloomImpl::AddHash(Lower32of64(h), Upper32of64(h), len,
    //                               num_probes_, data);
    // }

    const size_t num_entries = hash_entries_.size();
    constexpr size_t kBufferMask = 7;
    static_assert(((kBufferMask + 1) & kBufferMask) == 0,
                  "Must be power of 2 minus 1");

    std::array<uint32_t, kBufferMask + 1> hashes;
    std::array<uint32_t, kBufferMask + 1> byte_offsets;

    // Prime the buffer
    size_t i = 0;
    for (; i <= kBufferMask && i < num_entries; ++i) {
      uint64_t h = hash_entries_.front();
      hash_entries_.pop_front();
      FastLocalBloomImpl::PrepareHash(Lower32of64(h), len, data,
                                      /*out*/ &byte_offsets[i]);
	//cout << byte_offsets[i] << endl;
      hashes[i] = Upper32of64(h);
    }

    // Process and buffer
    for (; i < num_entries; ++i) {
      uint32_t& hash_ref = hashes[i & kBufferMask];
      uint32_t& byte_offset_ref = byte_offsets[i & kBufferMask];
      // Process (add)
      FastLocalBloomImpl::AddHashPrepared(hash_ref, num_probes_,
                                          data + byte_offset_ref);
      // And buffer
      uint64_t h = hash_entries_.front();
      hash_entries_.pop_front();
      FastLocalBloomImpl::PrepareHash(Lower32of64(h), len, data,
                                      /*out*/ &byte_offset_ref);
	//cout << byte_offset_ref << endl;
      hash_ref = Upper32of64(h);
    }

    // Finish processing
    for (i = 0; i <= kBufferMask && i < num_entries; ++i) {
      FastLocalBloomImpl::AddHashPrepared(hashes[i], num_probes_,
                                          data + byte_offsets[i]);
    }
  }

  

};

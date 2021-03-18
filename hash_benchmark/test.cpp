#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <algorithm>
#include <iostream>
// #include "../stdafx.h"
#include "../hash/sha-256.h"
#include "../hash/xxhash.h"
#include "../hash/md5.h"
#include "../hash/murmurhash.h"
#include "../hash/Crc32.h"
#include "../hash/city.h"

#include <functional>
#include <string>
#include <chrono>

using namespace std;

inline uint64_t get_sha_hash(const char input[])
{
    uint8_t hash[32];
    uint64_t result = 0;
    calc_sha_256(hash, input, strlen(input));

    result = ((hash[0] << 24) + (hash[1] << 16) + (hash[2] << 8) + hash[3]) << 32 + (hash[4] << 24) + (hash[5] << 16) + (hash[6] << 8) + hash[7];

    return result;
}

char *mkrndstr(size_t length)
{ // const size_t length, supra

    static char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,.-#'?!"; // could be const
    char *randomString;

    if (length)
    {
        randomString = (char *)malloc(length + 1); // sizeof(char) == 1, cf. C99

        if (randomString)
        {
            int l = (int)(sizeof(charset) - 1); // (static/global, could be const or #define SZ, would be even better)
            int key;                            // one-time instantiation (static/global would be even better)
            for (int n = 0; n < length; n++)
            {
                key = rand() % l; // no instantiation, just assignment, no overhead from sizeof
                randomString[n] = charset[key];
            }

            randomString[length] = '\0';
        }
    }

    return randomString;
}

inline XXH64_hash_t get_xx_hash(string key, uint32_t seed = 1)
{
    XXH64_hash_t const p = seed;
    const void *key_void = key.c_str();
    XXH64_hash_t const h = XXH64(key_void, key.size(), p);

    return h;
}

inline uint64_t get_md5_hash(string key)
{
    uint64_t result = 0;
    memcpy(&result, md5(key).c_str(), sizeof(result));

    return result;
}

inline uint64_t get_murmur_hash(string key, uint32_t seed = 1)
{
    return MurmurHash2(key.c_str(), key.size(), seed);
}

uint64_t get_murmur64_hash(string key, uint32_t seed = 1)
{
    return MurmurHash64A(key.c_str(), key.size(), seed);
}

inline uint64_t get_crc_hash(string key, uint32_t seed = 1)
{
    const void *key_void = key.c_str();
    return crc32_fast(key_void, (unsigned long)key.size(), seed);
}

inline uint64_t get_city64_hash(string  key){
	return CityHash64(key.c_str(),key.size());
}

int main(int argc, char *argv[])
{
    if (argc < 4)
    {
        std::cout << "usage: ./test rand_key_size num_tries hash_type" << endl;
        std::cout << "hash_type can be any one of XXHASH, SHA2, MD5, MURMUR, MURMUR64, CRC" << endl;
        return 0;
    }

    int l = atoi(argv[1]);
    int nops = atoi(argv[2]);

    string hash_type = argv[3];
    unsigned long long total_sha_time = 0;

    char *s = mkrndstr(l);
        auto hash_start = chrono::high_resolution_clock::now();
    for (int i = 0; i < nops; i++)
    {

        if (hash_type == "SHA2")
        {
            uint64_t hash_i = get_sha_hash(s);
        }
        else if (hash_type == "XXHASH")
        {
            XXH64_hash_t hash_i = get_xx_hash(std::string(s));
        }
        else if (hash_type == "MD5")
        {
            uint64_t hash_i = get_md5_hash(s);
        }
        else if (hash_type == "MURMUR")
        {
            uint64_t hash_i = get_murmur_hash(s);
        }
        else if (hash_type == "MURMUR64")
        {
            uint64_t hash_i = get_murmur64_hash(s);
        }
        else if (hash_type == "CRC")
        {
            uint64_t hash_i = get_crc_hash(s);
        }
	else if (hash_type == "CITY")
        {
            uint64_t hash_i = get_city64_hash(std::string(s));
        }
        else
        {
            std::cout << "Invalid hash_type. hash_type can be either XXHASH or SHA2" << endl;
            return 0;
        }

    }

        auto hash_end = chrono::high_resolution_clock::now();
        total_sha_time += chrono::duration_cast<chrono::microseconds>(hash_end - hash_start).count();
    std::cout << "Average time taken for hashing a key of " << l << " bytes = " << (total_sha_time / (double)nops) << " microseconds over " << nops << " tries with " << hash_type << endl;

    return 0;
}

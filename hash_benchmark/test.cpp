#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <algorithm>
#include <iostream>
#include "../hash/sha-256.h"

using namespace std;


uint64_t get_hash(const char input[])
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
        randomString = (char*)malloc(length + 1); // sizeof(char) == 1, cf. C99

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

int main(void)
{
    // size_t i;
    // for (i = 0; i < (sizeof STRING_VECTORS / sizeof (struct string_vector)); i++) {
    // 	const struct string_vector *vector = &STRING_VECTORS[0];
    // 	if (string_test(vector->input, vector->output))
    // 		return 1;
    // }

    int nops = 100;
    unsigned long long total_sha_time = 0;
    int l = 1024;
    char *s = mkrndstr(l);
    cout<<s<<endl;
    cout<<sizeof(s)<<endl;
    for (int i = 0; i < nops; i++)
    {
        uint64_t hash = get_hash(s);
    }

    return 0;
}
<H1> LSM-Tree Emulation </H1>


This repository contains an LSM emulator that was used to run the experiments for our latest work: ["Reducing Bloom Filter CPU Overhead in LSM-Trees on Modern Storage Devices"](http://cs-people.bu.edu/mathan/publications/damon21-zhu.pdf). 

In this work, we observe that as we move to faster storage devices, hashing for BFs in LSM-trees becomes a key performance bottleneck. We address this by decoupling the hashing overhead from the number of distinct levels in the tree (and, as a result, the data size), by sharing a single hash digest across different levels. Our technique reduces the fraction of time spent on hashing during lookups and leads to performance benefits varying from 10% for our PCIe SSD to more than 40% for an emulated NVM. T

---

Run `make` and you can execute lsm-emu which receives parameters as follows:

  OPTIONS:

      This group is all exclusive:
        -l[log], --log=[log]              path for log files
        -p[path], --path=[path]           path for writing the DB and all the
                                          metadata files
        -i[insert_workload],
        --insert_workload=[insert_workload]
                                          path for insert workload files
        -q[query_workload],
        --query_workload=[query_workload] path for query workload files
        -T[T], --size_ratio=[T]           The size ratio of two adjacent levels
                                          [def: 2]
        -P[P],
        --buffer_size_in_pageas=[P]       The number of pages that can fit into
                                          a buffer [def: 1024]
        -B[B], --entries_per_page=[B]     The number of entries that fit into a
                                          page [def: 128]
        -E[E], --entry_size=[E]           The size of a key-value pair inserted
                                          into DB [def: 64 B]
        -K[K], --key_size=[K]             The size of a key inserted into DB
                                          [def: 16 B]
        -b[bits_per_key],
        --bits_per_key=[bits_per_key]     The number of bits per key assigned to
                                          Bloom filter [def: 10]
        --elastic,
        --enable_elastic_filters          Enable elastic filters.
        --FLBF,
        --enable_fast_local_filters       Enable FastLocal Bloom filters. [def:
                                          false]
        --HT=[Hash Type],
        --hash_type=[Hash Type]           Hash type MM64, XXHash, CRC or CITY
                                          [def: MM64]
        --num_funit=[num_funit],
        --num_filter_units=[num_funit]    The number of filter units for elastic
                                          filter. [def: 2]
        --lvl_share_hash,
        --enable_leveled_shared_hashing   Enable sharing hash across levels
        --funits_share_hash,
        --enable_filter_units_shared_hashing
                                          Enable sharing hash across filter
                                          units
        --dd, --destroy_db                Delete the exsiting DB.
        --tries=[number_of_tries]         #Tries to run the experiment (measured
                                          statistics would be averaged across
                                          #tries [def: 5]
        -D[delay], --delay=[delay]        Read delay (nanos) added by human[def:
                                          0]
        --DIO, --direct_IO                Enable direct IO (DO NOT Enable this
                                          using RAM disk!)

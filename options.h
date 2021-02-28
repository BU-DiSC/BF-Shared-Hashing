#pragma once
using namespace std;

#include <string>

class options {
public:
  // path info
  string db_path;
  bool destroy_db;

  string out_path;
  string insert_path;
  string query_path;

  // basic LSM setting
  int size_ratio;
  int buffer_size_in_pages;
  int entries_per_page;
  int entry_size;
  int key_size;

  // BF related info
  int bits_per_key;
  bool elastic_filters;
  int num_filterunits;

  // share hash
  bool share_hash_across_levels;
  bool share_hash_across_filter_units;

  int parse(int argc, char *argv[]);
};

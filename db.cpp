using namespace std;

#include "db.h"
#include "FastLocalBF.h"
#include "LegacyBF.h"
#include <math.h>
#include <iostream>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <linux/fs.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/ioctl.h>

#define DB_PAGE_SIZE 4096
#define BYTE 8


db::db( options op )
{
    string db_path = op.db_path;
    out_path = op.out_path;

	bf_dir    = db_path + "/bf_home/";
	index_dir = db_path + "/index/";
	data_dir  = db_path + "/data/";

	settings_file = db_path + "settings.txt";
	fence_file = db_path + "fence.txt";

	string bf_command = "mkdir -p " + bf_dir;
	string data_command = "mkdir -p " + data_dir;
	string index_command = "mkdir -p " + index_dir;
	system(bf_command.c_str());
	system(data_command.c_str());
	system(index_command.c_str());
	cout << bf_command << endl;
	cout << data_command << endl;
	cout << index_command << endl;

    // basic LSM setting
    P = op.buffer_size_in_pages;
    B = op.entries_per_page;
    E = op.entry_size;
	K = op.key_size;
    size_ratio = op.size_ratio;
    fastlocal_bf = op.fastlocal_bf;

    buffer_size = P * B;

	index_size = P*K;
	data_size = B*E;

	// BF related
    bpk = op.bits_per_key;
    mod_bf = op.elastic_filters;
    num_filter_units = op.num_filterunits;
    hash_digests = vector<uint64_t> (num_filter_units, 0);
    BFHash::hash_digests_ = new vector<uint64_t> (num_filter_units, 0);
	//experiment
    tries = op.tries;
    delay = op.delay;
    file_read_flags = O_RDWR | O_SYNC;
    if(op.directIO){
        file_read_flags = O_RDWR | O_DIRECT | O_SYNC;

    }

    int bf_size = (buffer_size * bpk);
	int bf_index = (int)floor(0.693*bpk + 0.5);
    filter_unit_size = bf_size/num_filter_units;
	filter_unit_byte = ceil(float(filter_unit_size)/BYTE);
    filter_unit_index = bf_index/num_filter_units;

    BFHash::num_hash_indexes_ = filter_unit_index;
    BFHash::num_filter_units_ = num_filter_units;
    BFHash::share_hash_across_levels_ = op.share_hash_across_levels;
    if(!op.share_hash_across_levels){
        BFHash::reset = false;
    }
    BFHash::share_hash_across_filter_units_ = op.share_hash_across_filter_units;
    if(op.hash_type.compare("XXHash") == 0){
       BFHash::prepareHashFuncs(XXhash);
    }else if(op.hash_type.compare("CITY") == 0){
       BFHash::prepareHashFuncs(CITY);
    }else if(op.hash_type.compare("CRC") == 0){
       BFHash::prepareHashFuncs(CRC);
    }else{
       BFHash::prepareHashFuncs(MurMur64);
    }
}

int db::ReadSettings()
{
	ifstream infile;
	infile.open( settings_file );
	 
	if( infile.fail() ){
		cout << "Error opening " << settings_file << endl;
	}

	bool bf_rebuild = false;

	int num = 0;
	while( infile.peek() != EOF ){
		//int line = 0;
		string line;
		getline( infile, line);
		int value = stoi(line);
		//cout << num << " " << value << endl;
		if ( num==0 ) {
			if (value != P)
				return -1;
		} else if ( num==1 ){
			if (value != B)
				return -1;
		} else if ( num==2 ){
				if (value != E)
					return -1;
		} else if ( num==3 ){
			if (value != K)
				return -1;
		} else if ( num==4 ){
			if (value != num_filter_units){
				bf_rebuild = true;
			}
		
		} else if ( num==5 ){
			if ( value > 0 ){
				num_levels = value;
				num_sstPerLevel.resize( num_levels, 0);
				fence_pointers.resize( num_levels );
			}
			else {
				return -1;
			}
		} else if ( num==6 ){
			if( value == 0){
				return -1;
			}
			last_sst_keys = value;
		} else if ( num>=7 && num<7+num_levels ){
			num_sstPerLevel[num-7] = value;
			num_sst += value;
		}
		num++;
	}
	infile.close();

	if ( num < 7+num_levels )
		return -1;

	if (bf_rebuild==true){
		return 1;
	}

	//cout << fence_file << endl;
	infile.open( fence_file );
	if( infile.fail() ){
		cout << "Error opening " << fence_file << endl;
	}
	for ( int i=0 ; i<num_levels ; i++ ){
		vector <string> fence_pointer_one_level ( num_sstPerLevel[i]+1 );
		for ( int j=0 ; j<=num_sstPerLevel[i] ; j++ ){
			string first, last;
			getline( infile, first );
			fence_pointer_one_level[j] = first;
		}
		fence_pointers[i] = fence_pointer_one_level;
	}

	return 0;
}

void db::Build( vector<vector<vector<string> > > reallocated_keys, bool bf_only )
{
	cout << "DB Build started." << endl;

	string index_command = "rm -rf " + index_dir+"/*";
	string data_command = "rm -rf " + data_dir+"/*";
	string bf_command = "rm -rf " + bf_dir+"/*";
	system(bf_command.c_str());
	if ( bf_only==false ){
		system(data_command.c_str());
		system(index_command.c_str());
	}
	
	fp_vec = vector<uint64_t> (num_levels, 0);
	n_vec = vector<uint64_t> (num_levels, 0);
	ofstream file_settings (settings_file, ios::out);
	file_settings << P << endl;
	file_settings << B << endl;
	file_settings << E << endl;
	file_settings << K << endl;
	file_settings << num_filter_units << endl;
	hash_digests = vector<uint64_t> (num_filter_units, 0);
	file_settings << num_levels << endl;
	file_settings << last_sst_keys << endl;

	ofstream file_fence(fence_file, ios::out);
	for ( int i=0 ; i<num_levels ; i++ ){
                int len = fence_pointers[i].size();
		for ( int j=0 ; j<len ; j++ ){
			file_fence << fence_pointers[i][j] << endl;
		}
	}
	file_fence.close();
	if(fastlocal_bf){
		FastLocalBF tmpbf = FastLocalBF(bpk/num_filter_units);
		filter_size = tmpbf.CalculateSpace(B*P);
		last_filter_size = tmpbf.CalculateSpace(last_sst_keys);
		num_probes = tmpbf.num_probes_;
	}else{
		LegacyBF tmpbf = LegacyBF(bpk/num_filter_units);
		uint32_t dont_care;
		filter_size = tmpbf.CalculateSpace(B*P, &dont_care, &num_lines);
		last_filter_size = tmpbf.CalculateSpace(last_sst_keys, &dont_care, &last_num_lines);
		num_probes = tmpbf.num_probes_;

	}
	
	
	bf_prime = (char****) malloc  ( num_levels * sizeof(char***));
	blk_fp_prime = (char****) malloc  ( num_levels * sizeof(char***));
	blk_size_prime = (int**) malloc (num_levels*sizeof(int*));
	cout << "total lv " << num_levels << endl;
	for( int l = 0; l < num_levels; l++){
		cout << "lv " << l << endl;
		vector<vector<string> > keys_one_level = reallocated_keys[l];
		int num_sst = keys_one_level.size();
		bf_prime[l] = (char***) malloc (num_sst*sizeof(char**));
		blk_fp_prime[l] = (char***) malloc (num_sst*sizeof(char**));
		blk_size_prime[l] = (int*) malloc (num_sst*sizeof(int));
		file_settings << num_sst << endl;
		for(int sst_index = 0; sst_index < num_sst; sst_index++){
			vector<string> keys_one_sst = keys_one_level[sst_index];
			char** sst_bf = (char**) malloc (num_filter_units*sizeof(char*));
			string sst_bf_prefix = bf_dir + "level_" + to_string(l) + "-sst_" + to_string(sst_index);



			for(int blo = 0; blo < num_filter_units; blo++){
				string sst_bf_filename = sst_bf_prefix + "_" + to_string(blo) +".txt";
				//cout << sst_bf_filename << endl;
				int end = keys_one_sst.size();
				if(fastlocal_bf){
					FastLocalBF bf = FastLocalBF(bpk/num_filter_units);
					for( int i=0 ; i <end ; i++){
					
						BFHash bfHash (keys_one_sst[i]);	
						vector<uint64_t>* hash_digests = bfHash.getLevelwiseHashDigest(l);
						BFHash::reset = true;
						bf.AddKey(keys_one_sst[i], hash_digests->at(blo));
					}
					bf.Finish();
					flushBFfile(sst_bf_filename, bf.data_, bf.space_);
					char* blo_bf = ( char*) malloc (bf.space_*sizeof(char));
					memcpy(blo_bf, bf.data_, bf.space_);
					sst_bf[blo] = blo_bf;
				}else{
					LegacyBF bf = LegacyBF(bpk/num_filter_units);
					for( int i=0 ; i <end ; i++){
					
						BFHash bfHash (keys_one_sst[i]);	
						vector<uint64_t>* hash_digests = bfHash.getLevelwiseHashDigest(l);
						BFHash::reset = true;
						bf.AddKey(keys_one_sst[i], hash_digests->at(blo));
					}
					bf.Finish();
					flushBFfile(sst_bf_filename, bf.data_, bf.space_);
					char* blo_bf = ( char*) malloc (bf.space_*sizeof(char));
					memcpy(blo_bf, bf.data_, bf.space_);
					sst_bf[blo] = blo_bf;
				}
				//FastLocalBF bf (bpk/num_filter_units*1000);
				
			}
			bf_prime[l][sst_index] = sst_bf;

			// fence pointers
			
			string sst_index_filename = index_dir + "level_" + to_string(l) + "-sst_" + to_string(sst_index) + ".txt";
			// building index
			char** index_one_sst = (char**) malloc (ceil(keys_one_sst.size()/B)*sizeof(char*));
			int j = 0;
			for( int i=0; i<keys_one_sst.size() ; i+=B ){
				index_one_sst[j] = (char *) malloc ((K+1)*sizeof(char));
				strcpy(index_one_sst[j], keys_one_sst[i].c_str());
				j++;
			}
			blk_fp_prime[l][sst_index] = index_one_sst;
			blk_size_prime[l][sst_index] = j;
			flushIndexes(sst_index_filename, index_one_sst, j, K);
			string sst_data_filename = data_dir + "level_" + to_string(l) + "-sst_" + to_string(sst_index) + ".txt";
			for( int i=0 ; i<keys_one_sst.size() ; i++ ){
				keys_one_sst[i].resize(E, '0');
			}
			flushfile(sst_data_filename, &keys_one_sst);
		
		}
	}

	file_settings.close();

	return;
}

string db::Get( string key, bool * result )
{
	std::chrono::time_point<std::chrono::high_resolution_clock>  total_end;
	std::chrono::time_point<std::chrono::high_resolution_clock>  total_start = high_resolution_clock::now();

	string value;
	BFHash bfHash(key);
	//for ( int i=0 ; i<num_levels ; i++ ){
	int i = 0;

	while(true){

		value = GetLevel( i, bfHash, key, result );
		if ( *result == true ){
			num_lookups++;
			BFHash::reset = true;
 			total_end = high_resolution_clock::now();
			total_duration += duration_cast<microseconds>(total_end - total_start);
			return value;
		}
		i++;
		if(i >= num_levels){
			break;
		}

	}
	*result = false;
	BFHash::reset = true;

	num_lookups++;
 	total_end = high_resolution_clock::now();
	total_duration += duration_cast<microseconds>(total_end - total_start);
	
	return "";
}

inline string db::GetLevel( int i, BFHash & bfHash, string & key, bool * result )
{
	// Binary search for fense pointer
	//auto bs_start = high_resolution_clock::now();
	int bf_no = binary_search(key, fence_pointers[i]);
	//auto bs_end = high_resolution_clock::now();
	//bs_duration += duration_cast<microseconds>(bs_end - bs_start);	

 		//auto other_start = high_resolution_clock::now();

	// no matching sst
	if(bf_no < 0){
		*result = false;
 		//auto other_end = high_resolution_clock::now();
		//other_duration += duration_cast<microseconds>(other_end - other_start);
		return "";
	}
	//auto hash_start = high_resolution_clock::now();
	vector<uint64_t>* hash_digests = bfHash.getLevelwiseHashDigest(i);
	//auto hash_end = high_resolution_clock::now();
	//hash_duration += duration_cast<microseconds>(hash_end - hash_start);	


	bool bf_result = QueryFilter( i, bf_no, hash_digests, bf_prime[i][bf_no]);
		
	//cout << "QueryFilter result : " << i << " " << bf_no << " " << bf_result << endl;

	if( bf_result==false ){
		total_n++;
		//n_vec[i]++;
		*result = false;
 		//auto other_end = high_resolution_clock::now();
		//other_duration += duration_cast<microseconds>(other_end - other_start);
		return "";
	}

	int index_pos = GetFromIndex( i, bf_no, key );

	bool data_result = false;
	string data = GetFromData( i, bf_no, index_pos, key, &data_result );

	// false positive
	if (data_result == false){
		total_fp++;
		//fp_vec[i]++;
		total_n++;
		//n_vec[i]++;
	}else{

		total_p++;
	}
	

	*result = data_result;
 		//auto other_end = high_resolution_clock::now();
		//other_duration += duration_cast<microseconds>(other_end - other_start);
	return data;
}

inline bool db::QueryFilter( int i, int bf_no, vector<uint64_t>* hash_digests, char** bf_list)
{
	bool result = true;
	int tmp_filter_size = filter_size;
	int tmp_num_lines = num_lines;
/*	
	if(i == num_levels - 1 && bf_no == num_sstPerLevel[i]-1){
		tmp_filter_size = last_filter_size;
	}
*/	
	if(i == num_levels - 1 && bf_no == num_sstPerLevel[i]-1){
		tmp_num_lines = last_num_lines;
	}


	if(fastlocal_bf){
		for(int blo = 0; blo < num_filter_units; blo++){
			if(!FastLocalBF::MayMatch(hash_digests->at(blo), tmp_filter_size, num_probes, bf_list[blo])) return false;
		}
	}else{
		for(int blo = 0; blo < num_filter_units; blo++){
			if(!LegacyBF::MayMatch(hash_digests->at(blo), tmp_num_lines, num_probes, bf_list[blo])) return false;
		}
	}
	

	return result;
}


inline int db::GetFromIndex( int l, int bf_no, string key )
{
	char** index = blk_fp_prime[l][bf_no];

	int index_pos = binary_search(key, index, blk_size_prime[l][bf_no]);
	//cout << key << " " << l << " " << index_pos << endl;
	return index_pos;
}

inline string db::GetFromData( int i, int bf_no, int index_pos, string key, bool * result )
{
	vector <string> data_block;
	vector <string> filter;
	string sst_data_filename = data_dir + "level_" + to_string(i) + "-sst_" + to_string(bf_no) + ".txt";

	
	//auto data_start = high_resolution_clock::now();
	read_data( sst_data_filename, index_pos, key.size(), data_block );
	//auto data_end = high_resolution_clock::now();
	//data_duration += duration_cast<microseconds>(data_end - data_start);	
	int found = -1;
	for ( int k=0 ; k<data_block.size() ; k++){
		string data_entry = data_block[k].substr( 0, key.size() );
		if(data_entry.compare(key) == 0){
			found = k;
		}
	}
	if ( found == -1 ){
		*result = false;
		return "";
	}
	else {
		*result = true;
		return key; 
	}
}

inline int db::read_data ( string filename, int pos, int key_size, vector<string> & data_block )
{
	//int flags = O_RDWR | O_DIRECT | O_SYNC;
	mode_t mode=S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;//644

	int fd = open(filename.c_str(), file_read_flags, mode );
	if (fd <= 0) {
    	printf("Error %s\n", strerror(errno));
		cout << "Cannot open partiton file " << filename << endl;
		return 0;
	}
      
	data_block.clear();
	data_block.resize(B, "");

	char* buf;
	int offset = pos*B*E;


	auto data_start = high_resolution_clock::now();
	posix_memalign((void**)&buf,getpagesize(),DB_PAGE_SIZE);
	memset(buf, 0, DB_PAGE_SIZE);


	//read( fd, buf, DB_PAGE_SIZE );
	//auto data_end   = high_resolution_clock::now();
	//data_duration += duration_cast<microseconds>(data_end - data_start);

      
    // Moving pointer
	lseek( fd, DB_PAGE_SIZE+offset, SEEK_SET );

	memset(buf, 0, DB_PAGE_SIZE);
	//data_start = high_resolution_clock::now();
	read( fd, buf, DB_PAGE_SIZE );
	auto data_end   = high_resolution_clock::now();
	data_duration += duration_cast<microseconds>(data_end - data_start);

	//int key_size;
	int entry_size = E;
	char* tmp_buf = (char*) malloc(entry_size+1);
	offset = 0;
	tmp_buf[entry_size] = '\0';
	for(int i = 0; i < B; i++){
		if(offset + key_size >= DB_PAGE_SIZE){
			unsigned int inner_key_offset = 0;
			if(offset < DB_PAGE_SIZE){
				memcpy(tmp_buf, buf+offset,DB_PAGE_SIZE-offset);
				inner_key_offset = DB_PAGE_SIZE-offset;
			}
		
			 data_start = high_resolution_clock::now();
			read( fd, buf, DB_PAGE_SIZE );
			data_end   = high_resolution_clock::now();
			data_duration += duration_cast<microseconds>(data_end - data_start);

			offset = key_size - inner_key_offset;	
			memcpy(tmp_buf+inner_key_offset, buf, offset);
		}else{

			memcpy(tmp_buf, buf+offset, E);
			offset += E;
		}

		data_block[i] = string(tmp_buf);

		memset(tmp_buf, 0, key_size);
	}
	free(tmp_buf);

	free( buf );
	close( fd );

        if(delay > 0){
			 data_start = high_resolution_clock::now();
	    struct timespec req = {0}; 
            req.tv_sec = 0;
	    req.tv_nsec = delay;
	    nanosleep(&req, (struct timespec *)NULL);
			data_end   = high_resolution_clock::now();
			data_duration += duration_cast<microseconds>(data_end - data_start);
	}
		
	return 0;
}

inline int db::binary_search(string key, char** indexes, int size)
{
	int len = size;
	if(len == 0){return -1;}
	if(len == 1){return 0;}
	int start = 0;
	int end = len - 1;
	int mid;

	//for (int i=0 ; i<len ; i++){
	//	cout << fence_pointer[i] << endl;;
	//}

	if(strcmp(key.c_str(), indexes[start]) < 0){ return -1;}
	if(strcmp(key.c_str(), indexes[end]) > 0){ return -1;}

	while(end - start > 1){
		mid = (start + end)/2;
		if(strcmp(key.c_str(), indexes[mid])< 0){
			end = mid;
		} 
		else if(strcmp(key.c_str(), indexes[mid])  == 0){
			return mid;
		}
		else{
			start = mid;
		}
	}
	return start;
}



int db::binary_search(string key, vector<string> & fence_pointer)
{
	int len = fence_pointer.size();
	if(len == 0){return -1;}
	if(len == 1){return 0;}
	int start = 0;
	int end = len - 1;
	int mid;

	//for (int i=0 ; i<len ; i++){
	//	cout << fence_pointer[i] << endl;;
	//}

	if(key.compare(fence_pointer[start]) < 0){ return -1;}
	if(key.compare(fence_pointer[end]) > 0){ return -1;}

	while(end - start > 1){
		mid = (start + end)/2;
		if(key.compare(fence_pointer[mid]) < 0){
			end = mid;
		} 
		else if(key.compare(fence_pointer[mid]) == 0){
			return mid;
		}
		else{
			start = mid;
		}
	}
	return start;
}

void db::split_keys( vector<string> table_in, vector<vector<vector<string> > > & reallocated_keys )
{
	// calculate how many keys assigned in each level
	int num_keys = table_in.size();
	int level = 1;
	vector<int> bf_num_keys;
	if(num_keys < buffer_size){
		bf_num_keys.push_back(num_keys);	
	}else{
		num_sst = ceil((float)num_keys/(float)buffer_size);
		int tmp_num_keys = num_keys;
		level = (int) ceil(log(num_keys*1.0*(size_ratio - 1)/buffer_size + 1)/log(size_ratio));
		bf_num_keys = vector<int> (level, 0);
		// build the tree bottom-to-up
		for(int i = level - 1; i >= 1; i--){
			int sst_previous_levels = pow(size_ratio, i)/(size_ratio-1);
			int capacity_previous_levels = buffer_size * sst_previous_levels;
			if(capacity_previous_levels > tmp_num_keys) continue;
			int num_keys_curr_level = tmp_num_keys - capacity_previous_levels;
			bf_num_keys[i] = num_keys_curr_level;
			tmp_num_keys -= num_keys_curr_level;
		}
		bf_num_keys[0] = tmp_num_keys;
		
		int sum_keys = 0;
		for(int i = 0; i < level; i++){
			sum_keys += bf_num_keys[i];
		}
	}
	num_levels = level;
	num_sstPerLevel.resize( num_levels, 0);

	// allocate keys in each level
	vector<vector<string> > reversed_leveled_keys; // store the keys in each level reversely
	int start_index = 0;
	for(int i = level - 1; i >= 0; i--){
		int end_index = bf_num_keys[i] + start_index;
		if(end_index >= num_keys){
			reversed_leveled_keys.push_back(vector<string> (table_in.begin() + start_index, table_in.end()));
		}else{
			reversed_leveled_keys.push_back(vector<string> (table_in.begin() + start_index, table_in.begin() + end_index));
		}
		start_index = end_index;
	}	
	// split keys into equal-size sstables
	reallocated_keys.clear();
	fence_pointers.clear();
	for(int i = level - 1; i >= 0; i--){
		vector<vector<string> > sst_keys_one_level;
		vector<string> fence_pointer_one_level;
		split_keys_sst(buffer_size, reversed_leveled_keys[i], sst_keys_one_level, fence_pointer_one_level);

		num_sstPerLevel[(level-1)-i] = sst_keys_one_level.size();
		reallocated_keys.push_back(sst_keys_one_level);
		fence_pointers.push_back(fence_pointer_one_level);
		if(i == level - 1){
			last_sst_keys = sst_keys_one_level.back().size();
		}
	}
	SetTreeParam();
}

fsec db::loadBFAndIndex(){
	fp_vec = vector<uint64_t> (num_levels, 0);
	n_vec = vector<uint64_t> (num_levels, 0);
	bf_prime = (char****) malloc  ( num_levels * sizeof(char***));
	blk_fp_prime = (char****) malloc  ( num_levels * sizeof(char***));
	blk_size_prime = (int**) malloc (num_levels*sizeof(int*));
	if(fastlocal_bf){
		FastLocalBF bf = FastLocalBF(bpk/num_filter_units);
		filter_size = bf.CalculateSpace(B*P);
        	num_probes = bf.num_probes_;
	}else{
		LegacyBF bf = LegacyBF(bpk/num_filter_units);
		uint32_t dont_care;
		filter_size = bf.CalculateSpace(B*P, &dont_care, &num_lines);
        	num_probes = bf.num_probes_;
	}
	int tmp_filter_size = filter_size;
	auto l_start = high_resolution_clock::now();
	auto l_end = high_resolution_clock::now();
	fsec load_duration = std::chrono::microseconds::zero();
	for(int i = 0; i < num_levels; i++){
		bf_prime[i] = (char***) malloc (num_sstPerLevel[i]*sizeof(char**));
		blk_fp_prime[i] = (char***) malloc (num_sstPerLevel[i]*sizeof(char**));
		blk_size_prime[i] = (int*) malloc (num_sstPerLevel[i]*sizeof(int));

		for(int j = 0; j < num_sstPerLevel[i]; j++){
			string sst_index_filename = index_dir + "level_" + to_string(i) + "-sst_" + to_string(j) + ".txt";
			l_start = high_resolution_clock::now();
			int index_size = read_index_size(sst_index_filename);	
			l_end = high_resolution_clock::now();
			load_duration += duration_cast<microseconds>(l_end - l_start);

			char** index = (char**) malloc((index_size+1)*sizeof(char*));
			for(int k = 0; k <= index_size; k++){
				index[k] = (char*) malloc ((K+1)*sizeof(char));
			}
			l_start = high_resolution_clock::now();
			read_index(sst_index_filename, index_size, index);
			l_end = high_resolution_clock::now();
			load_duration += duration_cast<microseconds>(l_end - l_start);

			blk_fp_prime[i][j] = index;
			blk_size_prime[i][j] = index_size;

			if(i == num_levels - 1 && j == num_sstPerLevel[i] - 1){
				if(fastlocal_bf){
					FastLocalBF bf = FastLocalBF(bpk/num_filter_units);
					last_filter_size = bf.CalculateSpace(last_sst_keys);
					last_num_lines = bf.num_lines_;
				}else{
					LegacyBF bf = LegacyBF(bpk/num_filter_units);
					uint32_t dont_care;
					last_filter_size = bf.CalculateSpace(last_sst_keys, &dont_care, &last_num_lines);
					last_num_lines = bf.num_lines_;

				}
				tmp_filter_size = last_filter_size;
			} 
			
			char** sst_bf = (char**) malloc (num_filter_units*sizeof(char*));

			for(int k = 0; k < num_filter_units; k++){
				string sst_bf_filename = bf_dir + "level_" + to_string(i) + "-sst_" + to_string(j) + "_" + to_string(k) + ".txt";
				char* blo_bf = (char*) malloc (tmp_filter_size*sizeof(char));
				l_start = high_resolution_clock::now();
				read_bf(sst_bf_filename, blo_bf, tmp_filter_size);
				l_end = high_resolution_clock::now();
				load_duration += duration_cast<microseconds>(l_end - l_start);
				sst_bf[k] = blo_bf;
			}
			bf_prime[i][j] = sst_bf;
		}
	}
	return load_duration;
}

void db::SetTreeParam()
{
	positive_counters.resize(num_levels, 0);
	qnum_histogram = new vector<int> (num_filter_units, 0); 
	lnum_histogram = new vector<int> (num_levels, 0); 

	for( int l = 0; l < num_levels; l++){
		int num_l_sst = num_sstPerLevel[l];
		cout << "set param " << num_levels << " " << num_l_sst << endl;
	}

	bf_tp_eval_histogram = new vector<int> (num_levels, 0);
}


void db::split_keys_sst(int sst_capacity, vector<string> keys_one_level, vector<vector<string> > & sst, vector<string> & fence_pointer )
{
	sst.clear();
	fence_pointer.clear();
	if(keys_one_level.size() == 0) return;
	sort(keys_one_level.begin(), keys_one_level.end());
	int start_index = 0;
	int num_keys_one_level = keys_one_level.size();
	while(true){
		int end_index = start_index + sst_capacity;
		fence_pointer.push_back(keys_one_level[start_index]);
		if(end_index >= num_keys_one_level){
			sst.push_back(vector<string> (keys_one_level.cbegin() + start_index, keys_one_level.cend()));
			break;
		}else{
			sst.push_back(vector<string> (keys_one_level.cbegin() + start_index, keys_one_level.cbegin() + end_index));
		}
		start_index = end_index;
	}	
	fence_pointer.push_back(keys_one_level.back());	
}

void db::flushBFfile(string filename, const char* sst_bf_p, uint32_t size){
	ofstream outfile(filename, ios::out);
	char buffer[sizeof(unsigned int)];
	memset(buffer, 0, sizeof(unsigned int));
	
	for(int i = 0; i < size; i++){
		memcpy(buffer, &(sst_bf_p[i]), sizeof(char));
		outfile.write(buffer, sizeof(char));
		memset(buffer, 0, sizeof(char));
	}	
	outfile.close();

}
void db::flushIndexes( string filename, char** indexes, int size, int key_size){
	ofstream outfile(filename, ios::out);
	char buffer[DB_PAGE_SIZE];

	memcpy(buffer, &size, sizeof(unsigned int));
	outfile.write(buffer, sizeof(unsigned int));
	memset(buffer, 0, sizeof(unsigned int));

	memcpy(buffer, &key_size, sizeof(unsigned int));
	outfile.write(buffer, sizeof(unsigned int));
	memset(buffer, 0, sizeof(unsigned int));

	memset(buffer, 0, DB_PAGE_SIZE-2*sizeof(unsigned int));
	outfile.write(buffer, DB_PAGE_SIZE-2*sizeof(unsigned int));

	for(int i = 0 ;i < size; i++){
		memcpy(buffer, indexes[i], key_size);
		outfile.write(buffer, key_size);
		memset(buffer, 0, key_size);
	}
}
void db::flushfile( string filename, vector<string>* table){
	ofstream outfile(filename, ios::out);
	char buffer[DB_PAGE_SIZE];

	int size = table->size();
	memcpy(buffer, &size, sizeof(unsigned int));
	outfile.write(buffer, sizeof(unsigned int));
	memset(buffer, 0, sizeof(unsigned int));

	int key_size = (table->at(0)).size();
	memcpy(buffer, &key_size, sizeof(unsigned int));
	outfile.write(buffer, sizeof(unsigned int));
	memset(buffer, 0, sizeof(unsigned int));

	memset(buffer, 0, DB_PAGE_SIZE-2*sizeof(unsigned int));
	outfile.write(buffer, DB_PAGE_SIZE-2*sizeof(unsigned int));

	for(int i = 0 ;i < size; i++){
		char* str = (char*) malloc(key_size);
		strcpy (str, table->at(i).c_str());
		memcpy(buffer, str, key_size);
		outfile.write(buffer, key_size);
		memset(buffer, 0, key_size);
	}
}


void db::PrintStat(double program_total, double data_load_total)
{
	// log file
	string out_path_cmd = "mkdir -p " + out_path;
	system(out_path_cmd.c_str());
	string file_result = out_path + "result.txt";
	ofstream result_file(file_result);
	result_file << "Total false positives:\t" << total_fp << endl;
	result_file << "Total negatives:\t" << total_n << endl;
	result_file << "FPR:\t" << (double) total_fp/total_n << endl;
	double total = total_duration.count()/tries;
	result_file << "Total query time:\t" << total  << endl;
	total -= data_duration.count()/tries;
	result_file << "Data access time:\t" << data_duration.count()/tries << endl;
	result_file << "Program total time:\t" << program_total/tries << endl;
	result_file << "Data input time:\t" << data_load_total/tries << endl;
	total -= hash_duration.count()/tries;
	result_file << "Hash time:\t" << hash_duration.count()/tries << endl;
	//total -= bs_duration.count()/tries;
	//result_file << "BS time:\t" << bs_duration.count()/tries << endl;
	result_file << "Other time:\t" << total << endl;
	//cout << "Other2 time:\t" << other2_duration.count()/tries << endl;
	
	result_file << endl;


	/*
	for( int i=0 ; i<num_filter_units; i++ ){
		result_file << "qnum " << i << " : " << qnum_histogram->at(i) << endl;
		cout << "qnum " << i << " : " << qnum_histogram->at(i) << endl;
	}*/

	result_file.close();

	return;
}

inline void db::read_bf(string filename, char* bf_buffer, int size){
	//int flags = O_RDWR | O_DIRECT;
	int flags = O_RDWR;
	mode_t mode=S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;//644

	
	int fd_ = open(filename.c_str(), flags, mode );
	if (fd_ <= 0) {
		cout << "Cannot open partiton file " << filename << endl;
    	printf("Error %s\n", strerror(errno));
	}
	
	char* buffer_;	
	posix_memalign((void**)&buffer_,DB_PAGE_SIZE,DB_PAGE_SIZE);
	memset(buffer_, 0, DB_PAGE_SIZE);

	for(int i = 0; i < size; i++){
		if ( i%DB_PAGE_SIZE == 0){
			memset(buffer_, 0, DB_PAGE_SIZE);
			read( fd_, buffer_, DB_PAGE_SIZE );
		}
		memcpy(bf_buffer+i, buffer_+(i%DB_PAGE_SIZE), sizeof(char));
	}
	//cout << "string " << blo_bf.size() << endl;
	//for(int i = 0; i < blo_bf.size(); i++){
	//	cout << hex << (unsigned int)blo_bf[i] << endl;
	//}

	if(fd_ > 0)
		close( fd_ );	
}

inline int db::read_index_size( string filename ){
	//int flags = O_RDWR | O_DIRECT | O_SYNC;
	int flags = O_RDWR;
	mode_t mode=S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;//644

	int fd = open(filename.c_str(), flags, mode );
	if (fd <= 0) {
		cout << "Cannot open partiton file " << filename << endl;
    	printf("Error %s\n", strerror(errno));
		return 0;
	}

	char* buf;
	posix_memalign((void**)&buf,getpagesize(),DB_PAGE_SIZE);

	memset(buf, 0, DB_PAGE_SIZE);
	read( fd, buf, DB_PAGE_SIZE );
	
	int size;
	int key_size;
	memcpy(&size, buf, sizeof(unsigned int));	
	index_size = size;
	memcpy(&key_size, buf+sizeof(unsigned int), sizeof(unsigned int));	
	free( buf );
	if(fd > 0)
		close( fd );	
	return size;
}

inline void db::read_index( string filename, int size, char** index)
{
	//int flags = O_RDWR | O_DIRECT | O_SYNC;
	int flags = O_RDWR | O_SYNC;
	mode_t mode=S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;//644

	int fd = open(filename.c_str(), flags, mode );
	if (fd <= 0) {
		cout << "Cannot open partiton file " << filename << endl;
    	printf("Error %s\n", strerror(errno));
		return;
	}

	char* buf;
	posix_memalign((void**)&buf,getpagesize(),DB_PAGE_SIZE);

	memset(buf, 0, DB_PAGE_SIZE);
	read( fd, buf, DB_PAGE_SIZE );
	
	int key_size;
	memcpy(&key_size, buf+sizeof(unsigned int), sizeof(unsigned int));	

	int offset = 0;

	memset(buf, 0, DB_PAGE_SIZE);
	read( fd, buf, DB_PAGE_SIZE );

	for(int i = 0; i < size; i++){
		if(offset + key_size >= DB_PAGE_SIZE){
			unsigned int inner_key_offset = 0;
			if(offset < DB_PAGE_SIZE){
				memcpy(index[i], buf+offset,DB_PAGE_SIZE-offset);
				inner_key_offset = DB_PAGE_SIZE-offset;
			}
		
			read( fd, buf, DB_PAGE_SIZE );

			offset = key_size - inner_key_offset;	
			memcpy(index[i]+inner_key_offset, buf, offset);
		}else{
			memcpy(index[i], buf+offset, key_size);
			offset += key_size;
		}
		index[i][key_size] = '\0';
		
	}
	memset(index[size], 'z', key_size);
	index[size][key_size] = '\0';

	free( buf );
	close( fd );
	return;
}

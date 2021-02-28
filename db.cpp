using namespace std;

#include "db.h"
#include <math.h>

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

    buffer_size = P * B;

	index_size = P*K;
	data_size = B*E;

	// BF related
    bpk = op.bits_per_key;
    mod_bf = op.elastic_filters;
    num_filter_units = op.num_filterunits;

    int bf_size = (buffer_size * bpk);
	int bf_index = (int)floor(0.693*bpk + 0.5);
    filter_unit_size = bf_size/num_filter_units;
	filter_unit_byte = ceil(float(filter_unit_size)/BYTE);
    filter_unit_index = bf_index/num_filter_units;

    BFHash::num_hash_indexes_ = filter_unit_index;
    BFHash::num_filter_units_ = num_filter_units;
    BFHash::share_hash_across_levels_ = op.share_hash_across_levels;
    BFHash::share_hash_across_filter_units_ = op.share_hash_across_filter_units;
    BFHash::prepareHashFuncs();
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
		} else if ( num>=6 && num<6+num_levels ){
			num_sstPerLevel[num-6] = value;
			num_sst += value;
		}
		num++;
	}
	infile.close();

	if ( num < 6+num_levels )
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

	ofstream file_settings (settings_file, ios::out);
	file_settings << P << endl;
	file_settings << B << endl;
	file_settings << E << endl;
	file_settings << K << endl;
	file_settings << num_filter_units << endl;

	file_settings << num_levels << endl;

	ofstream file_fence(fence_file, ios::out);
	for ( int i=0 ; i<num_levels ; i++ ){
                int len = fence_pointers[i].size();
		for ( int j=0 ; j<len ; j++ ){
			file_fence << fence_pointers[i][j] << endl;
		}
	}
	file_fence.close();
	bf_prime.resize(num_levels);
	blk_fp_prime.resize(num_levels);
	cout << "total lv " << num_levels << endl;
	for( int l = 0; l < num_levels; l++){
		cout << "lv " << l << endl;
		bf_prime[l] = vector<vector< vector<unsigned char> > > ();
		blk_fp_prime[l] = vector<vector< string > > ();
		vector<vector<string> > keys_one_level = reallocated_keys[l];
		int num_sst = keys_one_level.size();
		file_settings << num_sst << endl;
		for(int sst_index = 0; sst_index < num_sst; sst_index++){
			vector<string> keys_one_sst = keys_one_level[sst_index];
			vector< vector<unsigned char> > sst_bf;
			string sst_bf_prefix = bf_dir + "level_" + to_string(l) + "-sst_" + to_string(sst_index);

			for(int blo = 0; blo < num_filter_units; blo++){
				string sst_bf_filename = sst_bf_prefix + "_" + to_string(blo) +".txt";
				//cout << sst_bf_filename << endl;
				vector<unsigned char> blo_bf (filter_unit_byte, 0);
				int end = keys_one_sst.size();
				for( int i=0 ; i <end ; i++){
					pgm_BF(keys_one_sst[i], l, blo, filter_unit_size, filter_unit_index, &blo_bf);
				}
				flushBFfile(sst_bf_filename, &blo_bf);
				sst_bf.push_back(blo_bf);
			}
			bf_prime[l].push_back(sst_bf);

			// fence pointers
			
			string sst_index_filename = index_dir + "level_" + to_string(l) + "-sst_" + to_string(sst_index) + ".txt";
			// building index
			vector<string> index_one_sst;
			for( int i=0 ; i<keys_one_sst.size() ; i+=B ){
				index_one_sst.push_back(keys_one_sst[i]);
			}
			blk_fp_prime[l].push_back(index_one_sst);
			flushfile(sst_index_filename, &index_one_sst);
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
	string value;
	BFHash bfHash(key);
	for ( int i=0 ; i<num_levels ; i++ ){
 		//auto other_start = high_resolution_clock::now();
		value = GetLevel( i, bfHash, key, result );
		if ( *result == true ){
			num_lookups++;
			return value;
		}
 		//auto other_end = high_resolution_clock::now();
		//other_duration += duration_cast<microseconds>(other_end - other_start);
	}	
	num_lookups++;
	*result = false;

	return "";
}

string db::GetLevel( int i, BFHash & bfHash, string key, bool * result )
{
	// Binary search for fense pointer
	auto bs_start = high_resolution_clock::now();
	int bf_no = binary_search(key, fence_pointers[i]);
	auto bs_end   = high_resolution_clock::now();
	bs_duration += duration_cast<microseconds>(bs_end - bs_start);	

	lnum++;
	(lnum_histogram->at(i))++;

	// no matching sst
	if(bf_no < 0){
		*result = false;
		return "";
	}

	vector<string> hash_digests;
	bfHash.getLevelwiseHashDigest(i, hash_digests);

	bool bf_result = QueryFilter( i, bf_no, hash_digests, key);
		
	//cout << "QueryFilter result : " << i << " " << bf_no << " " << bf_result << endl;

	if( bf_result==false ){
		total_n++;
		*result = false;
		return "";
	}
	total_p++;

	int index_pos = GetFromIndex( i, bf_no, key );

	bool data_result = false;
	string data = GetFromData( i, bf_no, index_pos, key, &data_result );

	// false positive
	if (data_result == false){
		total_fp++;
	}
	

	*result = data_result;
	return data;
}

bool db::QueryFilter( int i, int bf_no, vector<string> & hash_digests, string key )
{
	bool result = true;
	
	for(int blo = 0; blo < num_filter_units; blo++){
		qnum++;
		(qnum_histogram->at(blo))++;
		
		result = QueryModule( i, bf_no, blo, hash_digests[blo], key);
		if ( result == false ){
			return false;
		}
	}

	return result;
}

bool db::QueryModule( int i, int bf_no, int blo, string & m_dataHash, string & key )
{
    int * ind_dec;
    ind_dec = (int * )malloc( filter_unit_index * sizeof(int));
	get_index(m_dataHash, filter_unit_index, filter_unit_size, ind_dec );
    vector<unsigned char> filter_unit = bf_prime[i][bf_no][blo];
    for( int k=0 ; k<filter_unit_index ; k++ ){
    	auto bf_start = high_resolution_clock::now(); 
    	//unsigned int refBit = 1;
    	unsigned int refBit = bf_mem_access( filter_unit, ind_dec[k] );
    	auto bf_end = high_resolution_clock::now();
    
    	bf_duration += duration_cast<microseconds>(bf_end - bf_start);
    	if(refBit == 0){
		free(ind_dec);
    		return false;
    	}
    }	

    free(ind_dec);
    return true;
}

int db::GetFromIndex( int l, int bf_no, string key )
{
	vector <string> index = blk_fp_prime[l][bf_no];

	auto bs_start = high_resolution_clock::now();
	int index_pos = binary_search(key, index);
	auto bs_end   = high_resolution_clock::now();
	bs_duration += duration_cast<microseconds>(bs_end - bs_start);	
	//cout << key << " " << l << " " << index_pos << endl;
	return index_pos;
}

string db::GetFromData( int i, int bf_no, int index_pos, string key, bool * result )
{
	vector <string> data_block;
	vector <string> filter;
	string sst_data_filename = data_dir + "level_" + to_string(i) + "-sst_" + to_string(bf_no) + ".txt";

	
			auto data_start = high_resolution_clock::now();
	read_data( sst_data_filename, index_pos, key.size(), data_block );
			auto data_end   = high_resolution_clock::now();
			data_duration += duration_cast<microseconds>(data_end - data_start);
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

int db::read_data ( string filename, int pos, int key_size, vector<string> & data_block )
{
	int flags = O_RDWR | O_DIRECT | O_SYNC;
	mode_t mode=S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;//644

	int fd = open(filename.c_str(), flags, mode );
	if (fd <= 0) {
    	printf("Error %s\n", strerror(errno));
		cout << "Cannot open partiton file " << filename << endl;
		return 0;
	}
      
	char* buf;
	posix_memalign((void**)&buf,getpagesize(),DB_PAGE_SIZE);
	memset(buf, 0, DB_PAGE_SIZE);

	data_block.clear();
	data_block.resize(B, "");

	//auto data_start = high_resolution_clock::now();
	read( fd, buf, DB_PAGE_SIZE );
	//auto data_end   = high_resolution_clock::now();
	//data_duration += duration_cast<microseconds>(data_end - data_start);

	int size;
	int entry_size;
	memcpy(&size, buf, sizeof(unsigned int));	
	memcpy(&entry_size, buf+sizeof(unsigned int), sizeof(unsigned int));	
      
	int offset = pos*B*E;
    // Moving pointer
	lseek( fd, DB_PAGE_SIZE+offset, SEEK_SET );

	memset(buf, 0, DB_PAGE_SIZE);
	//data_start = high_resolution_clock::now();
	read( fd, buf, DB_PAGE_SIZE );
	//data_end   = high_resolution_clock::now();
	//data_duration += duration_cast<microseconds>(data_end - data_start);

	//int key_size;
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
		
			//auto data_start = high_resolution_clock::now();
			read( fd, buf, DB_PAGE_SIZE );
			//auto data_end   = high_resolution_clock::now();
			//data_duration += duration_cast<microseconds>(data_end - data_start);

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

	return 0;
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
	}
	SetTreeParam();
}

void db::loadBFAndIndex(){
	bf_prime.resize(num_levels);
	blk_fp_prime.resize(num_levels);
	for(int i = 0; i < num_levels; i++){
		bf_prime[i] = vector<vector< vector<unsigned char> > > ();
		blk_fp_prime[i] = vector<vector< string > > ();

		for(int j = 0; j < num_sstPerLevel[i]; j++){
			vector<string> index;
			string sst_index_filename = index_dir + "level_" + to_string(i) + "-sst_" + to_string(j) + ".txt";
			read_index(sst_index_filename, index);
			blk_fp_prime[i].push_back(index);

			vector< vector<unsigned char> > sst_bf;

			for(int k = 0; k < num_filter_units; k++){
				vector<unsigned char> filterunit(filter_unit_byte, 0);	
				string sst_bf_filename = bf_dir + "level_" + to_string(i) + "-sst_" + to_string(j) + "_" + to_string(k) + ".txt";
				read_bf(sst_bf_filename, filterunit, filter_unit_byte);
				sst_bf.push_back(filterunit);
			}
			bf_prime[i].push_back(sst_bf);
		}
	}

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

void db::flushBFfile(string filename, vector<unsigned char> * sst_bf_p){
	ofstream outfile(filename, ios::out);
	char buffer[sizeof(unsigned int)];
	memset(buffer, 0, sizeof(unsigned int));
	
	for(int i = 0; i < sst_bf_p->size(); i++){
		memcpy(buffer, &(sst_bf_p->at(i)), sizeof(unsigned char));
		outfile.write(buffer, sizeof(unsigned char));
		memset(buffer, 0, sizeof(unsigned char));
	}	
	outfile.close();

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


void db::PrintStat()
{
	// log file
	string file_result = out_path + "result.txt";
	ofstream result_file(file_result);

	result_file << total_n << " " << total_p << endl;
	//result_file << "Positive Counters: P \t TP \t FP" << endl;
	//for(int i = 0; i < num_levels; i++){
	//	result_file << "\tLevel " << i + 1 << ": " << positive_counters[i] << "\t" << bf_tp_eval_histogram->at(i) << "\t" << positive_counters[i] - bf_tp_eval_histogram->at(i) << endl;
	//}
	
	result_file << "the total number of BF queries " << lnum << endl;
	result_file << "the total number of filter_unit queries " << qnum << " " << (double)qnum/lnum << endl;
	result_file << endl;


	for( int i=0 ; i<num_filter_units; i++ ){
		result_file << "qnum " << i << " : " << qnum_histogram->at(i) << endl;
		cout << "qnum " << i << " : " << qnum_histogram->at(i) << endl;
	}

	result_file.close();

	return;
}

void db::read_bf(string filename, vector<unsigned char> & bf, int size){
	int flags = O_RDWR | O_DIRECT;
	mode_t mode=S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;//644

	
	int fd_ = open(filename.c_str(), flags, mode );
	if (fd_ <= 0) {
		cout << "Cannot open partiton file " << filename << endl;
    	printf("Error %s\n", strerror(errno));
	}
	
	unsigned char* buffer_;	
	posix_memalign((void**)&buffer_,DB_PAGE_SIZE,DB_PAGE_SIZE);
	memset(buffer_, 0, DB_PAGE_SIZE);
	bf.clear();
    	char* bf_buffer = new char[size];

	for(int i = 0; i < size; i++){
		if ( i%DB_PAGE_SIZE == 0){
			memset(buffer_, 0, DB_PAGE_SIZE);
			read( fd_, buffer_, DB_PAGE_SIZE );
		}
		memcpy(bf_buffer+i, buffer_+(i%DB_PAGE_SIZE), sizeof(unsigned char));
	}
       	bf = vector<unsigned char>(bf_buffer, bf_buffer+size); 
	//cout << "string " << blo_bf.size() << endl;
	//for(int i = 0; i < blo_bf.size(); i++){
	//	cout << hex << (unsigned int)blo_bf[i] << endl;
	//}
    	delete bf_buffer;

	if(fd_ > 0)
		close( fd_ );	
}

void db::read_index( string filename, vector<string> & index )
{
	int flags = O_RDWR | O_DIRECT | O_SYNC;
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
	
	int size;
	int key_size;
	memcpy(&size, buf, sizeof(unsigned int));	
	memcpy(&key_size, buf+sizeof(unsigned int), sizeof(unsigned int));	

	int offset = 0;
	char* tmp_buf = (char*) malloc(key_size+1);
	tmp_buf[key_size] = '\0';

	memset(buf, 0, DB_PAGE_SIZE);
	read( fd, buf, DB_PAGE_SIZE );

	index.clear();
	index.resize(size, "");
	index.resize(size+1, "");
	for(int i = 0; i < size; i++){
		if(offset + key_size >= DB_PAGE_SIZE){
			unsigned int inner_key_offset = 0;
			if(offset < DB_PAGE_SIZE){
				memcpy(tmp_buf, buf+offset,DB_PAGE_SIZE-offset);
				inner_key_offset = DB_PAGE_SIZE-offset;
			}
		
			read( fd, buf, DB_PAGE_SIZE );

			offset = key_size - inner_key_offset;	
			memcpy(tmp_buf+inner_key_offset, buf, offset);
		}else{
			memcpy(tmp_buf, buf+offset, key_size);
			offset += key_size;
		}

		index[i] = string(tmp_buf);
		memset(tmp_buf, 0, key_size);
	}

	index[size] = std::string(key_size, 'z');
	free(tmp_buf);

	free( buf );
	close( fd );
}

using namespace std;

#include "../stdafx.h"
#include "../BF_bit.h"
#include <zstd.h>
#include <cstring>

void loadfile( string filename, vector<string>* table, int* num );
HashType convert(int in);

int main(int argc, char * argv[])
{
	if ( argc>5 ){
		cout << "Error : requres 2 arguments." << endl;
		cout << "   > argv[1] : output folder" << endl;
		cout << "   > argv[2] : hash mode (0:share, 1:multi)" << endl;
		cout << "   > argv[3] : infile" << endl;
		cout << "   > argv[4] : queryfile" << endl;
		return 0;
	}

	/* make output directory */
	char out_dir[1000] = "out/", out_filename[1000];
	char command[1000] = "mkdir -p ";
	if (argc>1)
		strcat(out_dir, argv[1]);

	string filename = out_dir;
	strcat(command, filename.c_str());
	system(command);

	cout << command << endl;

	// hash mode
	int hash_mode = (argc>2)? atoi(argv[2]) : 2;
	int hash_start = (hash_mode==6)? 0: hash_mode;
	int hash_end = (hash_mode==6)? 6: hash_mode+1;

	string file_in_set  = (argc>3)? argv[3] : "in/in_set.txt";
	string file_out_set = (argc>4)? argv[4] : "in/out_set.txt";

	vector<string> table_in;
	vector<string> table_out;

	int in_size = 0;
	int out_size = 0;
	loadfile( file_in_set, &table_in, &in_size );
	loadfile( file_out_set, &table_out, &out_size );

	// key size
	int key_size = table_in[0].size();


	// bf related
	int bf_sf = 10; // bits per item
	int bf_index = (int)floor(0.693*bf_sf); // bf_index == 6
	int bf_size = in_size * bf_sf;
	float width_f = log10((float)in_size) / log10(2.0);
	int width = (int)ceil(width_f);
	int bf_width = width + (float)(log10((float)bf_sf) / log10(2.0));

	unsigned char bf[(int)ceil((float)bf_size/WORD)];

	// get starting timepoint
	fsec hash_duration = std::chrono::microseconds::zero();
	fsec mem_duration = std::chrono::microseconds::zero();
	fsec index_duration = std::chrono::microseconds::zero();

	string input_str;
	int bf_ind[bf_index];
	uint64_t digest;

    BFHash::hash_digests_ = vector<uint64_t> (1, 0);
    BFHash::num_hash_indexes_ = bf_index;
    BFHash::num_filter_units_ = 1;
    BFHash::share_hash_across_levels_ = false;
    BFHash::share_hash_across_filter_units_ = false;
    BFHash::prepareHashFuncs();
	constexpr unsigned char mask[WORD] = {0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80};

	for ( int i=0 ; i<table_in.size(); i++ ) {
		input_str = table_in[i];
		BFHash bfHash( input_str );

		for ( int j=hash_start ; j<hash_end ; j++ ) {
			HashType ht = convert(j);
			digest = bfHash.get_hash_digest( input_str, ht, 0xbc9f1d34);

			if (hash_mode==6){
				bf_ind[j] = (hash_mode==6)? digest%bf_size : 0;
			}
		}
		if ( hash_mode<6 ){
			get_index(digest, bf_index, bf_size, bf_ind );
		}
    	for( int k=0 ; k<bf_index ; k++ ){
			unsigned int ind_byte = bf_ind[k]/WORD;
			unsigned char ref = bf[ind_byte];
			bf[ind_byte] = ref | mask[bf_ind[k]%WORD];
		}
	}
	cout << "BF program done" << endl;


	bool result;

	int num_n = 0;
	int num_fp = 0;
	int num_tp = 0;

	for ( int i=0 ; i<table_out.size(); i++ ) {
		input_str = table_out[i];
		BFHash bfHash( input_str );
		for ( int j=hash_start ; j<hash_end ; j++ ) {
			HashType ht = convert(j);
    		auto hash_start = high_resolution_clock::now(); 
			digest = bfHash.get_hash_digest( input_str, ht, 0xbc9f1d34);
    		auto hash_end = high_resolution_clock::now(); 
    		hash_duration += duration_cast<microseconds>(hash_end - hash_start);

			if (hash_mode==6){
    			auto index_start = high_resolution_clock::now(); 
				bf_ind[j] = (hash_mode==6)? digest%bf_size : 0;
    			auto index_end = high_resolution_clock::now(); 
    			index_duration += duration_cast<microseconds>(index_end - index_start);
			}
		}
		if ( hash_mode<6 ){
    		auto index_start = high_resolution_clock::now(); 
			get_index(digest, bf_index, bf_size, bf_ind );
    		auto index_end = high_resolution_clock::now(); 
    		index_duration += duration_cast<microseconds>(index_end - index_start);
		}
		result = true;
    	for( int k=0 ; k<bf_index ; k++ ){
    		auto mem_start = high_resolution_clock::now(); 
    		unsigned int refBit = bf_mem_access( bf, bf_ind[k] );
    		auto mem_end = high_resolution_clock::now();
    		mem_duration += duration_cast<microseconds>(mem_end - mem_start);

    		if(refBit == 0){
    			result = false;
				break;
    		}
		}
		if ( result==true ){
			vector<string>::iterator iter;
		    iter = find(table_in.begin(), table_in.end(), input_str);

			// false positive
   			if (iter == table_in.end()){
				num_fp++;
			}
			else { // true positive
				num_tp++;
			}
		}
		else {
			num_n++;
		}
	}

	// log file
	string file_result = filename + "result.txt";
	ofstream result_file(file_result);

	result_file << num_n << " " << (num_fp+num_tp) << endl;
	result_file << "false positives : " << num_fp << " " << (float)num_fp/(num_n+num_fp) << endl;
	result_file << endl;

	result_file << "hash   : " << hash_duration.count() << endl;
	result_file << "mem    : " << mem_duration.count() << endl;
	result_file << "index  : " << index_duration.count() << endl;
	result_file.close();

	return 0;
}

void loadfile( string filename, vector<string>* table, int* num )
{
	ifstream infile;
	infile.open( filename );

	if( infile.fail() ){
		cout << "Error opening " << filename << endl;
	}

	while( infile.peek() != EOF ){
		string line;
		getline(infile,line);
		int pos1 = line.find(" ");
		int pos2 = line.find(" ", pos1+2);
	    string data = line.substr(pos1+1, pos2-pos1-1);
		//cout << data << " " << data.size() << endl;
		table->push_back( data );
		(*num)++;
	}

	infile.close();
	return;
}

HashType convert(int in)
{
    if(in == 0) return MD5;
    else if(in == 1) return SHA2;
    else if(in == 2) return MurMurhash;
    else if(in == 3) return MurMur64;
    else if(in == 4) return XXhash;
    else if(in == 5) return CRC;
}

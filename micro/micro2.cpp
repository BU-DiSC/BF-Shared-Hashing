using namespace std;

#include "../stdafx.h"
#include "../BF_bit.h"
#include <zstd.h>
#include <cstring>

int num_n;
int num_fp;
int num_tp;

int hash_func;
int hash_mode;

int bf_index;
int bf_size;
unsigned char** bf;
int* bf_ind;
int num_filter_units;
bool fpr;
HashType ht1;
HashType ht2;

// get starting timepoint
fsec hash_duration = std::chrono::microseconds::zero();
fsec total_duration = std::chrono::microseconds::zero();


vector<string> table_in;
vector<string> table_out;

void loadfile( string filename, vector<string>* table, int* num );
HashType convert(int in);
bool Get(string & key);

int main(int argc, char * argv[])
{
	if ( argc>7 ){
		cout << "Error : requres 6 arguments." << endl;
		cout << "   > argv[1] : output folder" << endl;
		cout << "   > argv[2] : hash func (0:MD5, 1:SHA2, 2:MurMurhash, 3:MurMur64, 4:XXHash, 5:CRC, 6:CITY)" << endl;
		cout << "   > argv[3] : hash mode (0:no-share, 1:share-1, 2:share-2)" << endl;
		cout << "   > argv[4] : infile" << endl;
		cout << "   > argv[5] : queryfile" << endl;
		cout << "   > argv[6] : calculate fpr (0:no, 1:yes)" << endl;
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
	hash_func = (argc>3)? atoi(argv[2]) : 2;
	hash_mode = (argc>3)? atoi(argv[3]) : 1;

	string file_in_set  = (argc>4)? argv[4] : "in/in_set.txt";
	string file_out_set = (argc>5)? argv[5] : "in/out_set.txt";
	fpr = (argc>6)? atoi(argv[6]): 0;

	int in_size = 0;
	int out_size = 0;
	loadfile( file_in_set, &table_in, &in_size );
	loadfile( file_out_set, &table_out, &out_size );

	// key size
	int key_size = table_in[0].size();
	num_filter_units = 7;

	ht1 = convert(hash_func);
	ht2 = convert(3);
	if(hash_func == 3){
            ht2 = convert(4);
        }
	// bf related
	int bf_sf = 10; // bits per item
	bf_index = 7; // bf_index == 1
	bf_size = (int) ceil((in_size * bf_sf)/num_filter_units);
	float width_f = log10((float)in_size) / log10(2.0);
	int width = (int)ceil(width_f);
	bf = new unsigned char*[num_filter_units];
	for(int i = 0; i < num_filter_units; i++){
		bf[i] = new unsigned char[(int)ceil((float)bf_size/WORD)];
		memset(bf[i],0,(int)ceil((float)bf_size/WORD));
	}


	string input_str;
	bf_ind = new int[bf_index];
	uint64_t digest1;
	uint64_t digest2;

    /*
    BFHash::hash_digests_ = vector<uint64_t> (num_filter_units, 0);
    BFHash::num_hash_indexes_ = bf_index;
    BFHash::num_filter_units_ = num_filter_units;
    BFHash::share_hash_across_levels_ = false;
    BFHash::share_hash_across_filter_units_ = false;
    BFHash::prepareHashFuncs(convert(hash_mode));
    */
	constexpr unsigned char mask[WORD] = {0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80};
	for ( int i=0 ; i<table_in.size(); i++ ) {
		input_str = table_in[i];
		//BFHash bfHash( input_str );
		if(hash_mode == 1){
			digest1 = BFHash::get_hash_digest( input_str, ht1, 0xbc9f1d34);
			get_index(digest1, bf_index, bf_size, bf_ind );

		}else if(hash_mode == 2){
			digest1 = BFHash::get_hash_digest( input_str, ht1, 0xbc9f1d34);
			digest2 = BFHash::get_hash_digest( input_str, ht2, 0xbc9f1d34);
			bf_ind[0] = digest1%bf_size;
			for(int j = 1; j < num_filter_units; j++){
				bf_ind[j] = (digest1 + digest2*j)%bf_size;
			}	

		}else{ 
			int j = 0;
			for(auto ht:{ MD5, MurMurhash, CRC, XXhash, MurMur64, SHA2, CITY}){
				digest1 = BFHash::get_hash_digest(input_str, ht,  0xbc9f1d34);
				bf_ind[j++] = digest1%bf_size;
			}
		}
		
    	for( int k=0 ; k<bf_index ; k++ ){
			unsigned int ind_byte = bf_ind[k]/WORD;
			unsigned char ref = bf[k][ind_byte];
			bf[k][ind_byte] = ref | mask[bf_ind[k]%WORD];
		}
	}
	cout << "BF program done" << endl;


	bool result;

	num_n = 0;
	num_fp = 0;
	num_tp = 0;

	auto total_start = high_resolution_clock::now();
	for ( int i=0 ; i<table_out.size(); i++ ) {
		input_str = table_out[i];
		Get(input_str);
		
	}
	auto total_end = high_resolution_clock::now();
	total_duration += duration_cast<microseconds>(total_end - total_start);

	// log file
	string file_result = filename + "result.txt";
	ofstream result_file(file_result);

	//result_file << num_n << " " << (num_fp+num_tp) << endl;
	if(fpr){
	    result_file << "false positives : " << num_fp << " " << (float)num_fp/(num_n+num_fp) << endl;
	}
	result_file << endl;

	result_file << "hash   : " << hash_duration.count() << endl;
	result_file << "total  : " << total_duration.count() << endl;
	result_file << "other  : " << total_duration.count() - hash_duration.count() << endl;
	result_file.close();
	
	delete bf_ind;
	for(int i = 0; i < num_filter_units; i++){
		delete[] bf[i];
	}
	delete[] bf;
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
    else if(in == 6) return CITY;
    return MurMur64;
}


bool Get(string & key){
    bool result = true;
    uint64_t digest1;
    uint64_t digest2;
    //auto hash_start = high_resolution_clock::now(); 
    if(hash_mode == 1){
	digest1 = BFHash::get_hash_digest( key, ht1, 0xbc9f1d34);
	get_index(digest1, bf_index, bf_size, bf_ind );
    }else if (hash_mode == 2){
	digest1 = BFHash::get_hash_digest( key, ht1, 0xbc9f1d34);
	digest2 = BFHash::get_hash_digest( key, ht2, 0xbc9f1d34);
	bf_ind[0] = digest1%bf_size;
	for(int j = 1; j < num_filter_units; j++){
	    bf_ind[j] = (digest1 + j*digest2)%bf_size;
	}	
    }else{
	int j = 0;
        for(auto ht: { MD5, MurMurhash, CRC, XXhash, MurMur64, SHA2, CITY}){
	    digest1 = BFHash::get_hash_digest( key, ht, 0xbc9f1d34);
	    bf_ind[j++] = digest1%bf_size; 
	}

    }
    //auto hash_end = high_resolution_clock::now(); 
    //hash_duration += duration_cast<microseconds>(hash_end - hash_start);

    
    result = true;
    for( int k=0 ; k<bf_index ; k++ ){
    	unsigned int refBit = bf_mem_access( bf[k], bf_ind[k] );

    	if(refBit == 0){
    	    result = false;
	    break;
    	}
    }
    if ( result==true && fpr){
	vector<string>::iterator iter;
        iter = find(table_in.begin(), table_in.end(), key);

			// false positive
   	if (iter == table_in.end()){
		num_fp++;
	}
	else { // true positive
		num_tp++;
	}
    }else {
	num_n++;
    }
    return result;

}

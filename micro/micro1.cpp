using namespace std;

#include "../stdafx.h"
#include "../BF_bit.h"
#include <zstd.h>
#include <cstring>

int num_n;
int num_fp;
int num_tp;

int hash_mode;
int hash_start;
int hash_end;

int bf_index;
int bf_size;
unsigned char* bf;
int* bf_ind;

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
	hash_mode = (argc>2)? atoi(argv[2]) : 2;
	hash_start = (hash_mode==6)? 0: hash_mode;
	hash_end = (hash_mode==6)? 6: hash_mode+1;

	string file_in_set  = (argc>3)? argv[3] : "in/in_set.txt";
	string file_out_set = (argc>4)? argv[4] : "in/out_set.txt";

	int in_size = 0;
	int out_size = 0;
	loadfile( file_in_set, &table_in, &in_size );
	loadfile( file_out_set, &table_out, &out_size );

	// key size
	int key_size = table_in[0].size();


	// bf related
	int bf_sf = 10; // bits per item
	bf_index = (int)floor(0.693*bf_sf); // bf_index == 6
	bf_size = in_size * bf_sf;
	float width_f = log10((float)in_size) / log10(2.0);
	int width = (int)ceil(width_f);
	int bf_width = width + (float)(log10((float)bf_sf) / log10(2.0));
	bf = new unsigned char[(int)ceil((float)bf_size/WORD)];


	string input_str;
	bf_ind = new int[bf_index];
	uint64_t digest;

  
	constexpr unsigned char mask[WORD] = {0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80};
	for ( int i=0 ; i<table_in.size(); i++ ) {
		input_str = table_in[i];

		for ( int j=hash_start ; j<hash_end ; j++ ) {
			HashType ht = convert(j);
			digest = BFHash::get_hash_digest( input_str, ht, 0xbc9f1d34);

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

	num_n = 0;
	num_fp = 0;
	num_tp = 0;

	for ( int i=0 ; i<table_out.size(); i++ ) {
	auto total_start = high_resolution_clock::now();
		input_str = table_out[i];
		Get(input_str);
	auto total_end = high_resolution_clock::now();
	total_duration = total_end - total_start;
		
	}

	// log file
	string file_result = filename + "result.txt";
	ofstream result_file(file_result);

	result_file << num_n << " " << (num_fp+num_tp) << endl;
	result_file << "false positives : " << num_fp << " " << (float)num_fp/(num_n+num_fp) << endl;
	result_file << endl;

	result_file << "hash   : " << hash_duration.count() << endl;
	result_file << "total  : " << total_duration.count() << endl;
	result_file << "other  : " << total_duration.count() - hash_duration.count() << endl;
	result_file.close();
	
	delete bf_ind;
	delete bf;
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


bool Get(string & key){
    bool result = true;
    uint64_t digest;
    for ( int j=hash_start ; j<hash_end ; j++ ) {
	HashType ht = convert(j);
    	auto hash_start = high_resolution_clock::now(); 
	digest = BFHash::get_hash_digest( key, ht, 0xbc9f1d34);
    	auto hash_end = high_resolution_clock::now(); 
    	hash_duration += duration_cast<microseconds>(hash_end - hash_start);

	if (hash_mode==6){
		bf_ind[j] = (hash_mode==6)? digest%bf_size : 0;
	}
    }
    if ( hash_mode<6 ){
	get_index(digest, bf_index, bf_size, bf_ind );
    }
    result = true;
    for( int k=0 ; k<bf_index ; k++ ){
    	unsigned int refBit = bf_mem_access( bf, bf_ind[k] );

    	if(refBit == 0){
    	    result = false;
	    break;
    	}
    }
    if ( result==true ){
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

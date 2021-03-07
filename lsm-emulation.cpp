using namespace std;

#include "stdafx.h"
#include "BF_bit.h"
#include "hash/md5.h"

#include "options.h"
#include "db.h"

#include "time.h"
#include "math.h"

#include <bits/stdc++.h> 
#include <algorithm>
#include <fcntl.h>
#include <unistd.h>
#include <linux/fs.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <utility>

#define DB_PAGE_SIZE 4096

void loadfile( string filename, vector<string>* table, int* num );

int main(int argc, char * argv[])
{
	options op;
	// parse the command line arguments
	if ( op.parse(argc, argv) ) {
	  exit(1);
	}
	string filename = op.out_path;
	cout << filename << endl;

	int aloc = 0;

	// -- load workload --------------------------------------------------------------------
	string file_in_set  = op.insert_path;
	string file_out_set = op.query_path;

	vector<string> table_in;
	vector<string> table_out;

	int S_size = 0;
	int SC_size = 0;

	loadfile( file_out_set, &table_out, &SC_size );

	op.key_size = table_out.at(0).size();
	// -------------------------------------------------------------------------------------
	
	vector<vector<vector<string> > > reallocated_keys;

        db database( op );

	// -- Program && Bloom Filters -------------------------------------------------------------------
	// log file
	bool bf_only = false;
	bool rebuild = false;
	if ( op.destroy_db==false ){
		int read_result  = database.ReadSettings();
		cout << "read db check " << read_result << endl;
		bf_only = ( read_result==1 )? true : false;
		rebuild = ( read_result==-1 )? true : false;
	}
	if ( op.destroy_db || rebuild || bf_only ) {
		loadfile( file_in_set, &table_in, &S_size );
		database.split_keys( table_in, reallocated_keys );
		database.Build( reallocated_keys, bf_only );
	}
	else {
		database.SetTreeParam();
		database.loadBFAndIndex();
	}
	cout << "DB has been loaded" << endl; 
	// ------------------------------------------------------------------------------


	// -- Query ---------------------------------------------------------------------
	unsigned long total_query = table_out.size();

	int temp = 0;

	string input_str;
	srand(time(0));

	bool result;

	
	//for ( long i=0 ; i<100 ; i++ ) {
	for(int k = 0; k < database.tries; k++){
		cout << "Run " << k << " : " << endl;
		for ( long i=0 ; i<table_out.size() ; i++ ) {
			input_str = table_out[i];
			//cout << input_str << endl;
			result = false;
			database.Get( input_str, &result );

			input_str.clear();
			if ( i%(table_out.size()/10) == 0 ) cout << i/(table_out.size()/10)*10 << "%" << std::flush;
			if ( i%(table_out.size()/100) == 0 && i != 0 ) cout << "=" << std::flush;
		}
		cout << "100%" << endl;
	}
	
	// ------------------------------------------------------------------------------
		// log file
	database.PrintStat();
	return temp;
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

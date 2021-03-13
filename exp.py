import os, sys

HASH_FUNCTIONS = {'MurmurHash64','XXH64','md5','cal_sha_256','MurmurHash2','crc32_fast'}

def find_fst_percent(line):
	start = -1
	end = 0
	for i, c in enumerate(line.strip()):
		if c.isdigit() and start == -1:
			start = i
		elif c == "%" and start != -1:
			end = i
			break
	return start,end

if __name__ == '__main__':
	tries = int(sys.argv[1])
	command = " ".join(sys.argv[2:])
	perf_data = []
	raw_data = []
	for i in range(tries):
		
		os.system(command)
		f = open("out/result.txt")
		total_data_pair = [0,0,0]
		for line in f:
			tmp = line.strip().split(":")
			if tmp[0] == "Total query time":
				total_data_pair[0] = float(tmp[1].strip())
			elif tmp[0] == "Data access time":
				total_data_pair[1] = float(tmp[1].strip())
			elif tmp[0] == "Hash time":
				total_data_pair[2] = float(tmp[1].strip())
		raw_data.append(total_data_pair)	
		f.close()
		os.system("perf record -g " + command)
		os.system("perf report > tmp.txt")
		
		f = open("tmp.txt","r")
		mem_hash_bs_triples = [0,0,0,0]
		for line in f:
			if '[.] bf_mem_access' in line:
				l = line.strip()
				start,end = find_fst_percent(l)		
				mem_hash_bs_triples[0] = float(l[start:end])*0.01
			elif '[.] db::binary_search' in line:
				l = line.strip()
				start,end = find_fst_percent(l)		
				mem_hash_bs_triples[2] = float(l[start:end])*0.01
			elif '[.] db::Get' in line and mem_hash_bs_triples[3] == 0:
				l = line.strip()
				start,end = find_fst_percent(l)		
				mem_hash_bs_triples[3] = float(l[start:end])*0.01
				
			elif mem_hash_bs_triples[1] == 0:
				for h_str in HASH_FUNCTIONS:	
					tmp = '[.] ' + h_str
					if h_str in line:
						l = line.strip()
						start,end = find_fst_percent(l)
						mem_hash_bs_triples[1] = float(l[start:end])*0.01
						break
			if mem_hash_bs_triples[0] != 0 and mem_hash_bs_triples[1] != 0 and mem_hash_bs_triples[2] != 0:
				break
		perf_data.append(mem_hash_bs_triples)
		f.close()
		os.system("rm -rf tmp.txt")
	#print(perf_data)
	#print(raw_data)
	total = 0
	data = 0
	hasht = 0
	for t, d, h in raw_data:
		total += t
		data += d
		hasht += h
	print("Average Data Access : " + str(data/tries))
	total = total/tries
	#print(total)
	am = 0.0
	ah = 0.0
	ab = 0.0
	at = 0.0
	for m, h, b, tt in perf_data:
		am += m
		ah += h
		ab += b	
		at += tt
	T = total/(at/tries)
	ht = max((T*ah)/tries, hasht/tries)
	bst = (T*ab)/tries
	mpt = (T*am)/tries
	print("Average Hash : " + str(ht))
	print("Average BS : " + str(bst))
	print("Average Memory Probing : " + str(mpt))
	print("Average Others : " + str(total - data/tries - ht - bst - mpt))
	
						

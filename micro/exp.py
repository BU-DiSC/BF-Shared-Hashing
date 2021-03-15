import os, sys

#HASH_FUNCTIONS = {'MurmurHash64','XXH64','md5','cal_sha_256','MurmurHash2','crc32_16bytes'}
HASH_FUNCTIONS = {'BFHash::get_hash_digest'}

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
	fpr_data = []
	for i in range(tries):
		
		os.system(command)
		f = open("out/result.txt")
		total_hash_pair = [0,0]
		for line in f:
			tmp = line.strip().split(":")
			tmp[0] = tmp[0].strip()
			if tmp[0] == "total":
				total_hash_pair[0] = float(tmp[1].strip())
			elif tmp[0] == "hash":
				total_hash_pair[1] = float(tmp[1].strip())
			elif "false positives" in line:
				fpr_data.append(float(tmp[-1].strip().split(' ')[-1]))
		raw_data.append(total_hash_pair)	
		f.close()
		os.system("perf record -g " + command)
		os.system("perf report > tmp.txt")
		
		f = open("tmp.txt","r")
		hash_total_pairs = [0,0]
		for line in f:
			if '[.] Get' in line and hash_total_pairs[1] == 0:
				l = line.strip()
				start,end = find_fst_percent(l)		
				hash_total_pairs[1] = float(l[start:end])*0.01
				
			elif hash_total_pairs[0] == 0:
				for h_str in HASH_FUNCTIONS:	
					tmp = '[.] ' + h_str
					if h_str in line:
						l = line.strip()
						start,end = find_fst_percent(l)
						if end == 0:
							break
						hash_total_pairs[0] = float(l[start:end])*0.01
						break
			if hash_total_pairs[0] != 0 and hash_total_pairs[1] != 0:
				break
		perf_data.append(hash_total_pairs)
		f.close()
		os.system("rm -rf tmp.txt")
	print(perf_data)
	print(raw_data)
	total = 0
	hasht = 0
	for h,t in raw_data:
		total += t
		hasht += h
	total = total/tries
	#print(total)
	ah = 0.0
	at = 0.0
	for h, tt in perf_data:
		ah += h
		at += tt
	print(total)
	T = total/(at/tries)
	print((T*ah)/tries)
	#print(hasht/tries)
	ht = max((T*ah)/tries, hasht/tries)
	print("Averag FPR : " + str(sum(fpr_data)/tries))
	print("Average Hash : " + str(ht))
	print("Average Others : " + str(max(total - ht,0)))
	
						

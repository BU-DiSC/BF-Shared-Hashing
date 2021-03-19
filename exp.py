import os, sys

HASH_FUNCTIONS = {'BFHash::getLevelwiseHashDigest'}

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
		total_data_pair = [0,0,0]
		for line in f:
			tmp = line.strip().split(":")
			if tmp[0] == "Total query time":
				total_data_pair[0] = float(tmp[1].strip())
			elif tmp[0] == "Data access time":
				total_data_pair[1] = float(tmp[1].strip())
			elif tmp[0] == "FPR":
				total_data_pair[2] = float(tmp[1].strip())
		raw_data.append(total_data_pair)	
		f.close()
		os.system("perf record -g " + command)
		os.system("perf report > tmp.txt")
		
		f = open("tmp.txt","r")
		hash_bs_triples = [0,0,0,0,0]
		for line in f:
			if '[.] db::binary_search' in line:
				l = line.strip()
				start,end = find_fst_percent(l)		
				if end == 0:
					break
				hash_bs_triples[1] = float(l[start:end])*0.01
			elif '[.] db::Get' in line and hash_bs_triples[2] == 0:
				l = line.strip()
				start,end = find_fst_percent(l)		
				if end == 0:
					break
				hash_bs_triples[2] = float(l[start:end])*0.01
			elif '[.] db::GetFromData' in line and hash_bs_triples[4] == 0:
				l = line.strip()
				start,end = find_fst_percent(l)		
				if end == 0:
					break
				hash_bs_triples[4] = float(l[start:end])*0.01
			elif '[.] db::QueryFilter' in line and hash_bs_triples[3] == 0:
				l = line.strip()
				start,end = find_fst_percent(l)		
				if end == 0:
					break
				hash_bs_triples[3] = float(l[start:end])*0.01

			elif hash_bs_triples[0] == 0:
				for h_str in HASH_FUNCTIONS:	
					tmp = '[.] ' + h_str
					if h_str in line:
						l = line.strip()
						start,end = find_fst_percent(l)
						hash_bs_triples[0] = float(l[start:end])*0.01
						break
			if hash_bs_triples[0] != 0 and hash_bs_triples[1] != 0 and hash_bs_triples[2] != 0 and hash_bs_triples[3] != 0 and hash_bs_triples[4] != 0:
				break
		perf_data.append(hash_bs_triples)
		f.close()
		os.system("rm -rf tmp.txt")
	print(perf_data)
	print(raw_data)
	total = 0
	data = 0
	total_fpr = 0.0
	for t, d, fpr in raw_data:
		total += t
		data += d
		total_fpr += fpr
	print("FPR : " + str(total_fpr/tries))
	data = data/tries
	print("Average Data Access : " + str(data))
	total = total/tries
	
	#print(total)
	ah = 0.0
	ab = 0.0
	at = 0.0
	af = 0.0
	adc = 0.0
	for h, b, tt, f, dc in perf_data:
		ah += h
		ab += b	
		at += tt
		af += f
		adc += dc
	T = (total - data)/(1-adc/tries)
	ht = (T*ah)/tries
	bst = (T*ab)/tries
	ft = (T*af)/tries
	print("Average Hash : " + str(ht))
	print("Average BS : " + str(bst))
	print("Average QueryFilter : " + str(ft))
	print("Average Others : " + str(T*at/tries - ht - bst - ft))
	
						

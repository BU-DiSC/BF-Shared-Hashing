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
		total_data_pair = [0,0,0,0]
		for line in f:
			tmp = line.strip().split(":")
			if tmp[0] == "Total query time":
				total_data_pair[0] = float(tmp[1].strip())
			elif tmp[0] == "Data access time":
				total_data_pair[1] = float(tmp[1].strip())
			elif tmp[0] == "Hash time":
				total_data_pair[3] = float(tmp[1].strip())
			elif tmp[0] == "FPR":
				total_data_pair[2] = float(tmp[1].strip())
		raw_data.append(total_data_pair)	
		f.close()
		
	
	print(raw_data)
	total = 0.0
	data = 0.0
	total_fpr = 0.0
	hasht = 0.0
	for t, d, fpr, ht in raw_data:
		total += t
		data += d
		total_fpr += fpr
		hasht += ht
	print("FPR : " + str(total_fpr/tries))
	data = data/tries
	print("Average Data Access : " + str(data))
	total = total/tries
	print("Average Hash : " + str(hasht/tries))
	print("Average Others : " + str(total/tries - data - hasht/tries))
	
						

# iesiyok
# 9 Apr 2015, 15:05
# This is the program running in parallel
# For running this program on the edward or any HPC system there are two options:
# 1. run it on cmd | python twit_search.py search_str data.csv job_id
# 2. run it by gen_script.sh
#    qsub -q fast -l nodes=1:ppn=8,walltime=12:00:00,pmem=2000mb -F "1 8 abbott" gen_script.sh

from sys import argv
from mpi4py import MPI
import re
import array
from datetime import datetime

#@ function analyser:
#Every process gets a different piece of the data, 
#and analyses it by splitting the piece into small chunks

def analyser(comm, infile, rank, size, search_str, t_start, job_id) :

	#file_size: actual size of file
	file_size = MPI.File.Get_size(infile) 

	#rank_size: the size of the piece that the individual process will analyse
	rank_size = file_size / size 

	#start_point: the start index that the process will begin reading from
	start_point = rank * rank_size 

	#av_chunk_size: the size of the chunk 
	#that the process will get into the buffer at each time
	av_chunk_size = 40000000 #average chunk_size

	item_size = MPI.CHAR.Get_size()#char size = 1
	if rank_size > av_chunk_size : 
		mem_size = item_size * av_chunk_size
	else :	
		mem_size = rank_size #no need to split small data
	
	#the number of times that the given search_str is mentioned
	search_count = 0 

	#dictionary for the @tweeter names that are mentioned the most, top 10
	user_dict = {} 

	#dictionary for the #topics that are discussed the most, top 10
	topic_dict = {} 

	current_mem_size = 0
	
	if rank == size - 1 : end_point = file_size
	else : 
		end_point = start_point + rank_size - 1

	local_start = start_point

	#get chunks of data and analyse it
	while local_start < end_point : 

		if rank_size - current_mem_size == 0 : 
			break
		if rank_size - current_mem_size < mem_size : 
			mem_size = rank_size - current_mem_size			 

		#memory allocation for the chunk
		chunk_buf = MPI.Alloc_mem(mem_size, MPI.INFO_NULL)
		current_mem_size += mem_size

		#move the index to local_start and local_start is the begin point of chunk
		MPI.File.Seek(infile, local_start, MPI.SEEK_SET)
		#read the data into the chunk_buf
		MPI.File.Read_at(infile, local_start, chunk_buf, None)

		#create word_list from chunk_buf
		line = ""
		i = 0
		while i < mem_size and local_start < end_point :
			line += chunk_buf[i] 
			i += 1
			local_start += 1
		#end of while loop

		#release the memory
		chunk_buf = MPI.Free_mem(chunk_buf) 		

		#search the search_string in the word_list
		#s = 
		#s = re.sub(r'^'+search_str+r'$', search_str, 1)
		
		search_res = re.findall(r"\b%s\b" % search_str, line, re.I)
		search_count += len(search_res)

		#retrieve all of the @tweeters that are mentioned		
		ul = findinData(re, r"@[\w]+", line)

		#retrieve all of the @topics that are discussed
		tl = findinData(re, r"#[\w]+", line)

		#add @tweeters into user_dictionary
		user_dict = add2Dictionary(ul, user_dict)

		#add @topics into topic_dictionary
		topic_dict = add2Dictionary(tl, topic_dict)
	
	#end of while loop
	
	#every process except rank=0[main process] 
	#sends their analyse results to main process(rank=0),
	#that is dict_send_buf holds all information
	dict_send_buf = {}
	dict_send_buf['search'] = search_count
	dict_send_buf['user_dict'] = user_dict
	dict_send_buf['topic_dict'] = topic_dict

	s_data = []
	rec_data = []
	s_data.append(dict_send_buf)
	search_sum = 0
	user_sum = {}
	topic_sum = {}

	MPI.File.Close(infile)
	comm.Barrier()

	#rank=0 gathers all of the data from other processes,
	#and merges the results
	results = comm.gather(s_data, rec_data, root = 0)

	comm.Barrier()

	if rank == 0 : 
		for res in results :
			search_sum += res[0].get('search')#search result
			u_dict = res[0].get('user_dict')
			user_sum = dictionary2List(u_dict, user_sum)#tweeter result
			t_dict = res[0].get('topic_dict')
			topic_sum = dictionary2List(t_dict, topic_sum)#topic result

		#end of for loop
			
		#write the results into a file in res_YYYYMMDD_HHMMSS format
		write_results(size, file_size, search_str, search_sum, user_sum, topic_sum, job_id)


#end of analyser function

#generic function for user and topic dictionary, 
#add values in dictionary into list
def dictionary2List(rdict, rsum) :
	for r in rdict :
		if r in rsum :
			rsum[r] += rdict[r]
		else :
			rsum[r] = rdict[r]
	return rsum
#end of dictionary2List function

#generic function for user and topic dictionary, 
#search all users or topics
def findinData(re, search_str, line) :
	rlist = re.findall(search_str, line, re.I)
	rl = map(lambda rl:rl.lower(), rlist)#to lower case
	return rl

#end of findinData

#generic function, adds new items to dictionary
def add2Dictionary(rlist, rdict) :
	for item in rlist :
		if rdict.has_key(item) :
			rdict[item] += 1
		else :
			rdict[item] = 1	
	return rdict

#end of add2Dictionary function

def write_results(size, file_size, search_str, r_search_sum, r_user_sum,r_topic_sum, job_id):
	i = datetime.now()
	f = open('res_'+ job_id + i.strftime('_%Y%m%d_%H%M%S'),'w+')
	t_diff = MPI.Wtime() - t_start
	f_str = "Process size = " + str(size) + " \n"
	f.write(f_str)
	f_str = "File size = " + str(file_size) + " \n"
	f.write(f_str)
	f_str = "The result is calculated in " + str(t_diff) + " s \n"
	f.write(f_str)
	f.write("TASK - 1 \n")
	f_str = "   String : '" + search_str + "' , " + "Count = " 
	f.write(f_str)
	f.write(str(r_search_sum))				
	f.write("\n")
	f.write("TASK - 2 \n")
	f.write("   Top 10 tweeters that are mentioned the most : \n")
	for key, value in sorted(r_user_sum.iteritems(), key=lambda (k,v): (v,k), reverse=True)[:10]:
		f_str = "      " + str(key) + " : " + str(value) + "\n"
		f.write(f_str)
	f.write("TASK - 3 \n")
	f.write("   Top 10 topics that are most frequently discussed : \n")
	for key, value in sorted(r_topic_sum.iteritems(), key=lambda (k,v): (v,k), reverse=True)[:10]:
		f_str = "      " + str(key) + " : " + str(value) + "\n"
		f.write(f_str)

	f.close()
#end of write_results function

 #MAIN PROGRAM
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
#comm.Barrier()
print rank,  " START.."
t_start = MPI.Wtime()
search_str = argv[1]
file_str = argv[2]
job_id = argv[3]
infile = MPI.File.Open(comm, file_str, MPI.MODE_RDONLY, MPI.INFO_NULL)
print "search_str = ", search_str
file_size = MPI.File.Get_size(infile)
chunk_size = file_size / size  
print "size =", file_size
print "chunk size =", chunk_size
analyser(comm, infile, rank, size, search_str, t_start, job_id)
#comm.Barrier()

t_diff = MPI.Wtime() - t_start
if comm.rank==0: print t_diff
print rank, " FINISH.."
#END OF PROGRAM		

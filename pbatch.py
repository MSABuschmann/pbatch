import sys
import os
import numpy as np

partition_list=['lr3_16', 'lr3_20', 'lr3', 
		'lr4', 
		'lr5_20', 'lr5_28', 'lr5', 
		'lr6_32', 'lr6_40', 'lr6']
node_list = [	'n0[000-163],n0[309-336]',  'n0[164-203],n0[213-308]', 'ALL' ,
	     	'ALL',
		'n0[148-191]','n0[000-143]','ALL',
		'n0[000-087],n0[088-115]','n0[144-271]','ALL']
cpu_list = [	16,20,16,
		24,
		20,28,20,
		32,40,32 ]
mem_list = [	64000,64000,64000,
		64000,
		128000,64000,64000,
		96000,128000,96000]
partition_list_plain=['lr3', 'lr3', 'lr3', 
		'lr4', 
		'lr5', 'lr5', 'lr5', 
		'lr6', 'lr6', 'lr6']

job_name = []
ntasks_per_node = []
time = []
mem_per_cpu = []
account = []
partition = []
qos = []
mail_type = []
code = []
exports = []
array_values = []

def ExtractExports(string):
	try:
		string.index('--export=')
	except ValueError:
		print('Parsing error: Unknown input: '+string)
		sys.exit(1)
	string = string.replace('--export=','')
	exports.append( string.split(',') )

def GetArrayValues(string):
	if string.find(',') != -1:
		return np.array(string.split(','),dtype='int')

	start = 0
	end = 0
	step = 1
	if string.find(':') != -1:
		split = string.split(':')
		step = int(split[1])
		string = split[0]
	split = string.split('-')
	start = int(split[0])
	end = int(split[1])
	vals = []
	for i in range(start,end+step,step):
		vals.append(i)	
	return vals

def mem_in_mb(string):
	v1 = string.replace('g','')
	v2 = string.replace('gb','')
	v3 = string.replace('m','')
	v4 = string.replace('mb','')
	if string.isnumeric():
		return int(string)
	if v1.isnumeric():
		return int(v1)*1000
	if v2.isnumeric():
		return int(v2)*1000
	if v3.isnumeric():
		return int(v3)
	if v4.isnumeric():
		return int(v4)
	return int(string)

def ParseFile(filename):
	file = open(filename,'r')
	lines = file.readlines() 

	HeaderEnd = -1
	for i in range(len(lines)):
		lines[i] = lines[i].replace('\n','')
		lines[i] = lines[i].replace('#SBATCH --','')
		if lines[i] == '#PBATCH END':
			HeaderEnd = i
		if HeaderEnd == -1:
			lines[i] = lines[i].replace(' ','')

	for i in range(HeaderEnd):
		split = lines[i].split('=')
		if split[0] == 'job-name':
			job_name.append(split[1])
		if split[0] == 'ntasks-per-node':
			ntasks_per_node.append(int(split[1]))
		if split[0] == 'time':
			time.append(split[1])
		if split[0] == 'mem-per-cpu':
			mem_per_cpu.append(split[1])
		if split[0] == 'account':
			account.append(split[1])
		if split[0] == 'partition':
			partition.append(split[1])
		if split[0] == 'qos':
			qos.append(split[1])
		if split[0] == 'mail-type':
			mail_type.append(split[1])
		if split[0] == 'array':
			array_values.append( GetArrayValues( split[1] ))

	code.append(lines[HeaderEnd+1:])

master_folder = sys.argv[1]
output_folder = sys.argv[2]
script_folder = sys.argv[3]
presamp = sys.argv[4]
for i in range(5,len(sys.argv)):
	if os.path.exists( sys.argv[i] ):
		ParseFile(sys.argv[i])
	else:
		ExtractExports(sys.argv[i])

if len(mem_per_cpu) + len(exports) != len(sys.argv)-5:
	print("parsing error: could not interpret argument list")
	sys.exit(1)

if len(exports) != 0 and len(exports) != len(mem_per_cpu):
	print("parsing error: --export list incorrect")
	sys.exit(1)

if len(mem_per_cpu) != len(ntasks_per_node):
	print("parsing error: information missing when reading slurm header")
	sys.exit(1)

if len(partition) == 0:
	print("no partition specified!")
	sys.exit(1)

if len(time) == 0:
	print("no walltime specified!")
	sys.exit(1)

if len(array_values) > 0:
	for i in range(1,len(array_values[0])):
		ntasks_per_node.append( ntasks_per_node[0] )
		mem_per_cpu.append( mem_per_cpu[0] )
		code.append( code[0] )

mem = np.zeros(len(mem_per_cpu))
for i in range(len(mem_per_cpu)):
	mem[i] = mem_in_mb(mem_per_cpu[i])

part = np.argwhere( np.array(partition_list) == partition[0] )[0][0]

node = np.zeros(len(mem_per_cpu))
num_per_node = [0]
fin_mem = []
fin_cpu = []
cum_mem = 0
cum_cpu = 0
cur_node = 0
for i in range(len(mem_per_cpu)):
	cum_mem += mem[i]
	cum_cpu += ntasks_per_node[i]
	if cum_mem > mem_list[part] or cum_cpu > cpu_list[part]:
		fin_mem.append(cum_mem - mem[i])
		fin_cpu.append(cum_cpu - ntasks_per_node[i])
		cum_mem = mem[i]
		cum_cpu = ntasks_per_node[i]
		cur_node += 1	
		num_per_node.append(0)
	node[i] = cur_node
	num_per_node[cur_node] += 1

fin_mem.append(cum_mem)
fin_cpu.append(cum_cpu)
		
print('Detected',len(mem_per_cpu),'jobs that fit on',cur_node+1,'node(s).')
for i in range(cur_node+1):
	print('node '+str(i+1)+': '+str(num_per_node[i])+' tasks, '+
		 str(fin_cpu[i])+' of '+str(cpu_list[part])+' CPUs, '+
		 str(fin_mem[i]/1000.)+' GB of '+str(mem_list[part]/1000.)+' GB memory')

print('\nDoes that sound correct?')
check_val = input('[yes] [no]\n')

if check_val != 'y' and check_val != 'yes':
	sys.exit(1)

for i in range(len(mem_per_cpu)):
	f = open(script_folder+'/'+presamp+'_'+str(i)+'.sh','w')
	if len(exports) != 0:
		for s in exports[i]:
			f.write(s+'\n')
	if len(array_values) > 0:
		f.write('SLURM_ARRAY_TASK_ID='+str(array_values[0][i])+'\n')
		f.write('SLURM_ARRAY_TASK_COUNT='+str(len(array_values[0]))+'\n')
		f.write('SLURM_ARRAY_TASK_MAX='+str(np.max(array_values[0]))+'\n')
		f.write('SLURM_ARRAY_TASK_MIN='+str(np.min(array_values[0]))+'\n')

	f.write('\n#######################\n')
	for l in range(len(code[i])):
		f.write(code[i][l]+'\n')
	f.close()

soff = 0
for i in range(cur_node+1):
	f = open(script_folder+'/'+presamp+'_'+str(i)+'.sl','w')
	f.write('#!/bin/bash\n')
	if len(job_name) > 0:
		f.write('#SBATCH --job-name='+job_name[0]+'\n')
	f.write('#SBATCH --nodes=1\n')
	f.write('#SBATCH --time='+time[0]+'\n')
	f.write('#SBATCH --mem='+str(mem_list[part])+'m\n')
	f.write('#SBATCH --account='+account[0]+'\n')
	f.write('#SBATCH --partition='+partition_list_plain[part]+'\n')
	#if node_list[part] != 'ALL':
	#	f.write('#SBATCH --exclude='+master_folder+'/node_config/'+partition_list[part]+'\n')
	#	f.write('#SBATCH --cores-per-socket='+str(cpu_list[part])+'\n')
	f.write('#SBATCH --qos='+qos[0]+'\n')
	f.write('#SBATCH --mail-type='+mail_type[0]+'\n\n')

	f.write('echo "PBATCH: This job is executing script #'+str(soff)+
		' through #'+str(soff+num_per_node[i]-1)+'"\n')
	f.write('echo "PBATCH: Please find the respective job output under '+
		output_folder+'/output-$SLURM_JOB_ID.\$scriptid"\n')

	for s in range(num_per_node[i]):
		f.write(script_folder+'/'+presamp+'_'+str(s+soff)+'.sh > '+output_folder+'/slurm-$SLURM_JOB_ID.' +str(s+soff) +'.out &\n')
	f.write('wait')
	soff = soff + num_per_node[i]


sys.exit(0)
	

import sys
graph = {}

with open('graph_10000_50000.txt') as f:
    	lines = f.readlines()
	for line in lines:
		strip_line = line.split()
		#print strip_line
		strip_line[2]=strip_line[2].strip("\n")
		if strip_line[0] not in graph:
			graph[strip_line[0]]=[]
		graph[strip_line[0]].append(strip_line[1])
		#sys.stdout.write(str(strip_line[2])+"\t"+strip_line[0]+"\t"+strip_line[1]+"\n")

flag=0
i=0
for key in graph:
	graph[key]=  sorted(set(graph[key]))
	sys.stdout.write(str(key)+"\t")
	for i in range(len(graph[key])-1):
		sys.stdout.write(str(graph[key][i])+',')
	sys.stdout.write(str(graph[key][len(graph[key])-1])+"|")
	if flag==0:
		sys.stdout.write("0|GRAY|\n")
		flag=1
	else:
		sys.stdout.write("Integer.MAX|WHITE|\n")
	
	

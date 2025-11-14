#!/usr/bin/env python
import os
import sys
files = os.listdir(sys.argv[1])
files = [f for f in files if f.startswith('t.')]
lines = []
for file in files:
	with open(sys.argv[1] + '/' + file) as f:
		for line in f:
			if 'Completed' in line and 'warm' not in line:
				print line
				line = line.replace('\n', '').split(' ')
				lines.append(line[-5] + ' ' + line[-2])
throughput = 0.0
for l in lines:
	l = l.split(' ')
	print float(l[0])/float(l[1])
	throughput += float(l[0])/float(l[1])
print throughput
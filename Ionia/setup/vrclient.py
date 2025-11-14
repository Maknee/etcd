#!/usr/bin/env python3

#Copyright (c) 2021 Aishwarya Ganesan and Ramnatthan Alagappan.

#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:

#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.

import os
import sys
import math
import subprocess
import time
from pathlib import Path

#first arg is client binary path
#second is config file path
#third is num clients
#fourth is num operations

def invoke_remote_cmd(machine_ip, pdir, command):
	cmd = 'ssh -i {0}/{1}.pem {2}@{3} \'{4}\''.format(pdir, "us-east-1", "ubuntu", machine_ip, command)
	# print (cmd)
	p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out, err = p.communicate()
	if err is not None and len(err) > 0:
		#print cmd, out
		print("Warning for" +cmd + ":" + str(err))
	return (out, err)


def invoke_cmd(command):
	p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out, err = p.communicate()
	if err is not None and len(err) > 0:
		pass
		#print("Warning for" +cmd + ":" + str(err))
	return (out, err)

client_binary_path = "sudo " + sys.argv[1]
config_file_path = sys.argv[2]
perf_dir = os.path.dirname(os.path.realpath(config_file_path))
client_id = int(sys.argv[3])
num_ops = int(sys.argv[4])
workload = sys.argv[5]
code = sys.argv[6]
time_run = sys.argv[7]
workload_trace_prefix = sys.argv[8]
readwindow = sys.argv[9]
windowfrac = sys.argv[10]
clients = sys.argv[12]
load = int(sys.argv[13])
machines = int(sys.argv[14])

if code == 'ionia' or code == 'skyros':
	consensus_config = sys.argv[11]

workload_trace_dir = '/mnt/sda4/traces/'

for i in range(1, 9):
	os.system("sudo pkill -9 client")
	os.system('sleep 3')

file_lock = Path('/tmp/ionia_lock')
if workload  == 't' or workload == 'w' or workload == 'e' or workload == 'fs' or workload == 'fsar' or workload == 'compat' or workload == 'nc':
	print ('Not going to load')
# elif workload == 'a' or workload == 'c' or workload == '25_75' or workload == '75_25' or workload == '10_90':
# 	print('Skipping load because it is already loaded')
else:
	print ('Going to load')

	for j in range(1, 9):
		load_file_prefix = 'load/load.'
		if workload == 'r':
			load_file_prefix = 'loadr/load.'

		if code == 'ionia':
			# if workload not in ['a']:
			with open(file_lock, 'w+') as f:
				f.write('loading')

			machines = machines
			loading_clients = 8

			os.system(f'cd /mnt/sda4/traces/load && ./split_results_load.sh w {machines * loading_clients}')
			#load_file_prefix = 'loadw/load.'

			offset = str(client_id + int(loading_clients) * j)
			load_offset = str(0)
			if client_id >= machines:
				break

			# run = ('{0} -c {1} -s {5} -m vr -n {2} -k {3} -t {6} -j {7} -l /tmp/latencies_load.{4} -p 1 -e 600 > /tmp/load.log.{4} 2>&1 &'.format(client_binary_path, config_file_path, 1000000000, workload_trace_dir + load_file_prefix, str(client_id), consensus_config, offset, loading_clients))
			run = ('{0} -c {1} -s {5} -m vr -n {2} -k {3} -t {6} -j {7} -l /tmp/latencies_load.{4} -p 0 -e 10000000 > /tmp/load.log.{4} 2>&1 &'.format(client_binary_path, config_file_path, 1000000000, workload_trace_dir + load_file_prefix, str(client_id), consensus_config, offset, loading_clients))
			print(run)
			os.system(run)

			# machines = 4
			# loading_clients = 8
			# offset = str(client_id + int(loading_clients) * j)
			# load_offset = str(0)
			# if client_id >= machines:
			# 	break

			# # run = ('{0} -c {1} -s {5} -m vr -n {2} -k {3} -t {6} -j {7} -l /tmp/latencies_load.{4} -p 1 -e 600 > /tmp/load.log.{4} 2>&1 &'.format(client_binary_path, config_file_path, 1000000000, workload_trace_dir + load_file_prefix, str(client_id), consensus_config, offset, loading_clients))
			# run = ('{0} -c {1} -s {5} -m vr -n {2} -k {3} -t {6} -j {7} -l /tmp/latencies_load.{4} -p 0 -e 1200 > /tmp/load.log.{4} 2>&1 &'.format(client_binary_path, config_file_path, 1000000000, workload_trace_dir + load_file_prefix, str(client_id), consensus_config, offset, loading_clients))
			# print(run)
			# os.system(run)

		for i in range(1, 9):
			#we don't want the load output when we download the results, so redirecting to dev null
			if code == 'paxos':
				os.system('{0} -c {1} -m vr -n {2} -k {3} > /tmp/load.log.{4} 2>&1 &'.format(client_binary_path, config_file_path, 125000, workload_trace_dir + load_file_prefix + str(i), str(i)))

		out = 'something'
		while out is not None and len(out) != 0:
			out, err = invoke_cmd('ps aux | grep bench | grep client | grep vr | grep -v py')
			time.sleep(5)

		with open('/tmp/load.log.{0}'.format(str(client_id)), 'r') as f:
			out = f.read()
			if 'Running client' in out:
				break
			else:
				os.system("sudo pkill -9 client")
				print('Load not complete, sleeping for 5s')
				time.sleep(5)
				continue

		break

	if code == 'ionia':
		# check if the filelock is still there
		with open(file_lock, 'w+') as f:
			f.write('done_loading')
		while True:
			with open(file_lock, 'r') as f:
				if 'done_loading_ack' in f.read():
					break
			time.sleep(1)
		# while file_lock.exists():
		# 	# print('Waiting for load to finish... load file lock exists')
		# 	time.sleep(1)


	print('Finished loading; sleeping for 5s')

	os.system('sleep 5')
	if load > 0:
		with open(file_lock, 'w+') as f:
			f.write('done_running')
		sys.exit(0)

	if code == 'rtop' or code == 'curp' or code == 'rtopcomm':
		#making sure that load completed in the background
		ip = ''
		with open('{0}/external_ips'.format(perf_dir)) as f:
			for line in f:
				ip = line.replace('\n', '')
				break

		times_checked = 0
		out = 'something'
		while '1000000' not in out:
			out, err = invoke_remote_cmd(ip, perf_dir + "/pems", "cat /tmp/vrlog* | grep -i lastcommitted")
			out = out.decode(sys.stdout.encoding)
			time.sleep(2)
			times_checked += 1
			if times_checked > 7:
				print('Load did not complete after checking for 7 times... exiting.')
				sys.exit(0)

		print('Load completed in the background. Seen {0}'.format(out.split('\n')[-2]))

with open(file_lock, 'w+') as f:
	f.write('running')

if workload != 'm' and workload != 'e' and workload != 'n':
	workload_file_prefix = workload_trace_dir + 'run' +str(workload) + '/run.' + str(workload) + '.'
else:
		if workload == 'e':
			#/mnt/data/rune/e0/
			#run.e.1
			workload_file_prefix = workload_trace_dir + '/rune/' + workload_trace_prefix + '/' + 'run.e.'
		elif workload == 'm':
			workload_file_prefix = workload_trace_dir + '/exp2-traces/' + workload_trace_prefix + '/' + workload_trace_prefix + '.'
		elif workload == 'n':
			workload_file_prefix = workload_trace_dir + '/2c-traces/' + workload_trace_prefix + '/' + workload_trace_prefix + '.'

# os.system("sudo pkill -9 replica || true")
# os.system("sudo pkill -9 client || true")

for i in range(1, 9):
	os.system("sudo pkill -9 client")
	os.system('sleep 5')


# os.system("rm -rf {0}/lat.*; rm -rf {0}/latencies.*".format(perf_dir))
# if workload == 'r':
# 	os.system('{0}/reqserver/server {1} {2} > /tmp/serverlog 2>&1  &'.format(perf_dir, str(readwindow), str(windowfrac)))
# 	os.system('sleep 1')

# if code == 'ionia' or code == 'skyros':
# 	if workload == 'r':			
# 		os.system('{0} -c {1} -s {7} -m vr -n {2} -k {5} -e {6} -l {4}/latencies.{3} -t {8} -j {9} -w 1 > {4}/lat.{3} 2>&1 &'.format(client_binary_path, config_file_path, num_ops, str(client_id), perf_dir, "nullfile", time_run, consensus_config, str(client_id), clients))
# 	else:
# 		os.system('{0} -c {1} -s {7} -m vr -n {2} -k {5} -e {6} -l {4}/latencies.{3} -t {8} -j {9} -w 1 >> {4}/lat.{3} 2>&1 &'.format(client_binary_path, config_file_path, num_ops, str(client_id), perf_dir, workload_file_prefix, time_run, consensus_config, str(client_id), clients))
# elif code == 'paxos':
# 	if workload == 'r':
# 		os.system('{0} -c {1} -m vr -n {2} -k {5} -e {6} -l {4}/latencies.{3} -w 1 > {4}/lat.{3} 2>&1 &'.format(client_binary_path, config_file_path, num_ops, str(client_id), perf_dir, "nullfile", time_run, clients))
# 	else:
# 		os.system('{0} -c {1} -m vr -n {2} -k {5} -e {6} -l {4}/latencies.{3} -w 1 > {4}/lat.{3} 2>&1 &'.format(client_binary_path, config_file_path, num_ops, str(client_id), perf_dir, workload_file_prefix, time_run, clients))
# else:
# 	assert False

# out = 'something'
# while out is not None and len(out) != 0:
# 	out, err = invoke_cmd('ps aux | grep bench | grep client | grep vr | grep -v py')
# 	time.sleep(2)

iii = 4
while True:
	iii += 1
	client_id__ = client_id + (int(clients) * iii)

	os.system("rm -rf {0}/lat.*; rm -rf {0}/latencies.*".format(perf_dir))
	if workload == 'r':
		os.system('{0}/reqserver/server {1} {2} > /tmp/serverlog 2>&1  &'.format(perf_dir, str(readwindow), str(windowfrac)))
		os.system('sleep 1')

	if code == 'ionia' or code == 'skyros':
		if workload == 'r':			
			os.system('{0} -c {1} -s {7} -m vr -n {2} -k {5} -e {6} -l {4}/latencies.{3} -t {8} -j {9} -w 1 > {4}/lat.{3} 2>&1 &'.format(client_binary_path, config_file_path, num_ops, str(client_id), perf_dir, "nullfile", time_run, consensus_config, str(client_id), clients))
		else:
			os.system('{0} -c {1} -s {7} -m vr -n {2} -k {5} -e {6} -l {4}/latencies.{3} -t {8} -j {9} -w 1 >> {4}/lat.{3} 2>&1 &'.format(client_binary_path, config_file_path, num_ops, str(client_id), perf_dir, workload_file_prefix, time_run, consensus_config, client_id__, clients))
	elif code == 'paxos':
		if workload == 'r':
			os.system('{0} -c {1} -m vr -n {2} -k {5} -e {6} -l {4}/latencies.{3} -w 1 > {4}/lat.{3} 2>&1 &'.format(client_binary_path, config_file_path, num_ops, str(client_id), perf_dir, "nullfile", time_run, clients))
		else:
			os.system('{0} -c {1} -m vr -n {2} -k {5} -e {6} -l {4}/latencies.{3} -w 1 > {4}/lat.{3} 2>&1 &'.format(client_binary_path, config_file_path, num_ops, str(client_id), perf_dir, workload_file_prefix, time_run, clients))
	else:
		assert False

	out = 'something'
	while out is not None and len(out) != 0:
		out, err = invoke_cmd('ps aux | grep bench | grep client | grep vr | grep -v py')
		time.sleep(2)

	good = True
	with open(f'{perf_dir}/lat.{str(client_id)}', 'r') as f:
		for line in f:
			if 'terminate called after throwing an instance' in line:
				os.system('sleep 5')
				good = False
				break
	
	if good:
		break

with open(file_lock, 'w+') as f:
	f.write('done_running')

with open('{0}/lat.{1}'.format(perf_dir, str(client_id)), 'r') as f:
	for line in f:
		print(line.replace('\n', ''))

os.system('killall -s 9 server')

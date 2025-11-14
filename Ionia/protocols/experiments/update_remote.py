#!/usr/bin/env python
import os
import sys
import subprocess
import time
import copy
import random
import threading


machines = {}
hostnames = {}
internal_ips = {}
machines = []
remote_user_name = 'aganesn2'
CURR_DIR = os.path.dirname(os.path.abspath(__file__))


def run_remote(machine_ip, command):
	cmd = 'ssh {0}@{1} \'{2}\''.format(remote_user_name, machine_ip, command)
	#print (cmd)
	os.system(cmd)

def invoke_cmd(command):
	p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out, err = p.communicate()
	return (out, err)

def parallel_start_and_join(threads):
	map(lambda t: t.start(), threads)
	map(lambda t: t.join(), threads)

def update_machine(src_machine):
	run_remote(src_machine, "cd /mnt/sda4/replicated_kvstore/protocols/src/ionia/; git pull; make -j; git status")

def main():
	with open('./external_ips', 'r') as ip_reader:
		for line in ip_reader:
			line = line.replace('\n','').replace('\t','')
			machines.append(line)

	print machines
	parallel_start_and_join([threading.Thread(target=update_machine, args=(str(m),)) for m in machines])

if __name__ == '__main__':
	main()

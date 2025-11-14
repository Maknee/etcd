#!/bin/bash


r_medium='data'
r_time=10 # time in seconds to run each single point in the graph
r_cluster='cloudlab' 
r_user='ramn' # aws user
r_system='vr' #VR for all protocol variants including Skyros
workload='t' #write-only workload for throughput latency graphs

# initial setup
#chmod 0400 ../../pems/$r_cluster.pem

#./update_sources.py

# Ionia
for n in 1; do
for i in 1; do
for code in ionia; do
        echo ../../remote-throughput.py --medium $r_medium --code $code --time $r_time --run $i --cluster $r_cluster --sync no --user $r_user --workload $workload --num_nodes 3 --target_system_name $r_system --sync_rep_factor 2 --num_clients $n --leader_reads yes --batch 64
        sleep 2
done
done
done

#rm -rf ./$workload.$r_system.rtop.*
#mv ../../$workload.$r_system.rtop.* .

#./plot_graphs.sh
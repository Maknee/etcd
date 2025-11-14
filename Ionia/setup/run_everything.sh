#!/bin/bash

# declare -a RUNS=("10_90" "25_75" "a" "75_25" "c")
# declare -a RUNS=("a" "25_75" "75_25" "10_90" "c")
# declare -a RUNS=("10_90" "c")
# declare -a RUNS=("10_90" "c" "a" "25_75" "75_25")
# declare -a RUNS=("10_90" "25_75" "a" "75_25" "c")
# declare -a RUNS=("c" "75_25" "a" "25_75" "10_90")
# declare -a RUNS=("c" "75_25" "a" "25_75" "10_90")
# declare -a RUNS=("a" "25_75" "75_25" "10_90" "c")
# declare -a RUNS=("a" "25_75" "75_25" "10_90" "c" "b" "f")
# declare -a RUNS=("a" "25_75" "75_25" "10_90")
# declare -a CLIENTS=("4")
# declare -a CLIENTS=("1" "2" "3" "4")
# declare -a CLIENTS=("7" "6" "5" "3")
declare -a RUNS=("a")
# declare -a CLIENTS=("4" "3" "2" "1")
# declare -a CLIENTS=("2")
# declare -a CLIENTS=("8" "6" "4" "2")
# declare -a CLIENTS=("1" "2" "3" "4")
declare -a CLIENTS=("4")
# declare -a CLIENTS=("4" "3" "2")
# declare -a CLIENTS=("4")
# declare -a CLIENTS=("1")

CURDIR=$(pwd)
CURTIME=$(date "+%Y_%m_%d-%I_%M_%S%p")
for j in "${CLIENTS[@]}"
do
    for i in "${RUNS[@]}"
    do
        echo "$i"
        i_data="${CURDIR}/${CURTIME}/${j}/${i}"
        mkdir -p $i_data
        python skyros_upload.py --step=6
        cd experiments
        until ./remote-throughput.py --medium data --code ionia --time 20000 --run 1 --cluster cloudlab --user makneee --workload $i --num_nodes 5 --target_system_name vr --num_clients $j --batch 64
        do
            echo "Failure remote-throughput.py, retrying..." >> failures.txt
            cd $CURDIR
            python skyros_upload.py --step=6
            cd experiments
        done
        python calc.py $i.vr.ionia.data.run1.dir/ > $i_data/throughput_raw
        python calc.py $i.vr.ionia.data.run1.dir/ | tail -n 1 > $i_data/throughput
        cp $i.vr.ionia.data.run1.dir/*.cli* $i_data/
        cp -r $i.vr.ionia.data.run1.dir/ $i_data/
        cd $CURDIR
        declare -a SERVERS=("1" "2" "3")
        for s in "${SERVERS[@]}"
        do
            python grab_data.py --hostname=$(sed -n ${s}p experiments/external_ips)
            mv iostat $i_data/iostat${s}
            mv iostat.csv $i_data/iostat${s}.csv
            mv load_iostat $i_data/load_iostat${s}
            mv load_iostat.csv $i_data/load_iostat${s}.csv
            mv glances $i_data/glances${s}
            mv glances_cores.csv $i_data/glances_cores${s}.csv
            mv glances.csv $i_data/glances${s}.csv
            mv vrlog $i_data/vrlog${s}
            mv vrlog.csv $i_data/vrlog${s}.csv

            cd $i_data
            python $CURDIR/input_graph.py --filename "replicated_workload_${i}_670_160_" --title "ionia replicated workload ${i} read/writes 670M/160M" --index ${s}
            cd $CURDIR
        done

        #declare -a LOADING_REPLICAS=("1" "2" "3" "4")
	    LOADING_REPLICAS=($(seq 1 1 $j))
        i_load_data="${CURDIR}/${CURTIME}/${j}/${i}/load_data"
        mkdir -p $i_load_data
        for s in "${LOADING_REPLICAS[@]}"
        do
            python grab_data_replica.py --hostname=$(sed -n $((s + 3))p experiments/external_ips) --index=$((s - 1))
            mv load.log.$((s - 1)) $i_load_data/load.load.run.${s}
            mv latencies_load.$((s - 1)).raw $i_load_data/latencies_load.${s}.raw
            python $CURDIR/read_latency.py --latencyfile $i_load_data/latencies_load.${s}.raw --readcdf $i_load_data/readcdf${s}.png > $i_load_data/readcdf${s}.txt
        done
        cat $i_load_data/readcdf*.txt | grep "Average total latency:" | awk '{print $4}' | awk -v c="$j" -v OFMT='%f' '{s+=$1} END {print s/c}' > $i_load_data/latencies_load
        cat $i_load_data/readcdf*.txt | grep "Average read latency" | awk '{print $4}' | awk -v c="$j" -v OFMT='%f' '{s+=$1} END {print s/c}' > $i_load_data/latencies_read_load
        cat $i_load_data/readcdf*.txt | grep "Average write latency" | awk '{print $4}' | awk -v c="$j" -v OFMT='%f' '{s+=$1} END {print s/c}' > $i_load_data/latencies_write_load
        python experiments/calc.py $i_load_data > $i_load_data/throughput_raw
        python experiments/calc.py $i_load_data | tail -n 1 > $i_load_data/throughput

        cd $i_data
        for ((k = 0; k < $j; ++k)); do
            python $CURDIR/read_latency.py --latencyfile $i.vr.ionia.data.run1.dir/latencies.$k.raw --readcdf readcdf${k}.png > readcdf${k}.txt
        done
        cat $i_data/readcdf*.txt | grep "Average total latency:" | awk '{print $4}' | awk -v c="$j" -v OFMT='%f' '{s+=$1} END {print s/c}' > $i_data/latencies_load
        cat $i_data/readcdf*.txt | grep "Average read latency" | awk '{print $4}' | awk -v c="$j" -v OFMT='%f' '{s+=$1} END {print s/c}' > $i_data/latencies_read_load
        cat $i_data/readcdf*.txt | grep "Average write latency" | awk '{print $4}' | awk -v c="$j" -v OFMT='%f' '{s+=$1} END {print s/c}' > $i_data/latencies_write_load
        cd $CURDIR
    done
done

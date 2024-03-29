#!/usr/bin/env bash

#Using VICCI confs
set -u
set -x

if [ $# -lt 1 ]; then
    echo "$0: [# servers] [email]"
    exit
fi



nservers=$1
dcl_config=${nservers}_in_vicci
client_config=${nservers}_clients_in_vicci

cops_root_dir="$HOME/COPS-SNOW"
cops2_root_dir="$HOME/COPS2-SNOW"
eiger_root_dir="$HOME/eiger"
contr_root_dir="$HOME/contrarion"
contr2_root_dir="$HOME/contrarion2"


exp_dir="${cops_root_dir}/experiments"  #shared with other algos

output_dir_base="${exp_dir}/exp10" #shared
exp_uid=$(date +%s)
output_dir="${output_dir_base}/${exp_uid}" #shared
mkdir -p ${output_dir}
rm $output_dir_base/latest
ln -s $output_dir $output_dir_base/latest


dcl_config_full="${cops_root_dir}/vicci_dcl_config/${dcl_config}" #shared

all_servers=($(cat $dcl_config_full | grep cassandra_ips | awk -F"=" '{ print $2 }' | xargs))
all_servers=$(echo "echo ${all_servers[@]}" | bash)
num_dcs=$(cat $dcl_config_full | grep num_dcs | awk -F"=" '{ print $2 }')

strategy_properties="DC0:1"
for i in $(seq 1 $((num_dcs-1))); do
    strategy_properties=$(echo ${strategy_properties}",DC${i}:1")
done

num_servers=$(echo $all_servers | wc -w)
num_servers_per_dc=$((num_servers / num_dcs))

for dc in $(seq 0 $((num_dcs-1))); do
    this_dc_servers=$(echo $all_servers | sed 's/ /\n/g' | head -n $((num_servers_per_dc * (dc+1))) | tail -n $num_servers_per_dc | xargs)
    servers_by_dc[$dc]=${this_dc_servers}
done
echo ${servers_by_dc[@]}



client_config_full="${cops_root_dir}/vicci_dcl_config/${client_config}" #shared

all_clients=$(cat $client_config_full | grep cassandra_ips | awk -F"=" '{ print $2 }' | xargs)
all_clients=$(echo "echo ${all_clients[@]}" | bash)

num_clients=$(echo $all_clients | wc -w)
num_clients_per_dc=$((num_clients / num_dcs))

for dc in $(seq 0 $((num_dcs-1))); do
    this_dc_clients=$(echo $all_clients | sed 's/ /\n/g' | head -n $((num_clients_per_dc * (dc+1))) | tail -n $num_clients_per_dc | xargs)
    clients_by_dc[$dc]=${this_dc_clients}
done
echo ${clients_by_dc[@]}

kill_all_cmd="${cops_root_dir}/vicci_cassandra_killer.bash ${cops_root_dir}/vicci_dcl_config/${dcl_config}" #shared
stress_killer="${cops_root_dir}/kill_stress_vicci.bash" #shared


exp_num=0


gather_results() {
    local root_dir=$1
    local exp_name=$2
    local exp_output_dir=${output_dir}/${exp_name}
    mkdir -p ${exp_output_dir}
    log_dir=${exp_output_dir}/logs/exp${exp_num}
    mkdir -p ${log_dir}
    for dc in $(seq 0 $((num_dcs - 1))); do
        for srv_index in $(seq 0 $((num_servers_per_dc - 1))); do
            server=$(echo ${servers_by_dc[$dc]} | sed 's/ /\n/g' | head -n $((srv_index + 1)) | tail -n 1)
            rsync -az $server:${root_dir}/cassandra_var/cassandra* ${log_dir} & #separate log directory for cassandra and algo
            if [[ "$exp_name" == "cops" ]]; then
                rsync -az $server:${root_dir}/cops.data ${log_dir}/cops_${dc}_${srv_index}.data
            fi
        done
        wait
        for cli_index in $(seq 0 $((num_clients_per_dc - 1))); do
            client_dir=${exp_output_dir}/client${dc}_${cli_index}
            client=$(echo ${clients_by_dc[$dc]} | sed 's/ /\n/g' | head -n $((cli_index+1)) | tail -n 1)
            rsync -az $client:${exp_output_dir}/* ${client_dir} & #shared output dir
            rsync -az $client:${root_dir}/tools/stress/stress.log ${log_dir}/client_${dc}_${cli_index}.log & #Only saving 1 log
        done
        wait
        for cli_index in $(seq 0 $((num_clients_per_dc - 1))); do
            client=$(echo ${clients_by_dc[$dc]} | sed 's/ /\n/g' | head -n $((cli_index+1)) | tail -n 1)
            ssh $client -o StrictHostKeyChecking=no "rm -f ${root_dir}/tools/stress/stress.log*" &
        done
        wait
    done
}


cleanup() {
    echo "Killing everything"
    ${cops_root_dir}/kill_all.bash $nservers #works for both
    #    gather_results $cops_root_dir cops
    #    gather_results $eiger_root_dir eiger
    #    gather_results $contr_root_dir contrarion
}


trap cleanup EXIT
#get cluster up an running
internal_cluster_start_cmd() {
    cur_dir=$PWD
    root_dir=$1
    cd ${root_dir};
    ${kill_all_cmd};
    sleep 1;
    while [ 1 ]; do
        ./vicci_dc_launcher.bash ${dcl_config_full}  #calling respective launcher using COPS config
        return_value=$?
        if [ ${return_value} -eq 0 ]; then
            break
        fi
    done
    cd ${cur_dir};
}


internal_populate_cluster() {
    local root_dir=$1
    local insert_cmd=$2
    local total_keys=$3
    local max_columns=$4
    local column_size=$5
    local column_per_key_write=$6
    local exp_name=$7
    local exp_output_dir=${output_dir}/${exp_name}
    #set the keyspace
    for i in $(seq 3); do
        first_dc_servers_csv=$(echo ${servers_by_dc[0]} | sed 's/ /,/g')

        # set up a killall for stress in case it hangs
        (sleep 60; killall stress) &
        killall_jck_pid=$!
        ${root_dir}/tools/stress/bin/stress --nodes=$first_dc_servers_csv --just-create-keyspace --replication-strategy=NetworkTopologyStrategy --strategy-properties=$strategy_properties
        kill $killall_jck_pid
        sleep 5
    done

    populate_attempts=0
    while [ 1 ]; do

        KILLALL_SSH_TIME=400
        MAX_ATTEMPTS=10
        (sleep $KILLALL_SSH_TIME; killall ssh) &
        killall_ssh_pid=$!

        #divide keys across first cluster clients only
        keys_per_client=$((total_keys / num_clients_per_dc))

        pop_pids=""
        for dc in 0; do
            local_servers_csv=$(echo ${servers_by_dc[$dc]} | sed 's/ /,/g')

            for cli_index in $(seq 0 $((num_clients_per_dc - 1))); do
                client=$(echo ${clients_by_dc[$dc]} | sed 's/ /\n/g' | head -n $((cli_index+1)) | tail -n 1)

                #all_servers_csv=$(echo $all_servers | sed 's/ /,/g')
                first_dc_servers_csv=$(echo ${servers_by_dc[0]} | sed 's/ /,/g')

                #write to ALL so the cluster is populated everywhere

                #output is shared bw Contrarion, COPS and EIGER
                #stress is called of respective algo
                ssh $client -o StrictHostKeyChecking=no "\
                    mkdir -p ${exp_output_dir}; \
                    $stress_killer; sleep 1; \
                    cd ${root_dir}/tools/stress; \
                    bin/stress \
                    --nodes=$first_dc_servers_csv \
                    --columns=$max_columns \
                    --column-size=$column_size \
                    --operation=${insert_cmd} \
                    --consistency-level=LOCAL_QUORUM \
                    --replication-strategy=NetworkTopologyStrategy \
                    --strategy-properties=$strategy_properties \
                    --num-different-keys=$keys_per_client \
                    --num-keys=$keys_per_client \
                    --stress-index=$cli_index \
                    --stress-count=$num_clients_per_dc \
                    --num-dcs=$num_dcs \
                    --dc-index=$dc \
                     > >(tee ${exp_output_dir}/populate.out) \
                    2> >(tee ${exp_output_dir}/populate.err) \
                    " 2>&1 | awk '{ print "'$client': "$0 }' &
                                    pop_pid=$!
                                    pop_pids="$pop_pids $pop_pid"
                sleep 1
            done
        done

        #wait for clients to finish
        for pop_pid in $pop_pids; do
            echo "Waiting on $pop_pid"
            wait $pop_pid
        done

        sleep 1

        kill $killall_ssh_pid
        # if we kill killall successfully, it will return 0 and that means we populated the cluster and can continue
        #  otherwise we try again
        killed_killall=$?

        if [ $killed_killall == "0" ]; then
            break;
        fi
        ((populate_attempts++))
        if [[ $populate_attempts -ge $MAX_ATTEMPTS ]]; then
            echo -e "\n\n \e[01;31m Could not populate the cluster after $MAX_ATTEMPTS attempts \e[0m \n\n"
            exit;
        fi
        echo -e "\e[01;31m Failed populating $populate_attempts times, trying again (out of $MAX_ATTEMPTS) \e[0m"
    done
}

run_exp10() {

    exp_num=$((exp_num + 1))
    #external vars :: output_dir, servers by dc, num servers, num clients per dc, clients by dc, strategy properties
    local keys_per_serv=$1
    local num_serv_dc=$2
    local column_size=$3
    local keys_per_read=$4
    local write_frac=$5
    local zipf_const=$6
    local active_clients_per_dc=${7}
    local num_threads=$8  #this is total number of threads in the entire DC
    local exp_time=$9
    local trial=$10
    local root_dir=${11}
    local exp_name=${12}
    local cli_output_dir="$output_dir/${exp_name}/trial${trial}/"
    local data_file_name=$1_${num_dcs}_$2_$3_$4_$5_$6_${7}x${8}_$9+${10}+data

    for dc in $(seq 0 $((num_dcs - 1))); do
        local local_servers_csv=$(echo ${servers_by_dc[$dc]} | sed 's/ /,/g')
        for cli_index in $(seq 0 $((active_clients_per_dc - 1))); do
            local client=$(echo ${clients_by_dc[$dc]} | sed 's/ /\n/g' | head -n $((cli_index + 1)) | tail -n 1)
            ssh $client -o StrictHostKeyChecking=no "\
            mkdir -p $cli_output_dir; \
            cd ${root_dir}/tools/stress; \
            timeout 6m bin/stress \
            --progress-interval=1 \
            --nodes=$local_servers_csv \
            --operation=EXP10 \
            --consistency-level=LOCAL_QUORUM \
            --replication-strategy=NetworkTopologyStrategy \
            --strategy-properties=$strategy_properties \
            --keys-per-server=$keys_per_serv \
            --num-servers-per-dc=$num_serv_dc \
            --stress-index=$cli_index \
            --stress-count=$active_clients_per_dc \
            --num-dcs=$num_dcs \
            --dc-index=$dc \
            --num-keys=20000000 \
            --column-size=$column_size \
            --keys-per-read=$keys_per_read \
            --write-fraction=$write_frac \
            --threads=$num_threads \
            --zipfian-constant=$zipf_const \
            --use-per-node-zipf \
            --expt-duration=$exp_time \
             > >(tee ${cli_output_dir}/${data_file_name}) \
            2> ${cli_output_dir}/${data_file_name}.stderr" \
            2>&1 | awk '{ print "'$client': "$0 }' &
        done
    done
    #wait for clients to finish
    wait
}

process_exp10() {
    local keys_per_serv=$1
    local num_serv=$2
    local column_size=$3
    local keys_per_read=$4
    local write_frac=$5
    local zipf_const=$6
    local data_file_name=$1_${num_dcs}_$2_$3_$4_$5_$6
    find $output_dir -name "${data_file_name}_*.stderr" | xargs -n1  grep -E 'COPS|Eiger|Contrarion' >> "${output_dir}/${data_file_name}.csv"
}

run_all() {
    local value_size=$1
    local write_frac=$2
    local keys_per_read=$3
    local zipf_c=$4
    local num_active_clients=$5
    local numT=$6

    echo "Exp $((exp_num + 1)) :: Contrarion trial=$trial value_size=$value_size zipf=$zipf_c numKeys=$keys_per_read write_frac=$write_frac  numClients = $num_active_clients numT=$numT started at $(date)" >> ~/progress
    internal_cluster_start_cmd ${contr_root_dir}
    internal_populate_cluster ${contr_root_dir} INSERTCL ${total_keys} 1 ${value_size} 1 contrarion
    run_exp10 ${keys_per_server} ${num_servers_per_dc} ${value_size} ${keys_per_read} ${write_frac} ${zipf_c} ${num_active_clients} ${numT} ${run_time} ${trial} ${contr_root_dir} contrarion
    ${kill_all_cmd}
    gather_results ${contr_root_dir} contrarion


    echo "Exp $((exp_num + 1)) :: Contrarion2 trial=$trial value_size=$value_size zipf=$zipf_c numKeys=$keys_per_read write_frac=$write_frac  numClients = $num_active_clients numT=$numT started at $(date)" >> ~/progress
    internal_cluster_start_cmd ${contr2_root_dir}
    internal_populate_cluster ${contr2_root_dir} INSERTCL ${total_keys} 1 ${value_size} 1 contrarion2
    run_exp10 ${keys_per_server} ${num_servers_per_dc} ${value_size} ${keys_per_read} ${write_frac} ${zipf_c} ${num_active_clients} ${numT} ${run_time} ${trial} ${contr2_root_dir} contrarion2
    ${kill_all_cmd}
    gather_results ${contr2_root_dir} contrarion2

    echo "Exp $((exp_num + 1)) :: COPS trial=$trial value_size=$value_size zipf=$zipf_c numKeys=$keys_per_read write_frac=$write_frac numClients = $num_active_clients numT=$numT started at $(date)" >> ~/progress
    internal_cluster_start_cmd ${cops_root_dir}
    internal_populate_cluster ${cops_root_dir} INSERTCL ${total_keys} 1 ${value_size} 1 cops
    run_exp10 ${keys_per_server} ${num_servers_per_dc} ${value_size} ${keys_per_read} ${write_frac} ${zipf_c} ${num_active_clients} ${numT} ${run_time} ${trial} ${cops_root_dir} cops
    ${kill_all_cmd}
    gather_results ${cops_root_dir} cops

    echo "Exp $((exp_num + 1)) :: COPS2 trial=$trial value_size=$value_size zipf=$zipf_c numKeys=$keys_per_read write_frac=$write_frac numClients = $num_active_clients numT=$numT started at $(date)" >> ~/progress
    internal_cluster_start_cmd ${cops2_root_dir}
    internal_populate_cluster ${cops2_root_dir} INSERTCL ${total_keys} 1 ${value_size} 1 cops2
    run_exp10 ${keys_per_server} ${num_servers_per_dc} ${value_size} ${keys_per_read} ${write_frac} ${zipf_c} ${num_active_clients} ${numT} ${run_time} ${trial} ${cops2_root_dir} cops2
    ${kill_all_cmd}
    gather_results ${cops2_root_dir} cops2

    echo "Exp $((exp_num + 1)) :: Eiger trial=$trial value_size=$value_size zipf=$zipf_c numKeys=$keys_per_read write_frac=$write_frac numClients = $num_active_clients numT=$numT started at $(date)" >> ~/progress
    internal_cluster_start_cmd ${eiger_root_dir}
    internal_populate_cluster ${eiger_root_dir} INSERTCL ${total_keys} 1 ${value_size} 1 eiger
    run_exp10 ${keys_per_server} ${num_servers_per_dc} ${value_size} ${keys_per_read} ${write_frac} ${zipf_c} ${num_active_clients} ${numT} ${run_time} ${trial} ${eiger_root_dir} eiger
    ${kill_all_cmd}
    gather_results ${eiger_root_dir} eiger
}
rm -f ~/progress
keys_per_server=100000 #TODO increase to 1M
total_keys=$((keys_per_server*num_servers_per_dc))
run_time=170   #Timeout is set to 6minutes

for allparams in `cat ${cops_root_dir}/allparams.txt`
do
    trial=1
    value_size=`echo $allparams | cut -d: -f1`
    write_frac=`echo $allparams | cut -d: -f2`
    keys_per_read=`echo $allparams | cut -d: -f3`
    zipf_c=`echo $allparams | cut -d: -f4`
    for numTotalThreads in 1 8 14 56 112
    do
       if [ $numTotalThreads -lt $num_clients_per_dc ]; then
            num_active_clients=$numTotalThreads
       else
	    num_active_clients=${num_clients_per_dc}
       fi
       run_all ${value_size} ${write_frac} ${keys_per_read} ${zipf_c} ${num_active_clients} ${numTotalThreads} 
    done
    process_exp10 ${keys_per_server} ${num_servers_per_dc} ${value_size} ${keys_per_read} ${write_frac} ${zipf_c}
done

if [ $# -ge 2 ]; then
    echo "$1 server experiment complete" | mail -s "Expt progress" $2
fi


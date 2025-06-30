#!/bin/bash

# Get the absolute path to the database, located in the same directory as the script.
db_name="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/test.db"

# =========================================================================
# JOB MANAGEMENT FUNCTIONS (Your Original Functions)
# =========================================================================

function updateStatus(){ #jobid stautus
    sqlite3 "$db_name" "UPDATE jobs SET STATUS='$2' WHERE JOBID='$1';"
}

function jobSubmitter(){
    count=$(sqlite3 "$db_name" "SELECT COUNT(*) FROM jobs WHERE LOCATION='$1';")
    if [ "$count" -gt 0 ]; then
        echo "Job already submitted"
        return
    fi
    result=$(sqlite3 "$db_name" "SELECT * FROM jobs WHERE STATUS=1;");
    if [[ -n "$result" ]]; then
        while IFS='|' read -r id jobid status location type script ; do
            cd "$location"
            if [ "$type" = "slurm" ];then
                newjobid=$(sbatch "$script")
                newjobid=${newjobid##* }
                echo "Jobid: $newjobid"
                sqlite3 "$db_name" "UPDATE jobs SET JOBID='$newjobid' WHERE JOBID='$jobid';"
                updateStatus "$newjobid" 2
            elif [ "$type" = "bash" ];then
                bash "$script"
            fi
            updateStatus "$jobid" 2
        done <<< "$result";
    fi
}

function checkJob(){ # jobid
    status=$(squeue -h -j "$1" -o "%T")
    if [[ -z "$status" ]]; then
        echo "The job $1 has finished"
        return 1;
    else
        case $status in
        "RUNNING")   echo "The job $1 is running"; return 0 ;;
        "PENDING")   echo "The job $1 is pending in queue"; return 2 ;;
        *)           echo "The job $1 is in: $status"; return 3 ;;
        esac
    fi
}

function statusUpdater(){
    result=$(sqlite3 "$db_name" "SELECT jobid FROM jobs;");
    while IFS=, read -r jobid; do
        checkJob "$jobid"
        case $? in
        0) updateStatus "$jobid" 0 ;;
        1) updateStatus "$jobid" 1 ;;
        2) updateStatus "$jobid" 2 ;;
        3) updateStatus "$jobid" 3 ;;
        esac
    done <<< "$result";
}

# ... (The rest of your job management functions: submitFirstJob, deleteJob, etc. remain here unchanged) ...
function submitFirstJob(){ # *location, script, resume script
    cd "$1"
    if [ $# -lt 2 ]; then
        if [ ! -f "start.sh" ]; then
            echo "start.sh not found in the directory"
            return
        fi
        newjobid=$(sbatch start.sh)
    else
        if [ ! -f "$2" ]; then
            echo "$2 not found in the directory"
            return
        fi
        newjobid=$(sbatch $2)
    fi
    newjobid=${newjobid##* }
    echo "Jobid: $newjobid"
    if [ $# -lt 3 ]; then
        sqlite3 "$db_name" "INSERT INTO jobs (jobid,status,location,type,script) VALUES ('$newjobid',2,'$1','slurm','resume.sh');";
    else
        if [ ! -f "$3" ]; then
            echo "$3 not found in the directory"
            return
        fi
        sqlite3 "$db_name" "INSERT INTO jobs (jobid,status,location,type,script) VALUES ('$newjobid',2,'$1','slurm','$3');"
    fi
}

function deleteJob(){ #jobid
    if [ $# -eq 0 ];then
        result=$(sqlite3 "$db_name" "SELECT * FROM jobs;")
        while IFS='|' read -r id jobid status location type script ; do
            scancel "$jobid"
        done <<< "$result";
        sqlite3 "$db_name" "DELETE FROM jobs;"
    else
        sqlite3 "$db_name" "DELETE FROM jobs WHERE JOBID='$1';"
        scancel "$1"
    fi
}

function deleteJobById(){ #id
    if [ $# -eq 0 ];then
        echo "Missing id"
    else
        result=$(sqlite3 "$db_name" "SELECT JOBID FROM jobs WHERE ID='$1';")
        sqlite3 "$db_name" "DELETE FROM jobs WHERE ID='$1';"
        scancel "$result"
    fi
}

function restartJob(){ #jobid startScript, resumeScript
    result=$(sqlite3 "$db_name" "SELECT * FROM jobs WHERE JOBID='$1';");
    if [[ -n "$result" ]]; then
        IFS='|' read -r id jobid status location type script<<<"$result"
        deleteJob "$1"
        if [ $# -eq 3 ]; then
            submitFirstJob "$location" "$2" "$3"
        elif [ $# -eq 2 ]; then
            submitFirstJob "$location" "$2"
        else
            submitFirstJob "$location"
        fi
    fi
}

function plotter(){ #plotScript
    result=$(sqlite3 "$db_name" "SELECT * FROM jobs;")
    while IFS='|' read -r id jobid status location type script ; do
        cd "$location"
        pwd
        eval bash "$1" &
    done <<< "$result";
}


# =========================================================================
# GPU STATUS FUNCTIONS (NEWLY ADDED AND INTEGRATED)
# =========================================================================

# Helper function to parse a Slurm TRES string for GPU info.
function _parse_tres_string() {
    local tres_string="$1"
    local -n result_map=$2; for k in "${!result_map[@]}"; do unset result_map["$k"]; done
    [[ -z "$tres_string" || ! "$tres_string" == *gres/gpu* ]] && return
    IFS=',' read -ra tres_parts <<< "$tres_string"
    for part in "${tres_parts[@]}"; do
        if [[ "$part" == gres/gpu* ]]; then
            local gres_key="${part%%=*}"; local gres_count="${part#*=}"
            local gpu_type="gpu"
            if [[ "$gres_key" == gres/gpu:* ]]; then gpu_type="${gres_key#gres/gpu:}"; elif [[ "$gres_key" != "gres/gpu" ]]; then continue; fi
            if [[ "$gres_count" =~ ^[0-9]+$ ]] && [ "$gres_count" -ge 0 ]; then result_map["$gpu_type"]=$(( ${result_map["$gpu_type"]:-0} + gres_count )); fi
        fi
    done
}

# Helper function to update or insert a single GPU row in the database.
function _gpu_upsert_row() {
    local db_path="$1"
    local node_name="$2"
    local gpu_type="$3"
    local available_count="$4"
    local used_count="$5"
    local conf_count="$6"
    local sql_command="
    INSERT INTO gpu (node, GPU_Type, GPU_available, GPU_used, GPU_configured, last_updated)
    VALUES ('$node_name', '$gpu_type', $available_count, $used_count, $conf_count, CURRENT_TIMESTAMP)
    ON CONFLICT(node, GPU_Type) DO UPDATE SET
        GPU_available = excluded.GPU_available,
        GPU_used = excluded.GPU_used,
        GPU_configured = excluded.GPU_configured,
        last_updated = CURRENT_TIMESTAMP;"
    sqlite3 "$db_path" "$sql_command"
}

# Helper function to remove stale node entries from the database.
function _gpu_cleanup_stale_nodes() {
    local db_path="$1"
    local operational_nodes_list="$2"
    local nodes_in_db; nodes_in_db=$(sqlite3 "$db_path" "SELECT DISTINCT node FROM gpu;")
    for node in $nodes_in_db; do
        if ! echo "$operational_nodes_list" | grep -qw "$node"; then
            echo "INFO: Removing stale DB entries for non-operational node: $node" >&2
            sqlite3 "$db_path" "DELETE FROM gpu WHERE node = '$node';"
        fi
    done
}

# Main function to poll Slurm and update the GPU status in the database.
function updateGpuStatus() {
    echo "Probing Slurm and updating GPU status in database: $db_name"
    echo "=========================================================================="
    printf "%-20s %-25s %-10s %-10s %-10s\n" "Node" "GPU_Type" "Configured" "Used" "Available"
    echo "-------------------- ------------------------- ---------- ---------- ----------"

    local operational_nodes; operational_nodes=$(sinfo -N -h -o "%N %T" | \
        grep -v -E 'DOWN|DRAIN|DRNG|FAIL|MAINT|POWER_DOWN|POWER_UP|REBOOT|UNK' | \
        awk '{print $1}' | sort -u)

    if [ -z "$operational_nodes" ]; then
        echo "No operational nodes found. Cleaning up all GPU entries from DB."
        sqlite3 "$db_name" "DELETE FROM gpu;"
        return 1
    fi

    _gpu_cleanup_stale_nodes "$db_name" "$operational_nodes"

    declare -A total_available_gpus_by_type
    local any_node_printed=0

    for node_name in $operational_nodes; do
        local scontrol_node_output; scontrol_node_output=$(scontrol show node -o "$node_name" 2>/dev/null)
        if [ -z "$scontrol_node_output" ]; then continue; fi

        local cfg_tres_str; cfg_tres_str=$(echo "$scontrol_node_output" | awk '{for(i=1;i<=NF;i++) if($i ~ /^CfgTRES=/) {sub(/^CfgTRES=/, "", $i); print $i; exit}}')
        local alloc_tres_str; alloc_tres_str=$(echo "$scontrol_node_output" | awk '{for(i=1;i<=NF;i++) if($i ~ /^AllocTRES=/) {sub(/^AllocTRES=/, "", $i); print $i; exit}}')

        if [[ "$cfg_tres_str" != *gres/gpu* ]]; then continue; fi

        declare -A configured_gpus_map; declare -A used_gpus_map
        _parse_tres_string "$cfg_tres_str" configured_gpus_map
        _parse_tres_string "$alloc_tres_str" used_gpus_map

        # Pruning logic for generic "gpu" key
        if [[ -n "${configured_gpus_map["gpu"]}" ]]; then
            local has_specific=0; for key in "${!configured_gpus_map[@]}"; do if [[ "$key" != "gpu" ]]; then has_specific=1; break; fi; done
            if [[ "$has_specific" -eq 1 ]]; then unset configured_gpus_map["gpu"]; fi
        fi

        for gpu_type in "${!configured_gpus_map[@]}"; do
            local conf_count=${configured_gpus_map[$gpu_type]}
            local used_count=${used_gpus_map[$gpu_type]:-0}
            local available_count=$(( conf_count - used_count ))
            if [ "$available_count" -lt 0 ]; then available_count=0; fi

            _gpu_upsert_row "$db_name" "$node_name" "$gpu_type" "$available_count" "$used_count" "$conf_count"

            if [ "$available_count" -gt 0 ]; then
                printf "%-20s %-25s %-10s %-10s %-10s\n" \
                    "$node_name" "$gpu_type" "$conf_count" "$used_count" "$available_count"
                any_node_printed=1
                total_available_gpus_by_type["$gpu_type"]=$(( ${total_available_gpus_by_type["$gpu_type"]:-0} + available_count ))
            fi
        done
    done

    echo "=========================================================================="
    echo "Database '$db_name' has been updated with the latest GPU status."
    echo
    if [ "$any_node_printed" -eq 1 ]; then
        echo "Overall summary of currently available GPUs (by type):"
        local sorted_gpu_types; sorted_gpu_types=($(for t in "${!total_available_gpus_by_type[@]}"; do echo "$t"; done | sort))
        for gpu_type in "${sorted_gpu_types[@]}"; do
            if [ "${total_available_gpus_by_type[$gpu_type]}" -gt 0 ]; then
                printf "  %-25s : %s\n" "$gpu_type" "${total_available_gpus_by_type[$gpu_type]}"
            fi
        done
        echo "--------------------------------------------------------------------------"
    else
        echo "No nodes found with currently available GPUs."
    fi
}


# =========================================================================
# MAIN SCRIPT LOGIC AND COMMAND PARSER
# =========================================================================

if [ $# -eq 0 ];then
    echo "Main function called (updating job status and submitting pending)"
    statusUpdater
    jobSubmitter

elif [ "$1" = "init" ];then
    echo "Initializing database '$db_name'..."
    sqlite3 "$db_name" "CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, jobid TEXT, status INTEGER, location TEXT, type TEXT, script TEXT);"
    sqlite3 "$db_name" "CREATE TABLE IF NOT EXISTS gpu (
        node TEXT NOT NULL,
        GPU_Type TEXT NOT NULL,
        GPU_available INTEGER NOT NULL,
        GPU_used INTEGER NOT NULL,
        GPU_configured INTEGER NOT NULL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        PRIMARY KEY (node, GPU_Type)
    );"
    echo "Database has been initialized."

elif [ "$1" = "gpu-update" ]; then
    updateGpuStatus

elif [ "$1" = "gpu-view" ]; then
    echo "Current GPU status from database '$db_name':"
    sqlite3 -header -column "$db_name" "SELECT * FROM gpu ORDER BY node, GPU_Type;"

elif [ "$1" = "submit" ];then
    # ... (rest of your command parsing logic is unchanged) ...
    if [ $# -eq 3 ]; then
        submitFirstJob "$2" "$3"
    elif [ $# -eq 2 ]; then
        submitFirstJob "$2"
    else
        echo "Incorrect number of arguments were passed"
    fi
# ...
# The rest of your elif statements...
# ...
elif [ "$1" = "start" ];then
    if [ $# -eq 4 ]; then
        submitFirstJob "$2" "$3" "$4"
    elif [ $# -eq 3 ]; then
        submitFirstJob "$2" "$3" "$3"
    elif [ $# -eq 2 ]; then
        submitFirstJob "$2" "start.sh" "start.sh"
    else
        echo "Incorrect number of arguments were passed"
    fi

elif [ "$1" = "restart" ];then
    if [ $# -eq 2 ]; then
        restartJob "$2"
    else
        echo "Incorrect number of arguments were passed"
    fi

elif [ "$1" = "insert" ];then
    if [ $# -eq 6 ]; then
        sqlite3 "$db_name" "INSERT INTO jobs (jobid,status,location,type,script) VALUES ('$2','$3','$4','$5','$6');"
    else
        echo "Incorrect number of arguments were passed"
    fi
elif [ "$1" = "view" ];then
    result=$(sqlite3 -header -column "$db_name" "SELECT * FROM jobs;")
    echo "$result"
elif [ "$1" = "delete" ];then
    if [ $# -eq 2 ]; then
        if [ "$2" = "all" ];then
            deleteJob
        elif [[ $2 =~ ^-?[0-9]+(\.[0-9]+)?$ ]]; then
            deleteJob "$2"
        else
            sqlite3 "$db_name" "DELETE FROM jobs WHERE $2;"
        fi
    elif [ $# -eq 3 ]; then
        if [ "$2" = "id" ];then
            deleteJobById "$3"
        else
            echo "Invalid command"
        fi
    else
        echo "Incorrect number of arguments were passed"
    fi

elif [ "$1" = "update" ];then
    if [ $# -eq 3 ]; then
        updateStatus "$2" "$3"
    else
        echo "Incorrect number of arguments were passed"
    fi
elif [ "$1" = "autoupdate" ];then
    statusUpdater
elif [ "$1" = "plot" ];then
    if [ $# -eq 2 ]; then
        plotter "$2"
    elif [ $# -eq 1 ]; then
        plotter "script.sh plot"
    else
        echo "Incorrect number of arguments were passed"
    fi
else
    echo "Invalid command"
    echo "Usage: $0 [init|submit|start|restart|delete|view|update|plot|gpu-update|gpu-view|autoupdate]"
fi
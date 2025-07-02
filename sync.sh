#!/bin/bash

# Get the absolute path to the database, located in the same directory as the script.
db_name="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/test.db"
GPUpreference="h100,l40,a100,a30"

# =========================================================================
# JOB MANAGEMENT FUNCTIONS (Your Original Functions)
# =========================================================================

function updateStatus(){ #jobid stautus
    sqlite3 "$db_name" "UPDATE jobs SET STATUS='$2' WHERE JOBID='$1';"
}

# NEW: Finds the best available node based on GPUpreference.
# Returns "nodename|GPU_Type" on success, empty on failure.
function find_best_available_gpu_node() {
    local preferences="$1"

    
    # First, ensure the GPU data is recent. It's good practice.
    # echo "INFO: Running a quick GPU status check before finding a node..."
    # updateGpuStatus > /dev/null # Run quietly

    IFS=',' read -ra pref_array <<< "$preferences"
    for gpu_type in "${pref_array[@]}"; do
        # Query for a node with the desired GPU type available.
        # Order by GPU_available DESC to pick the node with the most free slots,
        # which can be a good strategy for cluster load balancing.
        local result
        result=$(sqlite3 "$db_name" " SELECT node, GPU_Type FROM gpu 
            WHERE GPU_Type = '$gpu_type' 
              AND GPU_available > 0 
              AND is_private = 1 
            ORDER BY GPU_available DESC 
            LIMIT 1;")
        if [[ -n "$result" ]]; then
            echo "$result" # Success: Output "nodename|gputype"
            return 0
        fi
    done
    echo "ERROR: No preferred GPUs ($preferences) are currently available." >&2
    return 1 # Failure
}

# NEW: Preemptively updates the DB to "reserve" a GPU slot.
# function _gpu_soft_allocate() {
#     local node_name="$1"
#     local gpu_type="$2"
#     echo "INFO: Soft-allocating 1x $gpu_type on node $node_name in the database."
#     sqlite3 "$db_name" "UPDATE gpu SET GPU_available = GPU_available - 1, GPU_used = GPU_used + 1 WHERE node = '$node_name' AND GPU_Type = '$gpu_type' AND GPU_available > 0;"
# }


function jobSubmitter(){
    # count=$(sqlite3 "$db_name" "SELECT COUNT(*) FROM jobs WHERE LOCATION='$1';")
    # if [ "$count" -gt 0 ]; then
    #     echo "Job already submitted"
    #     return
    # fi
    result=$(sqlite3 "$db_name" "SELECT * FROM jobs WHERE STATUS=1;");
    if [[ -n "$result" ]]; then
        while IFS='|' read -r id jobid status location type script ; do
            cd "$location"
            if [ "$type" = "slurm" ];then
                gpuSetupContinue "$script"
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

function gpuSetupContinue(){ # filename
    if [ ! -f "$1" ]; then
        echo "File $1 not found"
        return
    fi
    # Check if the file contains a line exactly matching "#SCRATCH -G" and that it's the last line.
    line_info=$(awk '
  /^#SBATCH -w[[:space:]]+[a-zA-Z0-9_-]+$/ && prev ~ /^#SBATCH -G 1$/ {
    print NR ":" $0
  }
  {
    prev = $0
  }
' "$1")
    if [[ -n "$line_info" ]]; then
        echo "The file $1 contains a line exactly matching '#SBATCH -G' at line number: ${line_info%%:*}"
        echo "INFO: Job requires a GPU. Searching for the best available based on preference: $GPUpreference"
        local best_gpu_info; best_gpu_info=$(find_best_available_gpu_node "$GPUpreference")

        if [ -z "$best_gpu_info" ]; then
            # The find function already printed an error, so we just exit.
            echo "ERROR: No suitable GPU found. Please check the GPU availability or preferences."
            return 1
        fi

        local node_to_use; node_to_use=$(echo "$best_gpu_info" | cut -d'|' -f1)
        local gpu_type_found; gpu_type_found=$(echo "$best_gpu_info" | cut -d'|' -f2)

        # Update script

        sed -i.bak -E "s|^#SBATCH -w[[:space:]]+[a-zA-Z0-9_-]+|#SBATCH -w $node_to_use|" "$1"
        sqlite3 "$db_name" "UPDATE gpu SET GPU_available = GPU_available - 1, GPU_used = GPU_used + 1 WHERE node = '$node_to_use' AND GPU_Type = '$gpu_type_found' AND GPU_available > 0;"

        echo "SUCCESS: Found best available GPU: 1x $gpu_type_found on node $node_to_use."
        
    fi
}

function gpuSetup(){ # filename
    if [ ! -f "$1" ]; then
        echo "File $1 not found"
        return
    fi
    # Check if the file contains a line exactly matching "#SCRATCH -G" and that it's the last line.
    line_info=$(grep -n -E "^#SBATCH -G[[:space:]]*$" "$1")
    if [[ -n "$line_info" ]]; then
        echo "The file $1 contains a line exactly matching '#SBATCH -G' at line number: ${line_info%%:*}"
        echo "INFO: Job requires a GPU. Searching for the best available based on preference: $GPUpreference"
        local best_gpu_info; best_gpu_info=$(find_best_available_gpu_node "$GPUpreference")

        if [ -z "$best_gpu_info" ]; then
            # The find function already printed an error, so we just exit.
            echo "ERROR: No suitable GPU found. Please check the GPU availability or preferences."
            return 1
        fi

        local node_to_use; node_to_use=$(echo "$best_gpu_info" | cut -d'|' -f1)
        local gpu_type_found; gpu_type_found=$(echo "$best_gpu_info" | cut -d'|' -f2)

        # Update script
        sed -i.bak -E "s|^#SBATCH[[:space:]]+-G[[:space:]]*$|#SBATCH -G 1\n#SBATCH -w $node_to_use|" "$1"
        sqlite3 "$db_name" "UPDATE gpu SET GPU_available = GPU_available - 1, GPU_used = GPU_used + 1 WHERE node = '$node_to_use' AND GPU_Type = '$gpu_type_found' AND GPU_available > 0;"

        echo "SUCCESS: Found best available GPU: 1x $gpu_type_found on node $node_to_use."
        
    fi
}

function submitFirstJob(){ # *location, script, resume script
    cd "$1"
    local script_name="${2:-start.sh}"
    if [ ! -f "$script_name" ]; then
        echo "Script $script_name not found in the directory $1"
        return
    fi
    gpuSetup "$script_name"
    newjobid=$(sbatch $script_name)
    newjobid=${newjobid##* }
    echo "Jobid: $newjobid"
    if [ $# -lt 3 ]; then
        echo "Job"
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
    local is_private_flag="$7" # New argument

    local sql_command="
    INSERT INTO gpu (node, GPU_Type, GPU_available, GPU_used, GPU_configured, is_private, last_updated)
    VALUES ('$node_name', '$gpu_type', $available_count, $used_count, $conf_count, $is_private_flag, CURRENT_TIMESTAMP)
    ON CONFLICT(node, GPU_Type) DO UPDATE SET
        GPU_available = excluded.GPU_available,
        GPU_used = excluded.GPU_used,
        GPU_configured = excluded.GPU_configured,
        is_private = excluded.is_private,
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
    # This is the feature/tag we are looking for to identify a private node.
    local TARGET_FEATURE="private"

    echo "Probing Slurm, tagging nodes with feature '$TARGET_FEATURE', and updating database: $db_name"
    echo "=========================================================================="
    printf "%-20s %-25s %-10s %-10s %-10s\n" "Node" "GPU_Type" "Configured" "Used" "Available"
    echo "-------------------- ------------------------- ---------- ---------- ----------"

    # Get all operational nodes. This part remains the same.
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

        # --- START: Added logic to handle non-responding, down, or draining nodes ---
        # Extracts the State field from scontrol output, e.g., "State=IDLE+DRAIN"
        local node_state; node_state=$(echo "$scontrol_node_output" | grep -o 'State=[^ ]*')

        # Check if the state contains problematic keywords.
        if [[ "$node_state" == *DOWN* || "$node_state" == *DRAIN* || "$node_state" == *NOT_RESPONDING* ]]; then
            echo "Node '$node_name' is in a non-operational state ($node_state). Removing from DB and skipping."
            # Delete the node from the database if it exists.
            sqlite3 "$db_name" "DELETE FROM gpu WHERE node = '$node_name';"
            # Skip further processing for this node.
            continue
        fi
        # --- END: Added logic ---

        # A faster way to skip non-gpu nodes before more complex parsing
        if [[ "$scontrol_node_output" != *Gres=gpu* ]]; then continue; fi

        # --- NEW LOGIC: Parse AvailableFeatures ---
        local features_str
        features_str=$(echo "$scontrol_node_output" | awk '{for(i=1;i<=NF;i++) if($i ~ /^AvailableFeatures=/) {sub(/^AvailableFeatures=/, "", $i); print $i; exit}}')

        local is_private=0
        if [[ "$features_str" == *"$TARGET_FEATURE"* ]]; then
            is_private=1
            # echo "Node '$node_name' is marked as private (feature: $TARGET_FEATURE)."
        fi
        # --- END NEW LOGIC ---

        local cfg_tres_str; cfg_tres_str=$(echo "$scontrol_node_output" | awk '{for(i=1;i<=NF;i++) if($i ~ /^CfgTRES=/) {sub(/^CfgTRES=/, "", $i); print $i; exit}}')
        local alloc_tres_str; alloc_tres_str=$(echo "$scontrol_node_output" | awk '{for(i=1;i<=NF;i++) if($i ~ /^AllocTRES=/) {sub(/^AllocTRES=/, "", $i); print $i; exit}}')

        declare -A configured_gpus_map; declare -A used_gpus_map
        _parse_tres_string "$cfg_tres_str" configured_gpus_map
        _parse_tres_string "$alloc_tres_str" used_gpus_map

        if [[ -n "${configured_gpus_map["gpu"]}" ]]; then
            local has_specific=0; for key in "${!configured_gpus_map[@]}"; do if [[ "$key" != "gpu" ]]; then has_specific=1; break; fi; done
            if [[ "$has_specific" -eq 1 ]]; then unset configured_gpus_map["gpu"]; fi
        fi

        for gpu_type in "${!configured_gpus_map[@]}"; do
            local conf_count=${configured_gpus_map[$gpu_type]}
            local used_count=${used_gpus_map[$gpu_type]:-0}
            local available_count=$(( conf_count - used_count ))
            if [ "$available_count" -lt 0 ]; then available_count=0; fi

            _gpu_upsert_row "$db_name" "$node_name" "$gpu_type" "$available_count" "$used_count" "$conf_count" "$is_private"

            if [ "$available_count" -gt 0 ] && [ "$is_private" -eq 1 ]; then
                printf "%-20s %-25s %-10s %-10s %-10s\n" \
                    "$node_name" "$gpu_type" "$conf_count" "$used_count" "$available_count"
                any_node_printed=1
                total_available_gpus_by_type["$gpu_type"]=$(( ${total_available_gpus_by_type["$gpu_type"]:-0} + available_count ))
            fi
        done
    done

    echo "=========================================================================="
    echo "Database '$db_name' has been updated. Use 'gpu-view' to see all nodes."
    echo
    if [ "$any_node_printed" -eq 1 ]; then
        echo "Overall summary of currently available GPUs on nodes with feature '$TARGET_FEATURE':"
        local sorted_gpu_types; sorted_gpu_types=($(for t in "${!total_available_gpus_by_type[@]}"; do echo "$t"; done | sort))
        for gpu_type in "${sorted_gpu_types[@]}"; do
            if [ "${total_available_gpus_by_type[$gpu_type]}" -gt 0 ]; then
                printf "  %-25s : %s\n" "$gpu_type" "${total_available_gpus_by_type[$gpu_type]}"
            fi
        done
        echo "--------------------------------------------------------------------------"
    else
        echo "No nodes with currently available GPUs found with the feature '$TARGET_FEATURE'."
    fi
}

# Submits jobs from all subdirectories within a given parent directory.
# It looks for a 'start.sh' in each immediate subdirectory.
function batchSubmitter() {
    if [ -z "$1" ]; then
        echo "No directory specified for batch submission. Please provide a parent directory."
        return 1
    fi
    cd "$1"
    for d in */; do
        if [ -d "$d" ]; then
            echo "Processing folder: $d"
            cd "$d"
            if [ -f "start.sh" ]; then
                echo "Found start.sh in $d, submitting job..."
                submitFirstJob "$PWD" "start.sh" "start.sh"
            else
                echo "No start.sh found in $d, skipping..."
            fi
            cd ..
        fi
    done
    echo "Batch submission completed for all subdirectories."
    cd ..
}


# =========================================================================
# MAIN SCRIPT LOGIC AND COMMAND PARSER
# =========================================================================

if [ $# -eq 0 ];then
    echo "Main function called updating job status and gpu table, then submitting jobs"
    updateGpuStatus >> /dev/null &
    statusUpdater
    wait
    jobSubmitter
    
elif [ "$1" = "init" ];then
    echo "Initializing database '$db_name'..."
    sqlite3 "$db_name" "CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, jobid TEXT, status INTEGER, location TEXT, type TEXT, script TEXT);"
    sqlite3 "$db_name" "CREATE TABLE IF NOT EXISTS gpu (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
        node TEXT NOT NULL,
        GPU_Type TEXT NOT NULL,
        GPU_available INTEGER NOT NULL,
        GPU_used INTEGER NOT NULL,
        GPU_configured INTEGER NOT NULL,
        is_private INTEGER NOT NULL DEFAULT 0,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        UNIQUE(node, GPU_Type)
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
elif [ "$1" = "batch" ]; then
    batchSubmitter "$2"

elif [ "$1" = "test" ]; then
    gpuSetupContinue "$2"
#     gpuSetup $2
    # find_best_available_gpu_node "l40,h100"
else
    echo "Invalid command or no command provided."
    echo "Usage: $0 COMMAND [arguments]"
    echo ""
    echo "Commands:"
    echo "  init         : Initialize the database."
    echo "  submit       : Submit a job. Usage: $0 submit <location> [script]"
    echo "  start        : Start the first job. Usage: $0 start <location> [startScript] [resumeScript]"
    echo "  restart      : Restart a job. Usage: $0 restart <jobid>"
    echo "  delete       : Delete job(s). Usage: $0 delete all | <jobid> | id <db-id>"
    echo "  view         : View job status from the database."
    echo "  update       : Update a job status. Usage: $0 update <jobid> <status>"
    echo "  plot         : Run the plotting script. Usage: $0 plot [plotScript]"
    echo "  gpu-update   : Update GPU status."
    echo "  gpu-view     : View GPU status from the database."
    echo "  autoupdate   : Automatically update job statuses."
fi
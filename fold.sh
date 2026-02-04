#!/bin/bash

# Get the absolute path to the database, located in the same directory as the script.
db_name="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/fold.db"
# fold.sh - Manages batch job submissions based on folder structure and YAML configuration
# Usage: fold.sh start $PWD

# =========================================================================
# DATABASE INITIALIZATION
# =========================================================================

function initDatabase() {
    echo "Initializing database '$db_name'..."
    
    # Create folders table to track all created directories
    sqlite3 "$db_name" "CREATE TABLE IF NOT EXISTS folders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        path TEXT NOT NULL UNIQUE,
        parent_path TEXT NOT NULL,
        simulation_name TEXT NOT NULL,
        replica_number INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
    );"
    
    # Create jobs table to track job submissions
    sqlite3 "$db_name" "CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        jobid TEXT,
        status INTEGER,
        folder_id INTEGER,
        location TEXT,
        type TEXT,
        script TEXT,
        stage TEXT,
        progress REAL DEFAULT 0.0,
        max_steps INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        FOREIGN KEY(folder_id) REFERENCES folders(id)
    );"
    
    # Create setup_runs table to track each time setup is run
    sqlite3 "$db_name" "CREATE TABLE IF NOT EXISTS setup_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        base_path TEXT NOT NULL,
        output_folder TEXT NOT NULL,
        input_folder TEXT NOT NULL,
        replicas INTEGER NOT NULL,
        simulations_processed INTEGER NOT NULL,
        folders_created INTEGER NOT NULL,
        run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
    );"
    
    echo "Database has been initialized with tables: folders, jobs, setup_runs"
    echo "Database location: $db_name"
}

# =========================================================================
# YAML PARSING AND CONFIGURATION
# =========================================================================

function parseYaml() {
    local yaml_file="$1"
    local key="$2"
    
    # Simple YAML parser for our specific structure
    if [ ! -f "$yaml_file" ]; then
        echo "ERROR: YAML file $yaml_file not found" >&2
        return 1
    fi
    
    case $key in
        "Output.Folder")
            grep -A 1 "^Output:" "$yaml_file" | grep "Folder:" | sed 's/.*Folder: *//;s/ *$//' | tr -d '"'
            ;;
        "Input.Folder")
            grep -A 5 "^Input:" "$yaml_file" | grep "Folder:" | sed 's/.*Folder: *//;s/ *$//' | tr -d '"'
            ;;
        "Input.Replicas")
            grep -A 5 "^Input:" "$yaml_file" | grep "Replicas:" | sed 's/.*Replicas: *//;s/ *$//'
            ;;
        "Input.Files")
            # Returns comma-separated list of file patterns
            grep -A 10 "^Input:" "$yaml_file" | sed -n '/Files:/,/OutputName:/p' | grep '^ *-' | grep -v ':' | sed 's/^ *- *//;s/ *$//;s/"//g' | tr '\n' ',' | sed 's/,$//'
            ;;
        "Input.OutputName")
            # Returns comma-separated list of output names
            grep -A 15 "^Input:" "$yaml_file" | sed -n '/OutputName:/,/CopyAllFiles:/p' | grep '^ *-' | grep -v ':' | sed 's/^ *- *//;s/ *$//;s/"//g' | tr '\n' ',' | sed 's/,$//'
            ;;
        "Input.CopyAllFiles")
            grep -A 20 "^Input:" "$yaml_file" | grep "CopyAllFiles:" | head -1 | sed 's/.*CopyAllFiles: *//;s/ *$//'
            ;;
        "Input.Executable")
            grep -A 25 "^Input:" "$yaml_file" | grep "Executable:" | head -1 | sed 's/.*Executable: *//;s/ *$//' | tr -d '"'
            ;;
        "Input.ResumeExecutable")
            grep -A 30 "^Input:" "$yaml_file" | grep "ResumeExecutable:" | head -1 | sed 's/.*ResumeExecutable: *//;s/ *$//' | tr -d '"'
            ;;
    esac
}

function parseProcedureStage() {
    local yaml_file="$1"
    local stage="$2"
    local property="$3"
    
    # Extract the stage block and get the property
    local result=$(awk -v stage="$stage" -v prop="$property" '
        BEGIN { in_procedure=0; in_stage=0; }
        /^Procedure:/ { in_procedure=1; next; }
        in_procedure && $0 ~ "^  - " stage ":" { in_stage=1; next; }
        in_stage && /^  - [A-Z]/ { in_stage=0; }
        in_stage && $0 ~ "      - " prop " *:" {
            gsub(/^.*: */, "", $0);
            gsub(/^ *| *$/, "", $0);
            print $0;
            exit;
        }
    ' "$yaml_file")
    
    echo "$result"
}

function parseProcedureStageList() {
    local yaml_file="$1"
    local stage="$2"
    local property="$3"
    
    awk -v stage="$stage" -v prop="$property" '
        BEGIN { in_procedure=0; in_stage=0; in_prop=0; }
        /^Procedure:/ { in_procedure=1; next; }
        in_procedure && $0 ~ "^  - " stage ":" { in_stage=1; next; }
        in_stage && /^  - [A-Z]/ { in_stage=0; in_prop=0; }
        in_stage && $0 ~ "      - " prop ":" { in_prop=1; next; }
        in_prop && /^        - / {
            sub(/^        - /, "", $0);
            # Remove surrounding quotes if present (simple check)
            if ($0 ~ /^".*"$/ || $0 ~ /^'"'"'.*'"'"'$/) {
               gsub(/^["'"'"']|["'"'"']$/, "", $0);
            }
            print $0;
        }
        in_prop && !/^        - / && !/^ *$/ { in_prop=0; }
    ' "$yaml_file"
}

function getProcedureStages() {
    local yaml_file="$1"
    
    # Get all stage names from Procedure section, stop at next top-level section
    awk '/^Procedure:/ {flag=1; next} /^[A-Z]/ && flag {exit} flag && /^  - [A-Z]/ {match($0, /^  - ([^:]+):/, arr); print arr[1]}' "$yaml_file" | tr '\n' ',' | sed 's/,$//'
}

# =========================================================================
# JOB MANAGEMENT FUNCTIONS
# =========================================================================

function updateStatus() {
    local jobid="$1"
    local status="$2"
    sqlite3 "$db_name" "UPDATE jobs SET status='$status' WHERE jobid='$jobid';"
}

function checkJob() {
    local jobid="$1"
    local status=$(squeue -h -j "$jobid" -o "%T" 2>/dev/null)
    
    if [[ -z "$status" ]]; then
        echo "The job $jobid has finished"
        return 1
    else
        case $status in
        "RUNNING")   echo "The job $jobid is running"; return 0 ;;
        "PENDING")   echo "The job $jobid is pending in queue"; return 2 ;;
        *)           echo "The job $jobid is in: $status"; return 3 ;;
        esac
    fi
}

function checkSimulationCompletion() {
    local location="$1"
    
    # Check for log files with success message
    if ls "$location"/log.* 1> /dev/null 2>&1; then
        if grep -q "INFO: END OF THE SIMULATION, everything went OK!" "$location"/log.* 2>/dev/null; then
            return 0  # Success
        fi
    fi
    
    return 1  # Not successful
}

function isJobRunning() {
    local location="$1"
    
    # Check if any job at this location has status 2 (running/pending)
    local result=$(sqlite3 "$db_name" "SELECT count(*) FROM jobs WHERE location='$location' AND status=2;")
    
    if [ "$result" -gt 0 ]; then
        return 0  # Running
    else
        return 1  # Not running
    fi
}

function getNextStage() {
    local yaml_file="$1"
    local current_stage="$2"
    
    local stages; stages=$(getProcedureStages "$yaml_file")
    IFS=',' read -ra stages_array <<< "$stages"
    
    local found=false
    for stage in "${stages_array[@]}"; do
        if [ "$found" = true ]; then
            echo "$stage"
            return 0
        fi
        if [ "$stage" = "$current_stage" ]; then
            found=true
        fi
    done
    
    return 1  # No next stage
}

function submitJob() {
    local location="$1"
    local script="$2"
    local stage="$3"
    
    cd "$location"
    
    if [ ! -f "$script" ]; then
        echo "ERROR: Script $script not found in $location"
        return 1
    fi
    
    # Check for existing running job for this stage
    local old_jobid=$(sqlite3 "$db_name" "SELECT jobid FROM jobs WHERE location='$location' AND stage='$stage' AND status=2;")
    
    if [ -n "$old_jobid" ]; then
        echo "Found running job $old_jobid for this stage. Cancelling..."
        scancel "$old_jobid"
        # Optional: wait a bit to ensure cancellation propagates
        sleep 2
    fi
    
    local newjobid=$(sbatch "$script" 2>&1)
    if [[ $newjobid =~ Submitted\ batch\ job\ ([0-9]+) ]]; then
        newjobid="${BASH_REMATCH[1]}"
        echo "Submitted job $newjobid for $stage in $location"
        
        # Update database with job ID and status
        sqlite3 "$db_name" "UPDATE jobs SET jobid='$newjobid', status=2 WHERE location='$location' AND stage='$stage';"
        return 0
    else
        echo "ERROR: Failed to submit job: $newjobid"
        return 1
    fi
}

function submitNextStage() {
    local base_path="$1"
    local location="$2"
    local current_stage="$3"
    local yaml_file="$base_path/main.yaml"
    
    # Get next stage
    local next_stage; next_stage=$(getNextStage "$yaml_file" "$current_stage")
    
    if [ -z "$next_stage" ]; then
        echo "  No more stages after $current_stage. Workflow complete for $location"
        return 1
    fi
    
    echo "  Moving from $current_stage to $next_stage for $location"
    
    # Submit the next stage
    local script_name="start_${next_stage}.sh"
    submitJob "$location" "$script_name" "$next_stage"
}

function statusUpdater() {
    local base_path="$1"
    
    echo "Updating job statuses..."
    
    # Save IFS
    local OLD_IFS="$IFS"
    
    # First, check jobs that have job IDs
    IFS='|'
    local result=$(sqlite3 "$db_name" "SELECT id, jobid, location, stage, status, max_steps FROM jobs WHERE jobid IS NOT NULL AND jobid != '' AND jobid != 'NULL';")
    
    if [ -n "$result" ]; then
        while read -r id jobid location stage current_status max_steps; do
            # Restore IFS for function calls
            IFS="$OLD_IFS"
            
            # Skip if any critical field is empty
            if [ -z "$jobid" ] || [ -z "$location" ] || [ -z "$stage" ]; then
                echo "Warning: Skipping job with missing data (id=$id, jobid=$jobid, location=$location, stage=$stage)"
                IFS='|'
                continue
            fi
            
            checkJob "$jobid"
            local check_result=$?
            
            case $check_result in
            0) 
                # Job is running
                updateStatus "$jobid" 2
                
                # Check actual progress based on files
                if [ -n "$max_steps" ] && [ "$max_steps" -gt 0 ]; then
                     local current_progress=$(calculateProgress "$location" "$max_steps")
                     local progress_int=${current_progress%.*}
                     
                     if [ "$progress_int" -ge 100 ]; then
                         echo "Job $jobid ($stage) has reached target steps ($current_progress%). Cancelling..."
                         scancel "$jobid"
                         
                         # Save variables before calling functions that might corrupt them
                         local saved_stage="$stage"
                         local saved_location="$location"
                         local saved_jobid="$jobid"
                         
                         # Archive energy files
                         archiveEnergyFile "$saved_location" "$saved_stage"
                         
                         # Mark as finished
                         updateStatus "$saved_jobid" 1
                         
                         # Submit next stage
                         submitNextStage "$base_path" "$saved_location" "$saved_stage"
                     fi
                fi
                ;;
            1) 
                # Job has finished
                echo "Job $jobid ($stage) has finished"
                
                # Save variables before calling functions that might corrupt them
                local saved_stage="$stage"
                local saved_location="$location"
                local saved_jobid="$jobid"
                local saved_id="$id"
                
                # Check if simulation completed successfully
                if checkSimulationCompletion "$saved_location"; then
                    echo "  Simulation completed successfully!"
                    updateStatus "$saved_jobid" 1
                    
                    # Archive energy files
                    archiveEnergyFile "$saved_location" "$saved_stage"
                    
                    # Submit next stage
                    submitNextStage "$base_path" "$saved_location" "$saved_stage"
                else
                    echo "  Checking progress..."
                    # Update progress and check if 100%
                    updateJobProgress "$base_path"
                    
                    # Get the progress for this job
                    local progress=$(sqlite3 "$db_name" "SELECT progress FROM jobs WHERE id=$saved_id;")
                    local progress_int=${progress%.*}
                    
                    if [ "$progress_int" -ge 100 ]; then
                        echo "  Progress is 100% or more, moving to next stage"
                        updateStatus "$saved_jobid" 1
                        
                        # Archive energy files
                        archiveEnergyFile "$saved_location" "$saved_stage"
                        
                        submitNextStage "$base_path" "$saved_location" "$saved_stage"
                    else
                        echo "  Progress is $progress%, job incomplete - will resubmit"
                        # Resubmit the job
                        if [ -n "$saved_stage" ]; then
                            local script_name="start_${saved_stage}.sh"
                            submitJob "$saved_location" "$script_name" "$saved_stage"
                        else
                            echo "  ERROR: Stage is empty, cannot resubmit"
                        fi
                    fi
                fi
                ;;
            2) 
                # Job is pending
                updateStatus "$jobid" 2
                ;;
            3) 
                # Job in other state
                echo "Job $jobid ($stage) is in unusual state, checking if it needs resubmission..."
                if [ "$current_status" = "2" ]; then
                    # Was supposed to be running but isn't, resubmit
                    echo "  Resubmitting job..."
                    local script_name="start_${stage}.sh"
                    submitJob "$location" "$script_name" "$stage"
                else
                    updateStatus "$jobid" 3
                fi
                ;;
            esac
            
            # Reset IFS for next iteration
            IFS='|'
        done <<< "$result"
    fi
    
    # Restore IFS
    IFS="$OLD_IFS"
    
    # Now check for jobs that should be running but have no job ID (possibly never submitted or lost)
    IFS='|'
    local pending_result=$(sqlite3 "$db_name" "SELECT id, location, stage FROM jobs WHERE status=2 AND (jobid IS NULL OR jobid = '' OR jobid = 'NULL');")
    
    if [ -n "$pending_result" ]; then
        while read -r id location stage; do
            # Restore IFS for function calls
            IFS="$OLD_IFS"
            
            # Skip if any critical field is empty
            if [ -z "$location" ] || [ -z "$stage" ]; then
                echo "Warning: Skipping job with missing location or stage (id=$id)"
                IFS='|'
                continue
            fi
            
            echo "Job at $location ($stage) marked as running but has no job ID - submitting..."
            local script_name="start_${stage}.sh"
            submitJob "$location" "$script_name" "$stage"
            
            # Reset IFS for next iteration
            IFS='|'
        done <<< "$pending_result"
    fi
    
    # Restore IFS
    IFS="$OLD_IFS"
    
    echo "Status update complete."
}

function startWorkflow() {
    local base_path="$1"
    local yaml_file="$base_path/main.yaml"
    
    echo "Starting workflows for all simulations..."
    
    # Get first stage from procedure
    local stages; stages=$(getProcedureStages "$yaml_file")
    IFS=',' read -ra stages_array <<< "$stages"
    local first_stage="${stages_array[0]}"
    
    echo "First stage: $first_stage"
    
    # Get all jobs for the first stage that haven't been submitted yet
    local result=$(sqlite3 "$db_name" "SELECT location, stage FROM jobs WHERE stage='$first_stage' AND (jobid IS NULL OR jobid = '');")
    
    if [ -n "$result" ]; then
        while IFS='|' read -r location stage; do
            local script_name="start_${stage}.sh"
            submitJob "$location" "$script_name" "$stage"
        done <<< "$result"
    else
        echo "No pending jobs found for stage: $first_stage"
    fi
    
    echo "Workflow start complete."
}

# =========================================================================
# PROGRESS TRACKING FUNCTIONS
# =========================================================================

function getProgressFromEnergyFile() {
    local energy_file="$1"
    
    if [ ! -f "$energy_file" ]; then
        echo "0"
        return
    fi
    
    # Count the number of lines in the file
    local line_count=$(wc -l < "$energy_file")
    
    echo "$line_count"
}

function calculateProgress() {
    local location="$1"
    local max_steps="$2"
    
    local total_steps=0
    local replica_count=0
    
    for replica_dir in "$location"/*/; do
        if [ -d "$replica_dir" ]; then
            local energy_file="${replica_dir}energy.dat"
            local current_step=$(getProgressFromEnergyFile "$energy_file")
            total_steps=$((total_steps + current_step))
            replica_count=$((replica_count + 1))
        fi
    done
    
    if [ "$replica_count" -gt 0 ] && [ "$max_steps" -gt 0 ]; then
        local avg_step=$((total_steps / replica_count))
        local progress=$(awk -v step="$avg_step" -v max="$max_steps" 'BEGIN {printf "%.2f", (step / max) * 100}')
        echo "$progress"
    else
        echo "0"
    fi
}

function archiveEnergyFile() {
    local location="$1"
    local stage="$2"
    
    # Iterate over replicas and rename energy.dat
    for replica_dir in "$location"/*/; do
        if [ -d "$replica_dir" ] && [ -f "${replica_dir}energy.dat" ]; then
            mv "${replica_dir}energy.dat" "${replica_dir}energy${stage}.dat"
            # Create a new empty energy.dat to prevent errors if something checks right away
            touch "${replica_dir}energy.dat"
        fi
    done
}

function updateJobProgress() {
    local base_path="$1"
    local yaml_file="$base_path/main.yaml"
    
    echo "Updating job progress..."
    
    # Save IFS
    local OLD_IFS="$IFS"
    IFS='|'
    
    # Get all running jobs from database (status=2)
    local result=$(sqlite3 "$db_name" "SELECT id, location, stage, max_steps FROM jobs WHERE stage IS NOT NULL AND status=2;")
    
    while read -r job_id location stage max_steps; do
        # Restore IFS for function calls
        IFS="$OLD_IFS"
        
        if [ -z "$location" ] || [ -z "$stage" ] || [ -z "$max_steps" ]; then
            IFS='|'
            continue
        fi
        
        # Calculate progress using helper
        local progress=$(calculateProgress "$location" "$max_steps")
        
        # Update the progress in database
        sqlite3 "$db_name" "UPDATE jobs SET progress = $progress WHERE id = $job_id;"
        echo "  Job $job_id ($stage): $progress% (based on $max_steps steps)"
        
        # Reset IFS for next iteration
        IFS='|'
    done <<< "$result"
    
    # Restore IFS
    IFS="$OLD_IFS"
    
    echo "Progress update complete."
}

# =========================================================================
# SLURM SCRIPT GENERATION FUNCTIONS
# =========================================================================

function generateSlurmScript() {
    local script_path="$1"
    local job_name="$2"
    local num_cpus="$3"
    local num_gpus="$4"
    local memory="$5"
    local executable="$6"
    local replicas="$7"
    local input_file="$8"
    local sim_folder="$9"
    local run_before_cmds="${10}"
    
    cat > "$script_path" << 'EOFTEMPLATE'
#!/bin/sh
#SBATCH -q private
#SBATCH -p general
#SBATCH -t 7-00:00
#SBATCH -c NUMCPUS
GPULINE#SBATCH --mem=MEMORY
#SBATCH -o empty.out
#SBATCH -e empty.err
#SBATCH -J JOBNAME

module load cuda-12.1.1-gcc-12.1.0 gcc-12.1.0-gcc-11.2.0 cmake eigen-3.4.0-gcc-11.2.0

MPSSECTION

echo "Starting jobs"

REPLICACOMMANDS

wait
echo "All jobs completed"
EOFTEMPLATE

    # Replace placeholders
    sed -i "s/NUMCPUS/$num_cpus/g" "$script_path"
    sed -i "s/MEMORY/$memory/g" "$script_path"
    sed -i "s/JOBNAME/$job_name/g" "$script_path"
    
    # Handle GPU line
    if [ "$num_gpus" = "0" ] || [ -z "$num_gpus" ]; then
        sed -i '/GPULINE/d' "$script_path"
    else
        sed -i "s/GPULINE/#SBATCH -G $num_gpus\n/" "$script_path"
    fi
    
    # Handle MPS section - only include if GPU is used
    if [ "$num_gpus" = "0" ] || [ -z "$num_gpus" ]; then
        sed -i '/MPSSECTION/d' "$script_path"
    else
        local mps_lines="export CUDA_MPS_PIPE_DIRECTORY=\/tmp\/mps-pipe_\$SLURM_TASK_PID\nexport CUDA_MPS_LOG_DIRECTORY=\/tmp\/mps-log_\$SLURM_TASK_PID\nmkdir -p \$CUDA_MPS_PIPE_DIRECTORY\nmkdir -p \$CUDA_MPS_LOG_DIRECTORY\nnvidia-cuda-mps-control -d\n"
        sed -i "s/MPSSECTION/$mps_lines/" "$script_path"
    fi
    
    # Generate replica commands
    local commands=""
    for ((i=0; i<replicas; i++)); do
        commands+="cd $sim_folder/$i/"$'\n'
        if [ -n "$run_before_cmds" ]; then
             commands+="$run_before_cmds"$'\n'
        fi
        commands+="$executable $input_file &"$'\n'
        commands+="cd .."$'\n'
    done
    
    # Use a temporary file to safely replace the placeholder
    local temp_commands_file
    temp_commands_file=$(mktemp)
    printf "%s" "$commands" > "$temp_commands_file"
    
    local temp_script_file
    temp_script_file=$(mktemp)
    
    # Read the template and replace the placeholder with the content of the commands file
    local placeholder_found=false
    while IFS= read -r line; do
        if [[ "$line" == "REPLICACOMMANDS" ]]; then
            placeholder_found=true
            cat "$temp_commands_file"
        else
            echo "$line"
        fi
    done < "$script_path" > "$temp_script_file"
    
    # Overwrite the original script with the corrected content
    mv "$temp_script_file" "$script_path"
    
    # Clean up the temporary commands file
    rm "$temp_commands_file"
    
    chmod +x "$script_path"
}

function generateJobScripts() {
    local base_path="$1"
    local yaml_file="$base_path/main.yaml"
    local output_folder="$2"
    local replicas="$3"
    local sim_name="$4"
    local sim_folder="$5"
    
    # Get executable paths
    local executable; executable=$(parseYaml "$yaml_file" "Input.Executable")
    
    # Get procedure stages
    local stages; stages=$(getProcedureStages "$yaml_file")
    IFS=',' read -ra stages_array <<< "$stages"
    
    echo "  Generating SLURM scripts for $sim_name..."
    
    for stage in "${stages_array[@]}"; do
        # Parse stage properties
        local gpu; gpu=$(parseProcedureStage "$yaml_file" "$stage" "GPU")
        local cpus; cpus=$(parseProcedureStage "$yaml_file" "$stage" "CPUs")
        local memory; memory=$(parseProcedureStage "$yaml_file" "$stage" "Memory")
        local exec_input; exec_input=$(parseProcedureStage "$yaml_file" "$stage" "ExecutableInput")
        local jobs_per_gpu; jobs_per_gpu=$(parseProcedureStage "$yaml_file" "$stage" "JobsPerGPU")
        local max_steps; max_steps=$(parseProcedureStage "$yaml_file" "$stage" "MaxSteps")
        local run_before_cmds; run_before_cmds=$(parseProcedureStageList "$yaml_file" "$stage" "RunBefore")
        
        # Set defaults
        [ -z "$memory" ] && memory="40GB"
        [ -z "$cpus" ] && cpus=1
        [ -z "$max_steps" ] && max_steps=0
        
        # Calculate total CPUs
        local total_cpus=$((cpus * replicas))
        
        # Determine GPU count
        local num_gpus=0
        if [ "$gpu" = "true" ]; then
            if [ -n "$jobs_per_gpu" ] && [ "$jobs_per_gpu" -gt 0 ]; then
                # Calculate number of GPUs needed: ceil(replicas / jobs_per_gpu)
                num_gpus=$(( (replicas + jobs_per_gpu - 1) / jobs_per_gpu ))
            else
                num_gpus=1
            fi
        fi
        
        # Generate job name
        local job_name="${sim_name}_${stage}"
        
        # Generate script
        local script_name="start_${stage}.sh"
        local script_path="$sim_folder/$script_name"
        
        generateSlurmScript "$script_path" "$job_name" "$total_cpus" "$num_gpus" "$memory" "$executable" "$replicas" "$exec_input" "$sim_folder" "$run_before_cmds"
        
        # Insert job record into database
        sqlite3 "$db_name" "INSERT INTO jobs (location, type, script, stage, max_steps, status) 
            VALUES ('$sim_folder', 'slurm', '$script_name', '$stage', $max_steps, 0);" 2>/dev/null
        
        echo "    Generated: $script_name (CPUs: $total_cpus, GPUs: $num_gpus, Mem: $memory, MaxSteps: $max_steps)"
    done
}

# =========================================================================
# DIRECTORY SETUP FUNCTIONS
# =========================================================================

function setupDirectoryStructure() {
    local base_path="$1"
    local yaml_file="$base_path/main.yaml"
    
    if [ ! -f "$yaml_file" ]; then
        echo "ERROR: main.yaml not found at $base_path"
        return 1
    fi
    
    echo "Reading configuration from: $yaml_file"
    
    # Parse YAML configuration
    local output_folder; output_folder=$(parseYaml "$yaml_file" "Output.Folder")
    local input_folder; input_folder=$(parseYaml "$yaml_file" "Input.Folder")
    local replicas; replicas=$(parseYaml "$yaml_file" "Input.Replicas")
    local input_patterns; input_patterns=$(parseYaml "$yaml_file" "Input.Files")
    local output_names; output_names=$(parseYaml "$yaml_file" "Input.OutputName")
    local copy_all_folder; copy_all_folder=$(parseYaml "$yaml_file" "Input.CopyAllFiles")
    
    echo "Configuration:"
    echo "  Output folder: $output_folder"
    echo "  Input folder: $input_folder"
    echo "  Replicas: $replicas"
    echo "  File patterns: $input_patterns"
    echo "  Output names: $output_names"
    echo "  Copy all files from: $copy_all_folder"
    echo ""
    
    # Create output directory
    local output_path="$base_path/$output_folder"
    if [ ! -d "$output_path" ]; then
        mkdir -p "$output_path"
        echo "Created output directory: $output_path"
    else
        echo "Output directory already exists: $output_path"
    fi
    
    # Get input directory path
    local input_path="$base_path/$input_folder"
    if [ ! -d "$input_path" ]; then
        echo "ERROR: Input directory not found: $input_path"
        return 1
    fi
    
    echo "Processing simulations from: $input_path"
    echo ""
    
    # Convert comma-separated strings to arrays
    IFS=',' read -ra patterns_array <<< "$input_patterns"
    IFS=',' read -ra names_array <<< "$output_names"
    
    # Get input folder name for simulation naming
    local input_folder_name=$(basename "$input_path")
    
    # Process each folder in the input directory
    local count=0
    local total_folders_created=0
    
    for sim_folder in "$input_path"/*/; do
        if [ ! -d "$sim_folder" ]; then
            continue
        fi
        
        # Get the folder name without trailing slash
        local sim_name=$(basename "$sim_folder")
        # Combine input folder name with subfolder name for full simulation name
        local full_sim_name="${input_folder_name}_${sim_name}"
        
        echo "Processing simulation: $sim_name"
        
        # Create simulation folder in output
        local sim_output="$output_path/$sim_name"
        
        # Check if job is already running
        if isJobRunning "$sim_output"; then
            echo "Skipping setup for $sim_name: Job is already running or pending."
            continue
        fi
        
        mkdir -p "$sim_output"
        
        # Create replica folders (0, 1, 2, 3, ...)
        for ((i=0; i<replicas; i++)); do
            local replica_folder="$sim_output/$i"
            mkdir -p "$replica_folder"
            
            # Track folder in database
            sqlite3 "$db_name" "INSERT OR IGNORE INTO folders (path, parent_path, simulation_name, replica_number, last_modified) 
                VALUES ('$replica_folder', '$sim_output', '$sim_name', $i, CURRENT_TIMESTAMP);" 2>/dev/null
            
            total_folders_created=$((total_folders_created + 1))
            
            # Copy files based on patterns
            for idx in "${!patterns_array[@]}"; do
                local pattern="${patterns_array[$idx]}"
                local output_name="${names_array[$idx]}"
                
                # Find files matching the pattern in the input folder
                local found_files=( "$sim_folder"/$pattern )
                
                if [ -e "${found_files[0]}" ]; then
                    # Copy the first matching file with the new name
                    cp "${found_files[0]}" "$replica_folder/$output_name"
                    echo "  Replica $i: Copied ${found_files[0]##*/} -> $output_name"
                else
                    echo "  WARNING: No files matching pattern '$pattern' found in $sim_name"
                fi
            done
            
            # Copy all files from CopyAllFiles folder if specified
            if [ -n "$copy_all_folder" ] && [ -d "$base_path/$copy_all_folder" ]; then
                cp "$base_path/$copy_all_folder"/* "$replica_folder/" 2>/dev/null
                echo "  Replica $i: Copied all files from $copy_all_folder"
            fi
        done
        
        count=$((count + 1))
        echo "  Created $replicas replicas for $sim_name"
        
        # Generate SLURM scripts for this simulation
        generateJobScripts "$base_path" "$output_folder" "$replicas" "$full_sim_name" "$sim_output"
        echo ""
    done
    
    # Record this setup run in the database
    sqlite3 "$db_name" "INSERT INTO setup_runs (base_path, output_folder, input_folder, replicas, simulations_processed, folders_created) 
        VALUES ('$base_path', '$output_folder', '$input_folder', $replicas, $count, $total_folders_created);" 2>/dev/null
    
    echo "=========================================================================="
    echo "Setup complete!"
    echo "Total simulations processed: $count"
    echo "Total replica folders created: $total_folders_created"
    echo "Database updated with folder tracking information"
    echo "=========================================================================="
}

# =========================================================================
# MAIN SCRIPT LOGIC AND COMMAND PARSER
# =========================================================================

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then

    if [ $# -eq 0 ]; then
        # Default: update status and exit (run this periodically via cron or manually)
        if [ -f "$PWD/main.yaml" ]; then
            statusUpdater "$PWD"
            exit 0
        else
            echo "ERROR: main.yaml not found in current directory"
            echo "Run '$0 help' to see usage information"
            exit 1
        fi
    
    elif [ "$1" = "help" ]; then
    echo "fold.sh - Job management for batch simulations"
    echo ""
    echo "Usage: $0 COMMAND [arguments]"
    echo ""
    echo "Commands:"
    echo "  init                 : Initialize the database for tracking folders and jobs"
    echo "  start <path>         : Initialize directory structure from main.yaml"
    echo "  run <path>           : Start workflow for all simulations (submit first stage)"
    echo "  view-folders         : View all tracked folders in the database"
    echo "  view-jobs            : View all tracked jobs in the database"
    echo "  view-runs            : View all setup run history"
    echo "  update-progress <path> : Update progress for all jobs based on energy.dat files"
    echo "  view-progress        : View job progress with percentage completion"
    echo "  status <path>        : Update job statuses and progress stages"
    echo ""
    echo "Workflow:"
    echo "  1. $0 init                    # Initialize database"
    echo "  2. $0 start \$PWD              # Setup directories and create scripts"
    echo "  3. $0 run \$PWD                # Submit first stage jobs"
    echo "  4. $0                         # Update statuses (run periodically)"
    echo "  5. $0 view-progress           # Check progress"
    echo ""
    echo "Status Codes:"
    echo "  0 = Not started/Completed"
    echo "  1 = Completed successfully"
    echo "  2 = Running/Pending"
    echo "  3 = Failed/Other"
    exit 0

elif [ "$1" = "init" ]; then
    initDatabase

elif [ "$1" = "start" ]; then
    if [ $# -ne 2 ]; then
        echo "ERROR: start command requires a path argument"
        echo "Usage: $0 start <path>"
        exit 1
    fi
    
    if [ ! -d "$2" ]; then
        echo "ERROR: Directory not found: $2"
        exit 1
    fi
    
    setupDirectoryStructure "$2"
    echo ""
    echo "=========================================================================="
    echo "Setup complete! Now starting workflow..."
    echo "=========================================================================="
    echo ""
    startWorkflow "$2"
    exit 0

elif [ "$1" = "view-folders" ]; then
    echo "Tracked folders in database '$db_name':"
    sqlite3 -header -column "$db_name" "SELECT id, simulation_name, replica_number, path, created_at FROM folders ORDER BY simulation_name, replica_number;"
    exit 0

elif [ "$1" = "view-jobs" ]; then
    echo "Tracked jobs in database '$db_name':"
    sqlite3 -header -column "$db_name" "SELECT * FROM jobs ORDER BY id;"
    exit 0

elif [ "$1" = "view-runs" ]; then
    echo "Setup run history from database '$db_name':"
    sqlite3 -header -column "$db_name" "SELECT * FROM setup_runs ORDER BY run_timestamp DESC;"
    exit 0

elif [ "$1" = "update-progress" ]; then
    if [ $# -ne 2 ]; then
        echo "ERROR: update-progress command requires a path argument"
        echo "Usage: $0 update-progress <path>"
        exit 1
    fi
    
    if [ ! -d "$2" ]; then
        echo "ERROR: Directory not found: $2"
        exit 1
    fi
    
    updateJobProgress "$2"
    exit 0

elif [ "$1" = "view-progress" ]; then
    echo "Job progress from database '$db_name':"
    sqlite3 -header -column "$db_name" "SELECT id, location, stage, progress || '%' as progress, max_steps FROM jobs WHERE stage IS NOT NULL ORDER BY id;"
    exit 0

elif [ "$1" = "run" ]; then
    if [ $# -ne 2 ]; then
        echo "ERROR: run command requires a path argument"
        echo "Usage: $0 run <path>"
        exit 1
    fi
    
    if [ ! -d "$2" ]; then
        echo "ERROR: Directory not found: $2"
        exit 1
    fi
    
    # First update statuses before starting new jobs
    echo "Checking current job statuses..."
    statusUpdater "$2"
    echo ""
    
    # Then start workflow
    startWorkflow "$2"
    exit 0

elif [ "$1" = "status" ]; then
    if [ $# -ne 2 ]; then
        echo "ERROR: status command requires a path argument"
        echo "Usage: $0 status <path>"
        exit 1
    fi
    
    if [ ! -d "$2" ]; then
        echo "ERROR: Directory not found: $2"
        exit 1
    fi
    
    statusUpdater "$2"
    exit 0

else
    echo "ERROR: Unknown command '$1'"
    echo "Run '$0 help' to see usage information"
    exit 1
fi

fi

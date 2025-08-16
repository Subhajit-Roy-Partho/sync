start=0
stop=0
step=4
declare -a program=("/scratch/sroy85/Github/oxOriginal/build/bin/oxDNA inputMC"
         "/scratch/sroy85/Github/oxOriginal/build/bin/oxDNA inputForce"
         "/scratch/sroy85/Github/oxOriginal/build/bin/oxDNA input")
declare -a output=("outMC.txt"
        "outForce.txt"
        "out.txt")

function createDir(){ #dirName
    if [ ! -d "$1" ];then
        mkdir -p "$1"
    else
        echo "ouput exists"
    fi
}

run_simulation_pipeline() {
    # --- Function Setup ---
    if [[ "$#" -ne 2 ]]; then
        echo "Usage: run_simulation_pipeline <programs_array_name> <output_files_array_name>" >&2
        return 2
    fi

    local -n progs=$1
    local -n files=$2

    # --- Logic ---
    local num_stages=${#progs[@]}
    echo "--- Simulation Pipeline ---"
    echo "Found ${num_stages} total stages."
    echo

    # 1. Determine which stage to start from by finding the LAST existing file.
    local start_stage=0 # Default to stage 0 if no files are found
    echo "Searching for the last completed stage..."

    # Loop backwards from the last possible stage to the first
    for (( i=${num_stages}-1; i>=0; i-- )); do
        local output_file="${files[$i]}"
        if [[ -f "$output_file" ]]; then
            # We found the last stage that was at least started.
            # This is our restart point.
            start_stage=$i
            local stage_num=$((i + 1))
            echo "Restart point found: Stage ${stage_num} ('${output_file}')."
            echo "The script will re-run this stage and any subsequent ones."
            break # Exit the search loop
        fi
    done

    # If the loop finished without finding any file, start_stage is still 0.
    if [[ $start_stage -eq 0 && ! -f "${files[0]}" ]]; then
        echo "No output files found. Starting from the beginning (Stage 1)."
    fi

    echo # Blank line for readability

    # 2. Execute the necessary stages
    echo "--- Starting Execution ---"
    for (( i=$start_stage; i<num_stages; i++ )); do
        local stage_num=$((i + 1))
        local command="${progs[$i]}"
        local output_file="${files[$i]}"

        echo
        echo ">>>>> Executing Stage ${stage_num} of ${num_stages} <<<<<"
        echo "COMMAND: ${command}"
        
        # We will OVERWRITE the log file for the stage we are running.
        # This ensures a clean log for the current execution attempt.
        echo "OUTPUT:  Overwriting ${output_file}"

        # Execute the command, redirecting stdout and stderr to the output file.
        # Using '>' (overwrite) instead of '>>' (append) because we are re-running the step.
        eval "${command}" > "${output_file}" 2>&1

        # Check the exit code of the last command
        if [[ $? -ne 0 ]]; then
            echo
            echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            echo "ERROR: Command for stage ${stage_num} failed."
            echo "Check '${output_file}' for details. Aborting pipeline."
            echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            return 1 # Exit function with an error status
        else
            echo "Stage ${stage_num} completed successfully."
        fi
    done

    echo
    echo "--- Simulation Pipeline Finished Successfully ---"
    return 0
}

function jobSubmitter(){ #start, step, stop
    createDir output
    cd output
    if [ $2 -eq 0 ]; then
        echo "Single job"
        rsync -av --ignore-existing ../main/* .
        rsync -rzvP ../main/input .
        run_simulation_pipeline program output
        
    elif [ $1 -eq $3 ]; then
        echo "Replicas of $2 will be created"
        for i in $(seq 1 $2); do
            createDir "$i"
            rsync -av --ignore-existing ../main/* "$i"
            rsync -rzvP ../main/input "$i"
            cd "$i"
            run_simulation_pipeline program output > progress.txt &
            cd ..
        done
    else
        echo "Multiple jobs from $1 to $3 with step $2"
        for i in $(seq $1 $2 $3); do
            createDir "$i"
            rsync -av --ignore-existing ../main/* "$i"
            rsync -rzvP ../main/input "$i"
            echo "T=$i" >> "$i/input"
            cd "$i"
            run_simulation_pipeline program output > progress.txt &
            cd ..
        done
    fi
    cd ..
    
}

function plotter(){ #start, step, stop inputFile
    echo 'set terminal png
    set output "plot.png"
    set xlabel "Time (SU)"
    set ylabel "Energy (SU)"
    set logscale x
    '>plot.gp

    for i in $(seq $1 $2 $3); do
        if [ "$i" = "$1" ]; then
            echo 'plot "output/'$i'/energy.dat" u 1:4 w l t "T='$i'"'>>plot.gp
        else
            echo 'replot "output/'$i'/energy.dat" u 1:4 w l t "T='$i'"'>>plot.gp
        fi
        cd "output/$i"
        rm -rf last.mgl
        terminal autoconvert -i "$4" -d "last_conf.dat" -o "last.mgl" &
        cd ../..
    done

    echo "set output 'plot.png'
    replot">>plot.gp
    gnuplot plot.gp
}

function DNAmean(){
    # Create output/all
    mkdir -p output/all

    out="output/all/trajectory.dat"
    : > "$out"   # truncate/create

    found=0

    echo "Combining trajectory.dat files"

    # Iterate output subfolders in natural sort order, skip 'all'
    for d in $(ls -1v output 2>/dev/null); do
        [ "$d" = "all" ] && continue
        dir="output/$d"
        traj="$dir/trajectory.dat"
        if [ -f "$traj" ]; then
            # ensure a newline separates concatenated files
            if [ -s "$out" ]; then
                printf "\n" >> "$out"
            fi
            cat "$traj" >> "$out"
            found=1
        fi
        echo "Processed $d: $traj"
    done

    if [ "$found" -eq 0 ]; then
        echo "No trajectory.dat files found to combine." >&2
        return 1
    fi

    # Copy main/input.top into output/all/
    if [ -f main/input.top ]; then
        cp -v main/input.top output/all/
    else
        echo "Warning: main/input.top not found" >&2
    fi

    # Run oat mean inside output/all
    echo "Calculating mean trajectory using oat mean"


    cpu_count=$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)
    case $cpu_count in ''|*[!0-9]*) cpu_count=1 ;; esac
    echo "Using $cpu_count CPU cores for oat mean calculation"
    (cd output/all && oat mean -o mean.dat -p "$cpu_count" -d dev.json trajectory.dat)


    rc=$?
    if [ $rc -ne 0 ]; then
        echo "oat mean failed with exit code $rc" >&2
        return $rc
    fi

    return 0
}

# jobSubmitter $start $step $stop

if [ $# -eq 0 ];then
    echo "Main function called"
    jobSubmitter $start $step $stop
    # jobHandler program output
elif [ $1 = "plot" ];then
    plotter $start $step $stop $inputFile
elif [ $1 = "submit" ];then
    jobSubmitter $start $step $stop $inputFile
elif [ $1 = "mean" ];then
    DNAmean
elif [ $1 = "help" ];then
    echo "Usage: script.sh [plot|submit|mean|help]"
    echo "  plot: Generate plots from simulation data"
    echo "  submit: Submit jobs for simulation"
    echo "  mean: Calculate mean trajectory from all simulations"
    echo "  help: Show this help message"
fi
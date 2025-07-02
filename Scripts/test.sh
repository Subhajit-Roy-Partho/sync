#!/bin/bash

# ==============================================================================
#  The Core Pipeline Function
# ==============================================================================
# This function is generic and does not need to be edited.
# It takes two arguments: the NAME of the programs array and the NAME of the
# output files array.

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


# ==============================================================================
#  Main Script Execution (This is the only part you need to edit)
# ==============================================================================

main() {
    # --- Configuration ---
    declare -a PROGRAMS=(
        "/scratch/sroy85/Github/oxOriginal/build/bin/oxDNA inputMC"
        "/scratch/sroy85/Github/oxOriginal/build/bin/oxDNA inputForce"
        "/scratch/sroy85/Github/oxOriginal/build/bin/oxDNA input"
    )

    declare -a OUTPUT_FILES=(
        "outMC.txt"
        "outForce.txt"
        "out.txt"
    )

    # --- Run the pipeline ---
    run_simulation_pipeline PROGRAMS OUTPUT_FILES
    
    # Capture and exit with the status code from the function
    exit $?
}

# Call the main function to start the script
main
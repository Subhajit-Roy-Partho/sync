#!/bin/bash

# Get the absolute path to the database, located in the same directory as the script.
db_name="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/test.db"
# fold.sh - Manages batch job submissions based on folder structure and YAML configuration
# Usage: fold.sh start $PWD

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
            grep -A 20 "^Input:" "$yaml_file" | grep "CopyAllFiles:" | sed 's/.*CopyAllFiles: *//;s/ *$//'
            ;;
    esac
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
    
    # Process each folder in the input directory
    local count=0
    for sim_folder in "$input_path"/*/; do
        if [ ! -d "$sim_folder" ]; then
            continue
        fi
        
        # Get the folder name without trailing slash
        local sim_name=$(basename "$sim_folder")
        
        echo "Processing simulation: $sim_name"
        
        # Create simulation folder in output
        local sim_output="$output_path/$sim_name"
        mkdir -p "$sim_output"
        
        # Create replica folders (0, 1, 2, 3, ...)
        for ((i=0; i<replicas; i++)); do
            local replica_folder="$sim_output/$i"
            mkdir -p "$replica_folder"
            
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
        echo ""
    done
    
    echo "=========================================================================="
    echo "Setup complete!"
    echo "Total simulations processed: $count"
    echo "Total replica folders created: $((count * replicas))"
    echo "=========================================================================="
}

# =========================================================================
# MAIN SCRIPT LOGIC AND COMMAND PARSER
# =========================================================================

if [ $# -eq 0 ]; then
    echo "fold.sh - Job management for batch simulations"
    echo ""
    echo "Usage: $0 COMMAND [arguments]"
    echo ""
    echo "Commands:"
    echo "  start <path>  : Initialize directory structure from main.yaml"
    echo ""
    echo "Example:"
    echo "  $0 start \$PWD"
    exit 1

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

else
    echo "ERROR: Unknown command '$1'"
    echo "Run '$0' without arguments to see usage information"
    exit 1
fi

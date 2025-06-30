#!/bin/bash

# Script: empty.sh
# Description: Submits an "empty" Slurm job that starts a detached screen session.
# Usage: ./empty.sh [options]
# Options:
#   -p, --partition <partition_name>  (Default: general)
#   -q, --qos <qos_name>              (Default: private)
#   -t, --time <time_limit>           (Default: 7-00:00:00)
#   -c, --cpus-per-task <num_cpus>    (Default: 4)
#   -m, --mem <memory>                (Default: 8G, e.g., 8G, 16000M)
#   -J, --job-name <job_name>         (Default: empty_screen_job)
#   -A, --account <account_name>      (Optional: Slurm account)
#   -o, --output <output_file_pattern> (Default: slurm-<job_name>-%j.out)
#   -e, --error <error_file_pattern>   (Default: slurm-<job_name>-%j.err)
#   --extra-sbatch "<options>"        (Pass additional raw SBATCH options)
#   --help                            Show this help message

# --- Default values ---
partition_val="general"
qos_val="private" # From your original request
time_val="7-00:00:00"
cpus_val="4"
mem_val="8G"
job_name_val="empty_screen_job"
account_val=""
output_val="slurm-${job_name_val}-%j.out" # Default output file
error_val="slurm-${job_name_val}-%j.err"   # Default error file
extra_sbatch_opts=""

# --- Function to display help ---
show_help() {
    # Using a heredoc for the help message
    cat <<HELP_USAGE
Usage: $(basename "$0") [options]

Submits an "empty" Slurm job that starts a detached screen session.

Options:
  -p, --partition <name>      Slurm partition (Default: ${partition_val})
  -q, --qos <name>            Slurm QOS (Default: ${qos_val})
  -t, --time <D-HH:MM:SS>     Time limit (Default: ${time_val})
  -c, --cpus-per-task <num>   CPUs per task (Default: ${cpus_val})
  -m, --mem <memory>          Memory (e.g., 8G, 16000M) (Default: ${mem_val})
  -J, --job-name <name>       Job name (Default: ${job_name_val})
  -A, --account <name>        Slurm account (Optional)
  -o, --output <pattern>      Output file pattern (Default: ${output_val})
  -e, --error <pattern>       Error file pattern (Default: ${error_val})
  --extra-sbatch "<options>"  Pass additional raw SBATCH options.
                              Example: --extra-sbatch "#SBATCH --gres=gpu:1"
                              For multiple, use newlines: --extra-sbatch $'#SBATCH --option1\n#SBATCH --option2'
  --help                      Show this help message

Example:
  $(basename "$0") -t 1-00:00:00 -c 2 --mem 4G -J "my_interactive_session"
HELP_USAGE
}

# --- Parse arguments ---
# Using getopts for more robust parsing of short options
# For long options, we'll use a combined approach.
# Store original args for parsing long options
original_args=("$@")
positional_args=()

while (( "$#" )); do
  case "$1" in
    -p|--partition)
      if [ -n "$2" ] && [ "${2:0:1}" != "-" ]; then
        partition_val="$2"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    -q|--qos)
      if [ -n "$2" ] && [ "${2:0:1}" != "-" ]; then
        qos_val="$2"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    -t|--time)
      if [ -n "$2" ] && [ "${2:0:1}" != "-" ]; then
        time_val="$2"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    -c|--cpus-per-task)
      if [ -n "$2" ] && [ "${2:0:1}" != "-" ]; then
        cpus_val="$2"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    -m|--mem)
      if [ -n "$2" ] && [ "${2:0:1}" != "-" ]; then
        mem_val="$2"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    -J|--job-name)
      if [ -n "$2" ] && [ "${2:0:1}" != "-" ]; then
        job_name_val="$2"
        # Update default output/error file names if job name changes
        output_val="slurm-${job_name_val}-%j.out"
        error_val="slurm-${job_name_val}-%j.err"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    -A|--account)
      if [ -n "$2" ] && [ "${2:0:1}" != "-" ]; then
        account_val="$2"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    -o|--output)
      if [ -n "$2" ] && [ "${2:0:1}" != "-" ]; then
        output_val="$2"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    -e|--error)
      if [ -n "$2" ] && [ "${2:0:1}" != "-" ]; then
        error_val="$2"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    --extra-sbatch)
      if [ -n "$2" ] && [ "${2:0:1}" != "-" ]; then # Check if $2 exists and is not another option
        extra_sbatch_opts="$2"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    --help)
      show_help
      exit 0
      ;;
    -*) # unsupported flags
      echo "Error: Unsupported flag $1" >&2
      show_help
      exit 1
      ;;
    *) # preserve positional arguments
      positional_args+=("$1")
      shift
      ;;
  esac
done

# Restore positional arguments (if any were collected, though this script doesn't use them)
# eval set -- "$positional_args"


# --- Construct SBATCH directives ---
sbatch_directives="#!/bin/bash\n" # Shebang for the script sbatch will execute
sbatch_directives+="#SBATCH --job-name=\"${job_name_val}\"\n"
sbatch_directives+="#SBATCH --partition=${partition_val}\n"
sbatch_directives+="#SBATCH --qos=${qos_val}\n"
sbatch_directives+="#SBATCH --time=${time_val}\n"
sbatch_directives+="#SBATCH --cpus-per-task=${cpus_val}\n"
sbatch_directives+="#SBATCH --mem=${mem_val}\n"
sbatch_directives+="#SBATCH --output=${output_val}\n"
sbatch_directives+="#SBATCH --error=${error_val}\n"

if [[ -n "$account_val" ]]; then
    sbatch_directives+="#SBATCH --account=${account_val}\n"
fi

if [[ -n "$extra_sbatch_opts" ]]; then
    # Add each line from extra_sbatch_opts as a new #SBATCH directive
    # This handles multi-line input for --extra-sbatch if formatted with newlines
    # e.g., --extra-sbatch $'#SBATCH --option1\n#SBATCH --option2'
    # Or a single line e.g. --extra-sbatch "#SBATCH --gres=gpu:1"
    sbatch_directives+="${extra_sbatch_opts}\n"
fi

# --- The command to execute within the Slurm job ---
# The \$SLURM_JOB_ID needs to be escaped so it's interpreted by the shell *inside* the Slurm job
exec_command="exec screen -Dm -S slurm-\$SLURM_JOB_ID"

# --- Submit the job ---
echo "Submitting Slurm job: ${job_name_val}"
echo -e "--- SBATCH Script to be submitted ---"
echo -e "${sbatch_directives}"
echo -e "\n# Informative messages (will also be in the .out file)"
echo -e "echo \"SLURM Job ID: \$SLURM_JOB_ID\""
echo -e "echo \"Node: \$(hostname)\""
echo -e "echo \"Screen session name: slurm-\$SLURM_JOB_ID\""
echo -e "echo \"To attach (once job is running on the allocated node): screen -r slurm-\$SLURM_JOB_ID\""
echo -e "echo \"You might need to ssh to the node \$(hostname) first to attach to the screen.\""
echo -e "\n${exec_command}"
echo -e "------------------------------------"

# Use a heredoc to pass the script to sbatch
# Ensure that variables like $SLURM_JOB_ID are escaped with a backslash (\$)
# if they should be expanded by Slurm on the compute node, not locally.
# The sbatch_directives already contain escaped variables where necessary.

# The heredoc delimiter 'EOF' being unquoted means variable expansion will happen.
# This is fine for ${sbatch_directives} and ${exec_command} as they are constructed
# with appropriate escaping already.
# For the echo commands *within* the Slurm script, we need to ensure \$SLURM_JOB_ID
# is literally passed, so it's expanded on the node.

job_submission_output=$(sbatch <<EOF
${sbatch_directives}

echo "SLURM Job ID: \$SLURM_JOB_ID"
echo "Job Name: \$SLURM_JOB_NAME"
echo "Running on node: \$(hostname)"
echo "Allocated CPUs: \$SLURM_CPUS_PER_TASK"
echo "Allocated Memory: \$SLURM_MEM_PER_NODE (or \$SLURM_MEM_PER_CPU if specified per CPU)"
echo "Screen session will be named: slurm-\$SLURM_JOB_ID"
echo "To attach (once job is running on the allocated node):"
echo "  1. Find the node: squeue -j \$SLURM_JOB_ID -o %N"
echo "  2. SSH to the node (if necessary): ssh \$(squeue -j \$SLURM_JOB_ID -h -o %N)"
echo "  3. Attach to screen: screen -r slurm-\$SLURM_JOB_ID"
echo ""

${exec_command}
EOF
)

sbatch_exit_code=$?

if [ $sbatch_exit_code -eq 0 ]; then
    echo "Job submitted successfully."
    echo "${job_submission_output}" # Shows "Submitted batch job XXXXX"
else
    echo "Error submitting job (sbatch exit code: $sbatch_exit_code)." >&2
    echo "${job_submission_output}" # Shows error message from sbatch
fi

exit $sbatch_exit_code

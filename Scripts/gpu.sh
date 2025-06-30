#!/bin/bash

# Script to output GPUs available for allocation in Slurm,
# showing only nodes that currently have one or more GPUs available.

echo "Probing Slurm for GPUs available for allocation..."
echo "=========================================================================="
printf "%-20s %-25s %-10s %-10s %-10s\n" "Node" "GPU_Type" "Configured" "Used" "Available"
echo "-------------------- ------------------------- ---------- ---------- ----------"

# Function to parse a TRES string (unchanged, it works well)
parse_tres_string() {
    local tres_string="$1"
    local -n result_map=$2

    for key_in_map in "${!result_map[@]}"; do unset result_map["$key_in_map"]; done

    if [ -z "$tres_string" ] || [[ ! "$tres_string" == *gres/gpu* ]]; then
        return
    fi

    IFS=',' read -ra tres_parts <<< "$tres_string"
    for part in "${tres_parts[@]}"; do
        if [[ "$part" == gres/gpu* ]]; then
            local gres_key="${part%%=*}"
            local gres_count="${part#*=}"
            local gpu_type_from_spec="gpu"

            if [[ "$gres_key" == gres/gpu:* ]]; then
                gpu_type_from_spec="${gres_key#gres/gpu:}"
            elif [[ "$gres_key" != "gres/gpu" ]]; then
                continue
            fi

            if [[ "$gres_count" =~ ^[0-9]+$ ]] && [ "$gres_count" -ge 0 ]; then
                 result_map["$gpu_type_from_spec"]=$(( ${result_map["$gpu_type_from_spec"]:-0} + gres_count ))
            fi
        fi
    done
}

# =================================================================
# CHANGE #1: Get a unique list of operational nodes first.
# This prevents processing the same node multiple times if it appears
# in sinfo output more than once (e.g., in different partitions).
# We filter out non-operational states and then get unique node names.
# =================================================================
operational_nodes=$(sinfo -N -h -o "%N %T" | \
    grep -v -E 'DOWN|DRAIN|DRNG|FAIL|MAINT|POWER_DOWN|POWER_UP|REBOOT|UNK' | \
    awk '{print $1}' | \
    sort -u)

if [ -z "$operational_nodes" ]; then
    echo "No operational nodes with GPUs found."
    exit 1
fi

declare -A total_available_gpus_by_type
any_node_printed=0

# =================================================================
# CHANGE #2: Loop over the clean, unique list of node names.
# =================================================================
for node_name in $operational_nodes; do
    scontrol_node_output=$(scontrol show node -o "$node_name" 2>/dev/null)
    if [ -z "$scontrol_node_output" ]; then
        continue
    fi

    cfg_tres_str=$(echo "$scontrol_node_output" | awk '{for(i=1;i<=NF;i++) if($i ~ /^CfgTRES=/) {sub(/^CfgTRES=/, "", $i); print $i; exit}}')
    alloc_tres_str=$(echo "$scontrol_node_output" | awk '{for(i=1;i<=NF;i++) if($i ~ /^AllocTRES=/) {sub(/^AllocTRES=/, "", $i); print $i; exit}}')

    if [[ "$cfg_tres_str" != *gres/gpu* ]]; then
        continue
    fi

    declare -A configured_gpus_map
    declare -A used_gpus_map

    parse_tres_string "$cfg_tres_str" configured_gpus_map
    parse_tres_string "$alloc_tres_str" used_gpus_map

    # Pruning logic for generic "gpu" key (unchanged, it's good practice)
    if [[ -n "${configured_gpus_map["gpu"]}" ]]; then
        has_specific_gpu_keys_in_conf=0
        for key in "${!configured_gpus_map[@]}"; do
            if [[ "$key" != "gpu" ]]; then
                has_specific_gpu_keys_in_conf=1
                break
            fi
        done
        if [[ "$has_specific_gpu_keys_in_conf" -eq 1 ]]; then
            unset configured_gpus_map["gpu"]
        fi
    fi

    # =================================================================
    # CHANGE #3: Simplified loop for printing.
    # Instead of buffering output, print directly for each GPU type if
    # it has available units. This fixes the garbled output and is cleaner.
    # =================================================================
    for gpu_type in "${!configured_gpus_map[@]}"; do
        conf_count=${configured_gpus_map[$gpu_type]}
        used_count=${used_gpus_map[$gpu_type]:-0}
        available_count=$(( conf_count - used_count ))

        if [ "$available_count" -lt 0 ]; then
            available_count=0
        fi

        # Only print the line if there are GPUs of this type available
        if [ "$available_count" -gt 0 ]; then
            printf "%-20s %-25s %-10s %-10s %-10s\n" \
                "$node_name" \
                "$gpu_type" \
                "$conf_count" \
                "$used_count" \
                "$available_count"

            any_node_printed=1
            total_available_gpus_by_type["$gpu_type"]=$(( ${total_available_gpus_by_type["$gpu_type"]:-0} + available_count ))
        fi
    done
done

echo "=========================================================================="

if [ "$any_node_printed" -eq 1 ]; then
    echo "Overall summary of currently available GPUs (by type, from all nodes):"
    summary_has_content=0
    # Sort keys for consistent output
    sorted_gpu_types=($(for t in "${!total_available_gpus_by_type[@]}"; do echo "$t"; done | sort))

    for gpu_type in "${sorted_gpu_types[@]}"; do
        if [ "${total_available_gpus_by_type[$gpu_type]}" -gt 0 ]; then
            printf "  %-25s : %s\n" "$gpu_type" "${total_available_gpus_by_type[$gpu_type]}"
            summary_has_content=1
        fi
    done
    if [ "$summary_has_content" -eq 0 ]; then
        # This case should ideally not be hit if any_node_printed is 1
        echo "  No specific GPU types found with available units in the summary."
    fi
    echo "--------------------------------------------------------------------------"
else
    echo "No nodes found with currently available GPUs."
fi
echo "Note: 'Available' means configured on an operational node and not allocated."
echo "      Only nodes and GPU types with at least one available GPU are listed."
echo "      Node states like DRAIN, DOWN, MAINT, etc., are excluded."

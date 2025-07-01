scontrol_node_output=$(scontrol show node -o "sg250" 2>/dev/null)
echo "$scontrol_node_output"
features_str=$(echo "$scontrol_node_output" | awk '{for(i=1;i<=NF;i++) if($i ~ /^AvailableFeatures=/) {sub(/^AvailableFeatures=/, "", $i); print $i; exit}}')
echo $features_str
if [[ "$features_str" == *"$TARGET_FEATURE"* ]]; then
    echo "it private"
fi
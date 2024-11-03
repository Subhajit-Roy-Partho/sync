program="sync.sh"
program2="sync.sh;sync.sh plot"

while true; do
    for i in {1..90};do
        eval $program
        sleep 20
    done
    eval $program2
    sleep 600
done

# 1175856

start=0.02
stop=0.2
step=0.02
execType=1
program="oxDNA input >out.txt"

function createDir(){ #dirName
    if [ ! -d "$1" ];then
        mkdir -p "$1"
    else
        echo "ouput exists"
    fi
}

function copy(){}

function jobSubmitter(){ #start, step, stop
    createDir output
    cd output
    for i in $(seq $1 $2 $3); do
        createDir "$i"
        rsync -av --ignore-existing ../main/* "$i"
        echo "T=$i" >> "$i/input"
        eval $program &
    done
    cd ..
    
}

jobSubmitter 0.02 0.02 0.2;
# for i in $(seq $start $step $stop); do
#     mkdir -p "output/$i"
#     rsync -rzvP main/* "output/$i"
#     cd "output/$i"
#     echo "T=$i" >> input
#     if [ "$execType" -eq 1 ]; then
#         eval $program &
#     fi
#     if [ "$execType" -eq 2 ]; then
#         sbatch submit.sh
#     fi
#     cd ../..
# done
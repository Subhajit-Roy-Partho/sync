start=0
stop=0
step=0
execType=1
program="oxDNA inputForce > outForce.txt ;oxDNA input >out.txt"
programContinue="oxDNA input >out.txt"
inputFile="input.phb"

function createDir(){ #dirName
    if [ ! -d "$1" ];then
        mkdir -p "$1"
    else
        echo "ouput exists"
    fi
}

function jobSubmitter(){ #start, step, stop
    createDir output
    cd output
    if [ $step -eq 0 ]; then
        echo "Single job"
        rsync -av --ignore-existing ../main/* .
        rsync -rzvP ../main/input .
         if [ -f "energy.dat" ]; then
                echo "Continuing job"
                eval $programContinue &
            else
                echo "Starting fresh job"
                eval $program &
            fi
    else
        echo "Multiple jobs from $1 to $3 with step $2"
        for i in $(seq $1 $2 $3); do
            createDir "$i"
            rsync -av --ignore-existing ../main/* "$i"
            rsync -rzvP ../main/input "$i"
            echo "T=$i" >> "$i/input"
            cd "$i"
            if [ -f "energy.dat" ]; then
                echo "Continuing job"
                eval $programContinue &
            else
                echo "Starting fresh job"
                eval $program &
            fi
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
            echo 'plot "output/'$i'/energy.ign" u 1:4 w l t "T='$i'"'>>plot.gp
        else
            echo 'replot "output/'$i'/energy.ign" u 1:4 w l t "T='$i'"'>>plot.gp
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

# jobSubmitter $start $step $stop

if [ $# -eq 0 ];then
    echo "Main function called"
    jobSubmitter $start $step $stop
elif [ $1 = "plot" ];then
    plotter $start $step $stop $inputFile
elif [ $1 = "submit" ];then
    jobSubmitter $start $step $stop $inputFile
fi
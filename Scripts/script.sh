start=0
stop=0
step=4
execType=1
program="/scratch/sroy85/Github/oxOriginal/build/bin/oxDNA inputForce > outForce.txt ;/scratch/sroy85/Github/oxOriginal/build/bin/oxDNA input >out.txt"
programContinue="/scratch/sroy85/Github/oxOriginal/build/bin/oxDNA input >out.txt"
inputFile="input.phb"

# program="pmemd.cuda -O -i prod.mdin -p new.prmtop -c prod.rst7 -x prod.nc -inf prod.mdinfo -o energy.dat -r prod.rst7 -ref prod.rst7"
# programContinue="pmemd.cuda -O -i prod.mdin -p new.prmtop -c prod.rst7 -x prod.nc -inf prod.mdinfo -o energy.dat -r prod.rst7 -ref prod.rst7"


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
    if [ $2 -eq 0 ]; then
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
    elif [ $1 -eq $3 ]; then
        echo "Replicas of $2 will be created"
        for i in $(seq 1 $2); do
            createDir "$i"
            rsync -av --ignore-existing ../main/* "$i"
            rsync -rzvP ../main/input "$i"
            cd "$i"
            if [ -f "energy.dat" ]; then
                echo "Continuing job"
                if [ -f "prod.nc" ]; then
                  if [ -f "combined.nc" ];then
                    echo "Merging nc files"
                    cp -r combined.nc "combined_$RANDOM.nc"
		    cpptraj script.cpptraj
                    mv merged_combined.nc combined.nc
                    cp prod.nc "prod_$(date +%s).nc"
                    cat energy.dat >> combined.dat
		    cp energy.dat "energy_$(date +%s).dat"
		    rm energy.dat
                  else
                    cp prod.nc combined.nc
		    cp proc.nc prod_1.nc
                    cp energy.dat combined.dat
		    cp energy.dat energy_1.dat
                    echo "prod renamed to combined"
                  fi
                else
                  echo "This is not a AMBER simulation"
                fi
                eval $programContinue &
            else
                echo "Starting fresh job"
                eval $program &
            fi
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

# jobSubmitter $start $step $stop

if [ $# -eq 0 ];then
    echo "Main function called"
    jobSubmitter $start $step $stop
elif [ $1 = "plot" ];then
    plotter $start $step $stop $inputFile
elif [ $1 = "submit" ];then
    jobSubmitter $start $step $stop $inputFile
fi

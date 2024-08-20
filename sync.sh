db_name="/scratch/sroy85/sync/test.db"

function updateStatus(){ #jobid stautus
    sqlite3 "$db_name" "UPDATE jobs SET STATUS='$2' WHERE JOBID='$1';"
}

function jobSubmitter(){ 
    result=$(sqlite3 "$db_name" "SELECT * FROM jobs WHERE STATUS=1;");
    if [[ -n "$result" ]]; then
        while IFS='|' read -r id jobid status location type script ; do
            # echo "Jobid: $jobid"
            # echo "Status: $status"
            # echo "Location: $location"
            # echo "Type: $type"
            # echo "Script: $script"
            cd "$location"
            if [ $type="slurm" ];then
                newjobid=$(sbatch $script)
                newjobid=${newjobid##* }
                echo "Jobid: $newjobid"
                sqlite3 "$db_name" "UPDATE jobs SET JOBID='$newjobid' WHERE JOBID='$jobid';"
                updateStatus $newjobid 2
            elif [ $type="bash" ];then
                bash $script
            fi

            updateStatus $jobid 2
        done <<< "$result";
    fi

}

function checkJob(){
    status=$(squeue -h -j $1 -o "%T")

    if [[ -z "$status" ]]; then
        echo "The job $1 has finished"
        return 1;
    else
        case $status in
        "RUNNING")
            echo "The job $1 is running"
            return 0
            ;;
        "PENDING")
            echo "The job $1 is pending in queue"
            return 2
            ;;
        *)
            echo "The job $1 is in: $status"
            return 3
            ;;
        esac
    fi
}

function statusUpdater(){
    result=$(sqlite3 "$db_name" "SELECT jobid FROM jobs;");
    while IFS=, read -r jobid; do
        checkJob $jobid
        case $? in
        0)
            updateStatus $jobid 0
            ;;
        1)
            updateStatus $jobid 1
            ;;
        2)
            updateStatus $jobid 2
            ;;
        3)
            updateStatus $jobid 3
            ;;
        esac
    done <<< "$result";
}

function submitFirstJob(){ # *location, script, resume script
    cd "$1"
    if [ $# -lt 2 ]; then
        newjobid=$(sbatch start.sh)
    else
        newjobid=$(sbatch $2)
    fi
    newjobid=${newjobid##* }
    echo "Jobid: $newjobid"
    if [ $# -lt 3 ]; then
        sqlite3 "$db_name" "INSERT INTO jobs (jobid,status,location,type,script) VALUES ('$newjobid',2,'$1','slurm','resume.sh');";
    else
        sqlite3 "$db_name" "INSERT INTO jobs (jobid,status,location,type,script) VALUES ('$newjobid',2,'$1','slurm','$3');"
    fi
    # updateStatus $newjobid 2
}

function deleteJob(){ #jobid
    if [ $# -eq 0 ];then
        result=$(sqlite3 "$db_name" "SELECT * FROM jobs;")
        while IFS='|' read -r id jobid status location type script ; do
            scancel $jobid
            # sqlite3 "$db_name" "DELETE FROM jobs WHERE JOBID='$jobid';"
        done <<< "$result";
        sqlite3 "$db_name" "DELETE FROM jobs;"
    else
        sqlite3 "$db_name" "DELETE FROM jobs WHERE JOBID='$1';"
        scancel $1
    fi
}

function restartJob(){ #jobid startScript, resumeScript
    result=$(sqlite3 "$db_name" "SELECT * FROM jobs WHERE JOBID='$1';");
    if [[ -n "$result" ]]; then
        IFS='|' read -r id jobid status location type script<<<$result
        deleteJob $1
        if [ $# -eq 3 ]; then
            submitFirstJob $location $2 $3
        elif [ $# -eq 2 ]; then
            submitFirstJob $location $2
        else
            submitFirstJob $location
        fi
    fi
}

function plotter(){ #plotScript
    result=$(sqlite3 "$db_name" "SELECT * FROM jobs;")
    while IFS='|' read -r id jobid status location type script ; do
        cd "$location"
        pwd
        eval bash $1 &
    done <<< "$result";
}

if [ $# -eq 0 ];then
    echo "Main function called"
    statusUpdater
    jobSubmitter

elif [ $1 = "init" ];then
    sqlite3 "$db_name" "CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, jobid TEXT, status INTEGER, location TEXT, type TEXT, script TEXT);"
    echo "Database has been initialized"
elif [ $1 = "submit" ];then
    if [ $# -eq 3 ]; then
        submitFirstJob $2 $3
    elif [ $# -eq 2 ]; then
        submitFirstJob $2
    else
        echo "Incorrect number of arguments were passed"
    fi
elif [ $1 = "start" ];then
    if [ $# -eq 4 ]; then
        submitFirstJob $2 $3 $4
    elif [ $# -eq 3 ]; then
        submitFirstJob $2 $3 $3
    elif [ $# -eq 2 ]; then
        submitFirstJob $2 "start.sh" "start.sh"
    else
        echo "Incorrect number of arguments were passed"
    fi

elif [ $1 = "restart" ];then
    if [ $# -eq 2 ]; then
        restartJob $2
    else
        echo "Incorrect number of arguments were passed"
    fi
    
elif [ $1 = "insert" ];then
    if [ $# -eq 6 ]; then
        sqlite3 "$db_name" "INSERT INTO jobs (jobid,status,location,type,script) VALUES ('$2','$3','$4','$5','$6');"
    else
        echo "Incorrect number of arguments were passed"
    fi
elif [ $1 = "view" ];then
    result=$(sqlite3 "$db_name" "SELECT * FROM jobs;")
    echo "$result"
elif [ $1 = "delete" ];then
    if [ $# -eq 2 ]; then
        if [ $2 = "all" ];then
            deleteJob
        else
            deleteJob $2
        fi
    else
        echo "Incorrect number of arguments were passed"
    fi
# elif [ $1 = "deleteall" ];then
#     sqlite3 "$db_name" "DELETE FROM jobs;"
#     echo "All jobs have been deleted from the database"

elif [ $1 = "update" ];then
    if [ $# -eq 3 ]; then
        updateStatus $2 $3
    else
        echo "Incorrect number of arguments were passed"
    fi
elif [ $1 = "autoupdate" ];then
    statusUpdater
elif [ $1 = "plot" ];then
    if [ $# -eq 2 ]; then
        plotter $2
    elif [ $# -eq 1 ]; then
        # plotter "plot.sh"
        plotter "script.sh plot"
    else
        echo "Incorrect number of arguments were passed"
    fi
else
    echo "Invalid command"
    echo "Usage: $0 [insert|view|delete|update|autoupdate|init|submit|start|restart|plot]"
fi

#!/bin/bash
#get filename from command line
set -x
path=$1
bfsPath=${1:6}
statsdHost="brazil.vampire 8125"

echo "lfs.migrate.start:1|c" | nc -w 1 -u brazil.vampire 8125
function writeStatsd {
    echo $1 | nc -w 1 -u $statsdHost
}

writeStatsd "lfs.migrate.start:1|c"

if [[ ! -e $path ]]; then
    if [[ -e /lio/lfs/cms$path ]]; then
        echo "Source file missing, target file exists. Okay"
        exit 0
    else
        echo "Source file missing, target file also missing. Oh no"
        exit 2
    fi
fi

migrationVersion=$(lio_getattr -al user.bfs_migration_version @:/cms/$path 2>/dev/null | grep user.bfs_migration_version | sed 's/.*=//')
currentVersion=2
attributeVersion=2
checksumVersion=2
movementVersion=2
needToChecksum=0
if [[ "$migrationVersion" == "(null)" ]]; then
    migrationVersion=0
elif [[ $migrationVersion -eq $currentVersion ]]; then
    echo "Migration up to date, exiting"
    exit 0
else
    echo "Already migrated with version $migrationVersion"
fi

#get cksum and adler32 from BFS - possibly caching results if they don't exist
function getBFSAttr {
    gotResult=0
    for counter in 2 4 6 8 10 12 14 16 18 20; do
        val=$(lst-getAttr -u cms -p neutralpion $1 cms-lstore.vampire:$2)
        if [[ $? -eq 0 && $val != *FATAL* ]]; then
            echo $val
            gotResult=1
            break
        fi
        sleep $counter
    done
    if [[ ! $gotResult ]]; then
        echo "Couldn't get attribute from lstore"
        exit 5
    fi
}
accreCksum_tmp=$(getBFSAttr accre_cksum $bfsPath)
accreAdler32_tmp=$(getBFSAttr accre_adler32 $bfsPath)

accreCksum_bfs=${accreCksum_tmp:12}
accreAdler32_bfs=${accreAdler32_tmp:14}

if [[ "$accreAdler32_bfs" == "not found" || "$accreCksum_bfs" == "not found" || \
        -z "$accreAdler32_bfs" || -z "$accreCksum_bfs" || \
        "$accreAdler32_bfs" == "0" || "$accreCksum_bfs" == "0" ]]; then
    needToChecksum=1
    echo "Forcing a checksum of file in BFS (in background)"
    (exec java -Dchecksum.timeoutInMinutes=15 -cp /nfs_mounts/home/meloam/checksummer/debug-1.0-jar-with-dependencies.jar org.lstore.client.debug.DelegateChecksum $bfsPath)
else
    echo "Checksums appear to exist: \"$accreAdler32_bfs\" \"$accreCksum_bfs\""
fi
#attempt to copy to LFS
dirLen=$[${#path} - 41]
endDirPath=$(dirname $path)
if [[ $migrationVersion -lt $movementVersion ]]; then
    if [[ ! -d  /lio/lfs/cms$endDirPath ]]; then
        mkdir -p /lio/lfs/cms$endDirPath
    fi
    cpOutput=$(lio_cp $path @:/cms$path 2>&1)

    cpErrorCode=$?
    cpErrorStatus=$(echo $cpOutput | grep ERROR)

    if [[ $cpErrorCode -ne 0 ]]; then
        echo "Copy failed: lio_cp error code "$cpErrorCode
        exit 1
    elif [[ -n $cpErrorStatus ]]; then
        echo "Copy failed: error on copy"
        echo $cpErrorStatus
        exit 1
    fi

    #check if transfer was successful
    inspectOutput=$(lio_inspect -f -o inspect_quick_repair @:/cms$path)
    inspectErrorCode=$?
    inspectErrorStatus=$(echo $inspectOutput | grep ERROR)

    if [[ $inspectErrorCode -ne 0 ]]; then
        echo "Inspect failed: lio_inspect error code "$inspectErrorCode
        exit 1
    elif [[ -n $inspectErrorStatus ]]; then
        echo "Inspect failed: error in inspection"
        echo $inspectErrorStatus
        exit 1
    fi
fi

# do the actual move
if [[ $migrationVersion -lt $checksumVersion ]]; then
    lio_get @:/cms$path | ./checksum.py @:/cms$path
fi

if [[ $needToChecksum -eq 1 ]]; then
    # Wait on the BFS checksummer to come back
    wait
    accreCksum_tmp=$(getBFSAttr accre_cksum $bfsPath)
    accreAdler32_tmp=$(getBFSAttr accre_adler32 $bfsPath)

    accreCksum_bfs=${accreCksum_tmp:12}
    accreAdler32_bfs=${accreAdler32_tmp:14}
fi

# double check the checksum in LFS matches the one in BFS
lfsAttr=$(lio_getattr -ga "user.accre*" @:/cms$path | grep user.accre | sed 's/^\s*//')
accreCksum_lfs=$(echo "$lfsAttr" | awk '/accre_cksum/{print; exit}' | sed 's/.*=//')
accreAdler32_lfs=$(echo "$lfsAttr" | awk '/accre_adler32/{print; exit}' | sed 's/.*=//')
if [[ $accreCksum_bfs != $accreCksum_lfs || $accreAdler32_bfs != $accreAdler32_lfs ]]; then
    echo "Error: Checksums don't match between BFS and LFS"
    echo "$accreCksum_bfs != $accreCksum_lfs && $accreAdler32_bfs != $accreAdler32_lfs"
    # Should I delete the bad file?
    exit 3
fi
#set attributes in LFS
if [[ $migrationVersion -lt $attributeVersion ]]; then
    phedexCksum_tmp=$(getBFSAttr phedex_cksum $bfsPath)
    phedexAdler32_tmp=$(getBFSAttr phedex_adler32 $bfsPath)
    phedexCksum_bfs=${phedexCksum_tmp:13}
    phedexAdler32_bfs=${phedexAdler32_tmp:15}
    if [ ! -z "$accreCksum_bfs" ]; then
        lio_setattr -as user.bfs.accre.cksum=$accreCksum_bfs @:/cms$path || { echo "SetAttr accre.cksum failed!"; exit 1; }
    fi
    if [ ! -z "$accreAdler32_bfs" ]; then
        lio_setattr -as user.bfs.accre.adler32=$accreAdler32_bfs @:/cms$path || { echo "SetAttr accre.adler32 failed!"; exit 1; }
    fi
    if [ ! -z "$phedexCksum_bfs" ]; then
        lio_setattr -as user.bfs.phedex.cksum=$phedexCksum_bfs @:/cms$path || { echo "SetAttr phedex.cksum failed!"; exit 1; }
    fi
    if [ ! -z "$phedexAdler32_bfs" ]; then
        lio_setattr -as user.bfs.phedex.adler32=$phedexAdler32_bfs @:/cms$path || { echo "SetAttr phedex.adler32 failed!"; exit 1; }
    fi
fi
lio_setattr -as user.bfs_migration_version=$currentVersion @:/cms$path || { echo "SetAttr migrationversion failed!"; exit 1; }
exit 0

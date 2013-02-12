# Seems like Cloudfiles has socket errors which abort the transfer and
#  the script will terminate abnormally.

# This wrapper will execute the script over and over again until exit status is 0.

function retry() {
   nTrys=0
   maxTrys=200
   status=256
   until [ $status == 0 ] ; do
      $1
      status=$?
      nTrys=$(($nTrys + 1))
      if [ $nTrys -gt $maxTrys ] ; then
            echo "Number of re-trys exceeded. Exit code: $status" >> filewalker_undead.log
            exit $status
      fi
      if [ $status != 0 ] ; then
            echo "Failed (exit code $status)... retry $nTrys" >> filewalker_undead.log
      fi
   done
}

retry "python2.6 filewalker.py backup --config filewalker.conf" 


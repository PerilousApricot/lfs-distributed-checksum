#!/bin/bash

for HOST in se4.accre.vanderbilt.edu se9.accre.vanderbilt.edu se11.accre.vanderbilt.edu vmp801 vmp802 vmp803 vmp805; do
    echo "Killing worker on $HOST"
     ssh $HOST 'ps aux | grep worker-real | grep meloam | grep python | grep -v bash | awk "{print \$2 }" |xargs kill'
done

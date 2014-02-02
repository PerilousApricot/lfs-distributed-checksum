#!/usr/bin/env python2.6

import random
count = 1
timer = 10


tosort = {}
for _ in range(10):
    for _ in range(3):
        tosort[random.randint(0,1000)] = \
                {'requestedTime' : timer,
                 'priority' : random.randint(0,3) }
        timer += 1
def sortFunc(x,y):
    x = x[1]
    y = y[1]
    print "sorted x %s, y %s" % (x,y)
    if x['priority'] != y['priority']:
        return y['priority'] - x['priority']
    else:
        return x['requestedTime'] - y['requestedTime']

print sorted(tosort.items(), cmp=sortFunc)

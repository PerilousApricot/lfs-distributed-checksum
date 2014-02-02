#!/usr/bin/env python2.6
"""
    migrateDirectory.py paths_to_move ...

    Moves files from BFS to LFS. Hopefully does the right thing
"""

import multiprocessing
import optparse
import os
import os.path
import subprocess
import sys
import time

# constants
concurrency = 100
maxRetries = 1
monitorDelta = 5

parser = optparse.OptionParser()
parser.add_option('-f', '--file', dest='filelist',
                  help='file containing list of files',
                  default='')
opts,args = parser.parse_args()

def moveOneFile(filename):
    p = subprocess.Popen(["./clientFilemove.py",filename],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    output, _ = p.communicate()
    return (filename, p.returncode,output)

def getCallback(result):
    """
    called in main thread, updates global state
    """
    print "Received callback"
    for row in result:
        if row == None:
            continue
        if row[1] == 0:
            fileList[row[0]]['state'] = 'complete'
        else:
            fileList[row[0]]['state'] = 'failed'
            fileList[row[0]].setdefault('retries',0)
            fileList[row[0]]['retries'] += 1
        fileList[row[0]]['ret'] = row[1]
        fileList[row[0]]['out'] = row[2]

print "Extracting file list"
fileList = {}
for oneArg in args:
    for root, dirnames, filenames in os.walk(oneArg):
        for filename in filenames:
            fileList[os.path.join(root,filename)] =\
                        {'state':'new'}

if opts.filelist:
    fh = open(opts.filelist, 'r')
    for oneFile in fh:
        # remove trailing newline
        fileList[oneFile[:-1]] = {'state':'new'}


print "Processing %i files for transfer" % len(fileList)
def tallyJobs():
    stateCount = {"new":0, "complete":0, "failed":0}
    for k,v in fileList.iteritems():
        stateCount[v['state']] += 1
    return stateCount

p = multiprocessing.Pool(concurrency)
inFlight = {}
monitorUpdateTime = time.time()
while True:
    # Give an update about our status
    if monitorUpdateTime + monitorDelta < time.time():
        stateDict = {}
        for k,v in fileList.iteritems():
            stateDict.setdefault(v['state'],0)
            stateDict[v['state']] += 1
        outMsg = []
        for k in sorted(stateDict):
            outMsg.append('%s: %s' % (k,stateDict[k]))
        outStr = ", ".join(outMsg)
        print "Total jobs %s, in flight %s - %s" % (len(fileList), len(inFlight), outStr)
        monitorUpdateTime = time.time()
    # Fill up enough slots in the queue to keep the workers busy
    for k,v in fileList.iteritems():
        if len(inFlight) > 3*concurrency:
            break
        if v['state'] in ['new','failed'] and v.get('retries',0) < maxRetries:
            fileList[k]['state'] = 'running'
            inFlight[k] = {'p':p.map_async(moveOneFile,[k],callback=getCallback)}
    if len(inFlight) == 0:
        # everything's done! get outta here
        break

    # get responses from commands
    while True:
        needSleep = True
        toDelete = []
        for k,v in inFlight.iteritems():
            if v['p'].ready():
                v['p'].get()
                toDelete.append(k)
                needSleep = False
        for k in toDelete:
            del inFlight[k]
        if needSleep:
            time.sleep(1)
        # if we need more commands, loop around
        if len(inFlight) < 2*concurrency or monitorUpdateTime + monitorDelta < time.time():
            break
import pprint
print "Jobs complete. Failed files are:"
for k,v in fileList.items():
    if v['state'] == 'failed':
        print k

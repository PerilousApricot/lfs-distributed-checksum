#!/usr/bin/env python2.6

import fcntl
import json
import os
import os.path
import socket
import subprocess
import sys
import time
import zmq

if len(sys.argv) != 2:
    print "Usage %s [number of threads]" % sys.argv[0]
    sys.exit(1)

coreCount = int(sys.argv[1])
taskList = {}
taskCount = 0
for x in range(coreCount):
    taskList[x] = None
hostname = socket.getfqdn()
# Make the ZMQ context
context = zmq.Context()

# Socket for clients
frontendSock = context.socket(zmq.REQ)
frontendSock.connect("tcp://brazil.vampire:5556")

# loop forever
while 1:
    needSleep = True
    if taskCount < coreCount:
        message = {'type' : 'needWork',
                   'host' : hostname }
        frontendSock.send_json(message)
        reply = frontendSock.recv_json()
        if reply['target']:
            taskCount += 1
            needSleep = False
            path = reply['target']['path']
            jobType = reply['target']['jobType']
            if jobType == 'checksum':
                if path.find('/lio/lfs/cms/store') == -1:
                    extractCommand = 'cat'
                else:
                    extractCommand = 'lio_get'
                processCommand = "%s %s | ./checksum.py %s" % \
                                    (extractCommand,path,path)
            elif jobType == 'filemove':
                processCommand = './bfsToLFS.sh %s 2>&1' % path
            else:
                taskCount -= 1
                print "Got unknown job type %s .. skipping" % jobType
                continue

            print "Doing checksum %s : %s" % (reply, processCommand)
            for taskSlot in taskList:
                if taskList[taskSlot] == None:
                    break
            assert taskList[taskSlot] == None
            p = subprocess.Popen(processCommand,
                                    shell=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            taskList[taskSlot] = {'process' : p,
                                  'output' : '',
                                  'error' : '',
                                  'path' : path,
                                  'jobType' : jobType}
            # hopefully this is the magic to set nonblocking I/O
            for x in (p.stdout, p.stderr):
                fd = x.fileno()
                fl = fcntl.fcntl(fd, fcntl.F_GETFL)
                fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        else:
            print "No work received"
    if taskCount >= 1:
        # poll the subprocesses
        for taskSlot in taskList:
            task = taskList[taskSlot]
            if task == None:
                continue
            # Is this the best way for non-blocking I/O?
            # .. fuck you python
            try:
                task['output'] += task['process'].stdout.read()
            except:
                pass
            try:
                task['error'] += task['process'].stderr.read()
            except:
                pass
            task['process'].poll()
            if task['process'].returncode != None:
                task['process'].wait()
                print "Task number %s ended" % taskSlot
                message = { 'type' : 'workDone',
                            'path' : task['path'],
                            'jobType' : task['jobType'],
                            'output' : task['output'],
                            'exitCode' : task['process'].returncode,
                            'error' : task['error']}
                frontendSock.send_json(message)
                reply = frontendSock.recv_json()
                taskCount -= 1
                taskList[taskSlot] = None
                needSleep = False
    if needSleep:
        print "No work this cycle, sleeping (%s jobs running)" % taskCount
        time.sleep(5)

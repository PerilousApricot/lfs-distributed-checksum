#!/usr/bin/env python

import fcntl
import json
import os
import os.path
import select
import signal
import socket
import subprocess
import sys
import time
import uuid
import zmq

if len(sys.argv) != 2:
    print "Usage %s [number of threads]" % sys.argv[0]
    sys.exit(1)

coreCount = int(sys.argv[1])
taskList = {}
taskCount = 0
for x in range(coreCount):
    taskList[x] = None
# Make the ZMQ context
context = zmq.Context()

# Socket for clients
frontendSock = context.socket(zmq.REQ)
frontendSock.connect("tcp://brazil.vampire:5556")


# Helper to send/recv messages in a nonblocking way
def trySend(sock, message):
    reply = None
    for _ in range(4):
        try:
            sock.send_json(message)
        except zmq.ZMQError:
            # try resetting the socket
            sock.close()
            sock.connect("tcp://brazil.vampire:5556")
        ready,_,_ = zmq.select([sock],[],[],10.0)
        if ready:
            return sock.recv_json()
        print "Awaiting response from server"
    return reply

# loop forever
checkinTime = 0
isDraining = False
scriptModificationTime = os.stat(__file__).st_mtime
workerID = "%s-%s" % (socket.getfqdn(),uuid.uuid4())
timeToDie = 0
# used to wait on child jobs to exit
poller = select.epoll()
while 1:
    needSleep = True
    if time.time() > checkinTime:
        message = {'type' : 'workerHeartbeat',
                   'id' : workerID,
                   'cores' : coreCount,
                   'activeJobs' : taskCount}
        trySend(frontendSock, message)
        checkinTime = time.time() + 10
        if os.stat(__file__).st_mtime != scriptModificationTime:
            coreCount = 0
            isDraining = True
            timeToDie = time.time() + 10 * 60
            print "Worker script was updated, entering drain mode"
            print "  Worker will die in %s secs" % (timeToDie - time.time())

    if timeToDie and time.time() > timeToDie:
        print "Can't wait forever to restart the server, dying now"
        sys.exit(0)

    if isDraining and taskCount == 0:
        print "All jobs successfully drained, exiting"
        sys.exit(0)

    if taskCount < coreCount and not isDraining:
        if not os.path.exists('/store/user') or not os.path.exists('/lio/lfs/cms/store'):
            print "Couldn't access filesystems, not pulling work"
            time.sleep(15)
            continue
        message = {'type' : 'needWork',
                   'id' : workerID }
        reply = trySend(frontendSock, message)
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
                # lio_get is ruining me
                # extractCommand = 'cat'
                processCommand = "%s %s | ./checksum.py %s" % \
                                    (extractCommand,path,path)
            elif jobType == 'filemove':
                processCommand = './bfsToLFS.sh %s 2>&1' % path
            else:
                taskCount -= 1
                print "Got unknown job type %s .. skipping" % jobType
                continue

            print "Doing %s: %s" % (jobType, processCommand)
            for taskSlot in taskList:
                if taskList[taskSlot] == None:
                    break
            assert taskList[taskSlot] == None
            p = subprocess.Popen(processCommand,
                                    shell=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            poller.register(p.stdout, select.EPOLLHUP)
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
                poller.unregister(task['process'].stdout)
                print "Task number %s ended" % taskSlot
                message = { 'type' : 'workDone',
                            'path' : task['path'],
                            'jobType' : task['jobType'],
                            'output' : task['output'],
                            'exitCode' : task['process'].returncode,
                            'error' : task['error'],
                            'worker' : workerID}
                trySend(frontendSock, message)
                taskCount -= 1
                taskList[taskSlot] = None
                needSleep = False
    if needSleep:
        sys.stdout.write("No work this cycle, sleeping (%s jobs running)" % taskCount)
        # Jobs may end more quickly than we anticipated
        #time.sleep(5)
        startTime = time.time()
        poller.poll(5)
        print "..done (%s sec)" % (time.time() - startTime)

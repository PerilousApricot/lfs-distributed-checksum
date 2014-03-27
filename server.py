#!/usr/bin/env python2.6

import json
import os
import os.path
import socket
import sys
import time
import zmq

statsdHost = ('brazil.vampire',8125)
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
def sendMessage(message):
    sock.sendto(message, statsdHost)

# some constants
timeoutLength = {'filemove':40*60,'checksum':5*60}
workerTimeout = 60
reserveForChecksums = 10
# Make the ZMQ context
context = zmq.Context()

# Socket for clients
frontendSock = context.socket(zmq.REP)
frontendSock.bind("tcp://*:5555")

# Socket for workers
backendSock = context.socket(zmq.REP)
backendSock.bind("tcp://*:5556")

state = {}
workers = {}

# loop forever
poller = zmq.Poller()
poller.register(frontendSock)
poller.register(backendSock)
heartbeatCheck = time.time() + 5.0
statusUpdate = time.time() + 1.0

def addFileJob(path,jobType,priority=0):
    stateKey = (path,jobType)
    temp = { 'requestedTime' : time.time(),
                     'path' : path,
                     'jobType' : jobType,
                     'priority' : priority}
    state.setdefault(stateKey,{})
    if not 'output' in state[stateKey]:
        state[stateKey]['acquired'] = False
    state[stateKey].update(temp)

def sortFunc(x,y):
    x = x[1]
    y = y[1]
    if x['priority'] != y['priority']:
        return y['priority'] - x['priority']
    else:
        return int(x['requestedTime'] - y['requestedTime'])


while 1:
    # what does an excepted ZMQ socket even mean?
    toRead,_,_ = zmq.select([frontendSock, backendSock], [], [], timeout = 5*1000)
    if time.time() > statusUpdate:
        acquired = complete = pending = 0
        jobSummary = {}
        for x in state:
            if not state[x]['jobType'] in jobSummary:
                jobSummary[state[x]['jobType']] = {'acquired':0,'complete':0,'pending':0}
            if state[x].get('acquired', False) and not 'output' in state[x]:
                acquired += 1
                jobSummary[state[x]['jobType']]['acquired'] += 1
            elif 'output' in state[x]:
                complete += 1
                jobSummary[state[x]['jobType']]['complete'] += 1
            else:
                pending += 1
                jobSummary[state[x]['jobType']]['pending'] += 1

        for oneType in jobSummary:
            for status in jobSummary[oneType]:
                sendMessage("lfs.dist_workers.%s.%s:%i|g" % (oneType,
                                                            status,
                                                            jobSummary[oneType][status]))
        activeCores = workerCores = 0
        for x in workers:
            activeCores += workers[x]['activeJobs']
            workerCores += workers[x]['cores']
        sendMessage("lfs.dist_workers.worker_cores:%i|g" % workerCores)
        sendMessage("lfs.dist_workers.active_cores:%i|g" % activeCores)

        print "All jobs %s, acquired jobs %s, unacquired jobs %s, cores active %s, cores total %s" %\
                                                                    (len(state),
                                                                    acquired,
                                                                    pending,
                                                                    activeCores,
                                                                    workerCores)
        while time.time() > statusUpdate:
            statusUpdate += 4
    if frontendSock in toRead:
        message = frontendSock.recv_json()
        if message['type'] == 'submit':
            addFileJob(message['path'],message['jobType'],message.get('priority',0))
            # if it's been submitted again, assume we need to reove the output
            stateKey = (message['path'],message['jobType'])
            for k in ['output', 'doneTime', 'acquired']:
                if k in state[stateKey]:
                    del state[stateKey][k]
            reply = {'type' : 'confirmed'}
            print "Got request to %s %s" % (message['jobType'],
                                            message['path'])
            frontendSock.send_json(reply)
        elif message['type'] == 'query':
            message.setdefault('jobType','checksum')
            stateKey = (message['path'],message['jobType'])
            if stateKey not in state:
                addFileJob(message['path'],message['jobType'],message.get('priority',0))
            reply = {'status' : 'pending'}
            if 'output' in state[stateKey]:
                reply['status'] = 'done'
                for k in ('output','error','exitCode'):
                    reply[k] = state[stateKey][k]
            frontendSock.send_json(reply)

    if backendSock in toRead:
        message = backendSock.recv_json()
        if message['type'] == 'needWork':
            reply = {'target' : None}
            # make sure we don't fill the queue with non-checksum jobs
            allCores = currentlyRunningTasks = 0
            for w in workers:
                allCores += workers[w]['cores']
            for k in state:
                if state[k].get('acquired', False) and not 'output' in state[k]:
                    currentlyRunningTasks += 1
            checksumOnly = (allCores - currentlyRunningTasks < reserveForChecksums)
            otherTasks = False
            for k,_ in sorted(state.iteritems(), cmp=sortFunc):
                if state[k]['jobType'] != 'checksum':
                    otherTasks = True
                if state[k].get('acquired', False) == False and not\
                        (checksumOnly and state[k]['jobType'] != 'checksum'):
                    reply = {'target' : state[k]}
                    state[k]['acquired'] = True
                    state[k]['acquireTime'] = time.time()
                    break
            if checksumOnly and reply['target'] == None and otherTasks:
                print "Stalling all non-checksum jobs to save empty slots (%s - %s) < %s" %\
                            (allCores, currentlyRunningTasks, reserveForChecksums)
            backendSock.send_json(reply)
        elif message['type'] == 'workDone':
            stateKey = (message['path'],message['jobType'])
            reply = {'message' : 'ok'}
            state.setdefault(stateKey, {'priority':0,'requestedTime':0,\
                                        'jobType':message['jobType'],
                                        'path':message['path']})
            for k in ('output','error','exitCode'):
                state[stateKey][k] = message[k]
            state[stateKey]['worker'] = message.get('worker','UNKNOWN')
            state[stateKey]['doneTime'] = time.time()
            backendSock.send_json(reply)
            print "Worker completed %s of %s" % (message['jobType'],message['path'])
        elif message['type'] == 'workerHeartbeat':
            workers.setdefault(message['id'], {'coreCount':0,'activeTasks':0,'lastHeartbeat':0})
            workers[message['id']]['lastHeartbeat'] = time.time()
            for k in ('cores','activeJobs'):
                workers[message['id']][k] = message[k]
            backendSock.send_json('ok')

    if heartbeatCheck - time.time() < 0:
        # clean things up
        print "Beginning cleanup cycle"
        toDelete = []
        done = unacquired = deleted = 0
        for k in state:
            assert 'jobType' in state[k]
            jobTimeout = timeoutLength[state[k]['jobType']]
            if 'acquireTime' in state[k] and \
                    time.time() - state[k]['acquireTime'] > jobTimeout and \
                    state[k].get('acquired',False) == True and \
                    not 'output' in state[k]:
                sendMessage("lfs.dist_workers.server_errors.task_timeout:1|c")
                state[k]['acquired'] = False
                unacquired += 1
            elif state[k].get('acquired', False) == False:
                unacquired += 1
            if 'doneTime' in state[k] and \
                    time.time() - state[k]['doneTime'] > 3600:
                toDelete.append(k)
                deleted += 1
        for k in toDelete:
            del state[k]

        toDelete = []
        for w in workers:
            if time.time() - workers[w]['lastHeartbeat'] > workerTimeout:
                for k in state:
                    if state[k].get('worker', False) == w:
                        # worker went away, reassign the task
                        state[k]['acquired'] = False
                        sendMessage("lfs.dist_workers.server_errors.task_timeout:1|c")
                        del state[k]['worker']
                        unacquired += 1
                toDelete.append(w)
                if workers[w].get('cores', 0) != 0:
                    # don't complain if a worker drained out on purpose
                    sendMessage("lfs.dist_workers.server_errors.worker_timeout:1|c")
        for w in toDelete:
            del workers[w]
        print "Removed %s tasks, unacquired %s tasks, %s tasks remaining"%\
                (deleted, unacquired, len(state.keys()))
        while heartbeatCheck - time.time() < 0:
            heartbeatCheck += 60

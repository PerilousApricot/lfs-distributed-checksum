#!/usr/bin/env python2.6

import json
import os
import os.path
import sys
import time
import zmq

# some constants
timeoutLength = {'filemove':40*60,'checksum':2*60}

# Make the ZMQ context
context = zmq.Context()

# Socket for clients
frontendSock = context.socket(zmq.REP)
frontendSock.bind("tcp://*:5555")

# Socket for workers
backendSock = context.socket(zmq.REP)
backendSock.bind("tcp://*:5556")

state = {}
# Try and persist data across restarts
if os.path.exists("state.json"):
    state = json.loads(open("state.json", "r").read())

# loop forever
poller = zmq.Poller()
poller.register(frontendSock)
poller.register(backendSock)
heartbeatCheck = time.time() + 5.0
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
    acquired = 0
    for x in state:
        if state[x].get('acquired', False):
            acquired += 1
    print "All jobs %s, acquired jobs %s, unacquired jobs %s" % (len(state),
                                                                 acquired,
                                                                 len(state) - acquired)
    if frontendSock in toRead:
        message = frontendSock.recv_json()
        if message['type'] == 'submit':
            addFileJob(message['path'],message['jobType'],message.get('priority',0))
            # if it's been submitted again, assume we need to reove the output
            stateKey = (message['path'],message['jobType'])
            for k in ['output', 'doneTime']:
                if k in state[stateKey]:
                    del state[stateKey][k]
            reply = {'type' : 'confirmed'}
            print "Got request to checksum %s" % message['path']
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
            print "Got status request for %s - %s" % (message['path'],state[stateKey])
            frontendSock.send_json(reply)

    if backendSock in toRead:
        message = backendSock.recv_json()
        if message['type'] == 'needWork':
            reply = {'target' : None}
            for k,_ in sorted(state.iteritems(), cmp=sortFunc):
                if state[k].get('acquired', False) == False:
                    reply = {'target' : state[k]}
                    state[k]['acquired'] = True
                    state[k]['acquireTime'] = time.time()
                    break
            backendSock.send_json(reply)
            print "Worker requested work"
        if message['type'] == 'workDone':
            stateKey = (message['path'],message['jobType'])
            reply = {'message' : 'ok'}
            state.setdefault(stateKey, {'priority':0,'requestedTime':0,\
                                        'jobType':message['jobType'],
                                        'path':message['path']})
            for k in ('output','error','exitCode'):
                state[stateKey][k] = message[k]
            state[stateKey]['doneTime'] = time.time()
            backendSock.send_json(reply)
            print "Worker completed %s of %s" % (message['jobType'],message['path'])
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
                    not 'output' in state[k]:
                state[k]['acquired'] = False
                unacquired += 1
            if 'doneTime' in state[k] and \
                    time.time() - state[k]['doneTime'] > 3600:
                toDelete.append(k)
                deleted += 1
            if state[k].get('acquired', False) == False:
                unacquired += 1
        for k in toDelete:
            del state[k]
        print "Removed %s tasks, unacquired %s tasks, %s tasks remaining"%\
                (deleted, unacquired, len(state.keys()))
        heartbeatCheck += 60

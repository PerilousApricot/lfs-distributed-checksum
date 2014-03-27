#!/usr/bin/env python2.6

import json
import os
import os.path
import sys
import time
import zmq

# Make the ZMQ context
context = zmq.Context()

# Socket for clients
frontendSock = context.socket(zmq.REQ)
frontendSock.connect("tcp://brazil.vampire:5555")
def trySend(sock, message):
    reply = None
    for _ in range(3):
        sock.send_json(message)
        ready = zmq.select([sock],[],[],10.0)
        if ready:
            return sock.recv_json()
        print "Awaiting response from server"
    return reply


path = sys.argv[1]
message = {'type' : 'submit',
           'jobType' : 'checksum',
           'priority' : 10,
           'path' : path}
reply = trySend(frontendSock, message)
if reply == None or reply['type'] != 'confirmed':
    raise RuntimeError, "Couldn't submit query to checksum server"
# loop forever
while 1:
    message = {'type' : 'query',
               'jobType' : 'checksum',
               'priority' : 10,
               'path' : path }
    reply = trySend(frontendSock, message)
    if reply == None:
        continue
    if reply['status'] == 'done':
        print reply['output']
        sys.stderr.write("accreChecksumWorker=%s\n" % \
                         reply.get('worker','UNKNOWN'))
        sys.exit(0)
    elif reply['status'] == 'unknown':
        print "Couldn't get status of checksum"
        sys.exit(1)
    time.sleep(5)

#!/usr/bin/env python2.6

import json
import os
import os.path
import socket
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
           'jobType' : 'filemove',
           'path' : path}
reply = trySend(frontendSock, message)
if reply == None or reply['type'] != 'confirmed':
    raise RuntimeError, "Couldn't submit query to checksum server"
# loop forever
while 1:
    message = {'type' : 'query',
               'jobType' : 'filemove',
               'path' : path }
    reply = trySend(frontendSock, message)
    if reply == None:
        continue
    print reply
    if reply['status'] == 'done':
        print reply['output']
        sys.exit(reply['exitCode'])
    elif reply['status'] == 'unknown':
        print "Couldn't get status of filemove"
        sys.exit(120)
    time.sleep(5)

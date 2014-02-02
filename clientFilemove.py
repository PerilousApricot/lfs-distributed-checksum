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

path = sys.argv[1]
message = {'type' : 'submit',
           'jobType' : 'filemove',
           'path' : path}
frontendSock.send_json(message)
reply = frontendSock.recv_json()
if reply['type'] != 'confirmed':
    raise RuntimeError, "Couldn't submit query to checksum server"
# loop forever
while 1:
    message = {'type' : 'query',
               'jobType' : 'filemove',
               'path' : path }
    frontendSock.send_json(message)
    reply = frontendSock.recv_json()
    print reply
    if reply['status'] == 'done':
        print reply['output']
        sys.exit(reply['exitCode'])
    elif reply['status'] == 'unknown':
        print "Couldn't get status of filemove"
        sys.exit(120)
    time.sleep(5)

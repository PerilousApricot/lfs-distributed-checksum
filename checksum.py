#!/usr/bin/env python2.6
"""
    checksummer.py - reads from stdin and returns checksums
"""

import hashlib
import subprocess
import sys
import time
import zlib

targetFile = None
if len(sys.argv) == 2:
    targetFile = sys.argv[1]

def trimBytes(val):
    prefix = ('B','KB','MB','GB','TB')
    inc = 1
    origVal = val
    for scale in prefix:
        if val < 1024:
            return "%1.2f%s" % (val, scale)
        val = val / 1024.0
    return "%sB" % origVal

adler32 = zlib.adler32("")
cksum = subprocess.Popen("cksum", stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE)
md5 = hashlib.md5()
sha1 = hashlib.sha1()

blockSize = 1*1024*1024
byteCount = 0
startTime = time.time()
timeCount = 1
statusIncrement = 3
lastBytes = 0
lastTime = time.time()
while 1:
    # read 10 megs at a time
    inputBytes = sys.stdin.read(blockSize)
    if inputBytes == "":
        break
    byteCount += len(inputBytes)
    lastBytes += len(inputBytes)
    adler32 = zlib.adler32(inputBytes,adler32)
    cksum.stdin.write(inputBytes)
    md5.update(inputBytes)
    sha1.update(inputBytes)
    if time.time() - (startTime + timeCount * statusIncrement) > 0:
        currentTime = time.time()
        while currentTime - (startTime + timeCount * statusIncrement) > 0:
            timeCount += 1
        instTime = currentTime - lastTime
        totalTime = currentTime - startTime
        msg = "Inst: %s, %1.2fsec, %s/sec, "
        msg += "Total: %s, %1.2fsec, %s/sec\n"
        sys.stderr.write(msg % (trimBytes(lastBytes), instTime,\
                     trimBytes(lastBytes/instTime),\
                     trimBytes(byteCount), totalTime,\
                     trimBytes(byteCount/totalTime)))
        lastTime = currentTime
        lastBytes = 0

if adler32 < 0:
    adler32 += 2**32
cksumOutput, _ = cksum.communicate()
cksumOutput = cksumOutput.split()[0]
print "md5 %s" % md5.hexdigest()
print "sha1 %s" % sha1.hexdigest()
print "cksum %s" % cksumOutput
adler32Formatted = hex(adler32)[2:10].zfill(8).lower()
print "adler32 %s" % adler32Formatted
print "bytes %s" % byteCount
# construct attribute-setting command line
if targetFile:
    sys.stderr.write('Adding attributes to file %s\n' % targetFile)
    args = ['lio_setattr', '-as', 'user.accre_md5=%s' % md5.hexdigest(),
                        '-as', 'user.accre_sha1=%s' % sha1.hexdigest(),
                        '-as', 'user.accre_cksum=%s' % cksumOutput,
                        '-as', 'user.accre_adler32=%s' % adler32Formatted,
                        '-as', 'user.accre_cksumtime=%s' % time.time(),
                        targetFile]
    print "Executing: %s" % " ".join(args)
    p = subprocess.Popen(args, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
    out, _ = p.communicate()
    if p.returncode:
        sys.stderr.write("Got an error (%s) setting attributes\n" % p.returncode)
        sys.stderr.write("Output:\n%s\n" % out)
        sys.exit(1)
sys.exit(0)

#!/usr/bin/env python


def print_usage():
    usage = 'Usage: FileDownloadVerify [-c] [-d] [-f] STATUS PFN SIZE CHECKSUM'
    print usage

import random
import sys,os,time,re
import subprocess
import tempfile
#sys.exit(1)
import socket
import time
import commands
import traceback

statsdHost = ('brazil.vampire',8125)
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
def sendMessage(message):
    print "transmitting to statsd: %s" % message
    sock.sendto(message, statsdHost)

fh = open('/tmp/globallog.log','a')
print "DOING LFS VERISON OF FILE VERIFY"
mismatchListFH = open('/tmp/mismatchedChecksums-lfs.log','a')
def handleChecksumMismatch(lstorelfn,sumType,desiredSum,actualSum):
    newname = "/lio/lfs/cms/checksums/mismatch/" + lstorelfn.replace('/','___') + "----%s" % time.time()
    msg = "checksum on %s does not match (%s: desired=%s,actual=%s) moving file to %s for investigation" % (lstorelfn,sumType,desiredSum,actualSum,newname)
    print msg
    mismatchListFH.write( msg + "\n" )
    cmdline = "lio_mv %s %s" % (lstorelfn,newname)
    print "commandline is %s" % cmdline
    cmdoutput = commands.getoutput( cmdline )
    print cmdoutput

def doLog(input):
    sys.stdout.write("%s-%s\n" % (time.time(), input))
    fh.write("%s-%s\n" %(time.time(), input))

def checksumsMatch(sourceHash, destHash):
    hashFound = False
    # not doing cksum
    for hashType in ('md5','sha1','adler32','cksum'):
        if not hashType in sourceHash or not hashType in destHash:
            continue
        source = sourceHash[hashType]
        dest = destHash[hashType]
        if (source != dest):
            # somehow, casting to an int helps ?!?
            print "Checksum failure: %s != %s (%s)" % (source, dest, hashType)
            print "Checksum failure: %s != %s (%s)" % (int(source,16), int(dest,16), hashType)
            if (int(source,16) != int(dest,16)):
                return False
    return True

# stupid hack
def fileVerifyLFS( status, pfn, phedex_size, checksum, do_checksum=False, do_delete=False):
    isLFS = False
    if status == "pre":
        print "Preverification! Go ahead and retransfer."
        return 1
    sendMessage("lfs.fdv.started:1|c")
    sshPrefix = "ssh meloam@brazil.accre.vanderbilt.edu -oControlMaster=no -oProxyCommand=none -oControlPath=none"
    status = status.split()[-1]
    print "FileDownloadVerify STATUS = %s" % status
    print "FileDownloadVerify PFN = >%s<" % pfn
    print "FileDownloadVerify PHEDEX_SIZE = %s" % phedex_size
    print "FileDownloadVerify CHECKSUM = %s" % checksum
    phedexChecksum = {}
    for oneChecksum in checksum.split(','):
        phedexChecksum[ oneChecksum.split(':')[0] ] = oneChecksum.split(':')[1]
        print "FileDownloadVerify CHECKSUMHASH = %s" % phedexChecksum
    position = pfn.find('SFN=/lio/lfs/cms/store')
    assert position != -1, "Needs to be an LFS path: %s" % pfn
    myPfn = pfn[position + len('SFN='):]
    lfsName = myPfn[len('/lio/lfs'):]
    cmdLine = "%s time stat %s" % (sshPrefix, myPfn)
    p = subprocess.Popen(cmdLine,stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True, shell=True)
    (retval, reterr) = (p.stdout, p.stderr)
    print "Executing stat.."
    oneline = retval.readline()
    twoline = retval.readline()
    splitStr = twoline.split()
    if len(splitStr) < 2:
        print "File didn't make it to SE"
        sendMessage("lfs.fdv.errors.missing_file:1|c")
        return 2

    # store checksums
    checksumStringList = []
    for (k,v) in phedexChecksum.items():
        checksumStringList.append("-as user.phedex.%s=%s %s" % (k, v, lfsName))
    cmdLine = "%s lio_setattr %s 2>&1" % (sshPrefix, " ".join(checksumStringList))
    print "Storing phedex checksums in files: %s" % cmdLine
    (status, output) = commands.getstatusoutput(cmdLine)
    if (status != 0):
        print "Couldn't add checksum to file:"
        print output
        sendMessage("lfs.fdv.errors.setattr_failed:1|c")
        return 3

    # Handle checksums stored by gridftp
    cmdLine = "%s lio_getattr -ga 'user.gridftp.*' %s" % (sshPrefix, lfsName)
    gridftpChecksum = {}
    print "Getting gridftp checksums: %s" % cmdLine
    (status, output) = commands.getstatusoutput(cmdLine)
    if (status != 0):
        print "Couldn't retrieve checksums from file:"
        print output
        sendMessage("lfs.fdv.errors.getattr_failed:1|c")
        return 3

    for line in output.split('\n'):
        if not 'user.gridftp' in line:
            continue
        k,v = line.split('=')
        _,algorithm = k.split('user.gridftp.')
        if algorithm == 'cksum':
            # the cksum values are currently bunk from gridftp
            continue
        gridftpChecksum[algorithm] = v

    print "Gridftp checksums: %s" % gridftpChecksum
    if gridftpChecksum.get('success',False) != 'okay':
        sendMessage("lfs.fdv.errors.no_okay_flag:1|c")
        return 5

    gridftp_size = splitStr[1]
    print "gridftp_size is (?) %s" % gridftp_size
    if (int(phedex_size) != int(gridftp_size)):
        print "Wrong file size on GridFTP"
        print "Phedex_size is (%s), GridFTP size is (%s)" % (int(phedex_size), int(gridftp_size))
        sendMessage("lfs.fdv.errors.bad_size:1|c")
        return 2

    if not checksumsMatch(phedexChecksum, gridftpChecksum):
        sendMessage("lfs.fdv.errors.gridftp_checksum_failed:1|c")
        return 4
        pass

    # Do we want to compute our own checksums?
    doChecksums = True
    debugChecksumPercentage = 1.0
    if lfsName.startswith('/cms/store/PhEDEx'):
        # Debug stuff
        ourChecksum = random.random()
        if debugChecksumPercentage < ourChecksum:
            doChecksums = False
    if doChecksums:
        checksumSucceeded = False
        retryCount = 1
        checksumFailureReason = ""
        while not checksumSucceeded and retryCount < 3:
            retryCount += 1
            print "Getting checksums, calculating if necessary, this may take a while\n"
            sendMessage("lfs.checksum.start:1|c")
            checksumStartTime = time.time()
            cmdline = "python2.6 /nfs_mounts/home/meloam/checksummer/clientChecksum.py %s" % myPfn
            print "commandline is %s" % cmdline
            (status,output) = commands.getstatusoutput( cmdline )
            checksumSucceeded = True
            sendMessage("lfs.fdv.checksum_delay:%i|ms" % ((time.time()-checksumStartTime)*1000.0))
            if(status != 0):
                sendMessage("lfs.fdv.errors.checksummer_died:1|c")
            accreChecksumHash = {}
            for aChecksum in output.split('\n'):
                if len(aChecksum.split(' ')) == 2:
                    accreChecksumHash[ aChecksum.split(' ')[0] ] = aChecksum.split(' ')[1]
            if int(accreChecksumHash['bytes']) != int(phedex_size):
                print "Checksummer got the wrong byte size: %s" % accreChecksumHash['bytes']
                checksumFailureReason = "checksummer_bytes_wrong"
                checksumSucceeded = False
                continue
            print "FileDownloadVerify ACCRE CHECKSUMHASH = %s" % accreChecksumHash
            if not checksumsMatch(phedexChecksum, accreChecksumHash):
                gridftp_size = -3
                checksumFailureReason = "checksum_failed"
                checksumSucceeded = False
                continue
            # if we got here, then the checksum as okay
            break
        if not checksumsMatch(accreChecksumHash, gridftpChecksum):
            print "GRIDFTP AND ACCRE DON'T LINE UP %s %s" % (accreChecksumHash, gridftpChecksum)
        if checksumSucceeded == False:
            sendMessage("lfs.fdv.errors.%s:1|c" % checksumFailureReason)
            return 3
    return 0

def delete( pfn ):
    print "Attempting Delete"
    cmd  = '/home/moraj/srmclient2/bin/srm-rm '
    cmd2 = ' -delegation'
    keepGoing = True
    retryCount = 5

    while keepGoing:
        (tempHandle, _) = tempfile.mkstemp(dir="/home/phedex/SITECONF/T3_US_Vanderbilt/PhEDEx/deleteQueue/")
        tempFileObject = os.fdopen(tempHandle, 'w')
        tempFileObject.write(pfn)
        tempFileObject.close()
        keepGoing = False
        return 0
    try:

            #subProcess = subprocess.Popen(['/home/phedex/SITECONF/T3_US_Vanderbilt/PhEDEx/srm-bypass-rm.py',
            #				'dummy',
            #				pfn],
            subProcess = subprocess.Popen( ['/home/phedex/SITECONF/T3_US_Vanderbilt/PhEDEx/FileDownloadDeleteLCG.sh',
                                            pfn,
                                            pfn],
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.STDOUT)
    except OSError, e:
        print "Could not fork subprocess in FileDownloadVerify (delete part): %s " % e
        sys.exit(1)

        # get the filesize
        buffer = ""
        for line in subProcess.stdout.readlines():
            buffer += line
            # check the return code
            subProcess.communicate()
            return subProcess.returncode

args = sys.argv[1:]
do_checksum, do_delete, do_success = False, False, False
if '-c' in args:
    do_checksum = True
    args.remove('-c')
if '-d' in args:
    do_delete = True
    args.remove('-d')
if '-f' in args:
    do_success = True
    args.remove('-f')
try:
    print "args are %s" % " ".join(args)
    status, pfn, phedex_size, checksum = args[:4]
except:
    print_usage()
    sys.exit(11)

exit_code = -1
verifyStartTime = time.time()
try:
    exit_code = fileVerifyLFS( status, pfn, phedex_size, checksum, do_checksum, do_delete )
except Exception,e:
    sendMessage("lfs.fdv.errors.raised_exception:1|c")
    print "got exception %s" % e
    traceback.print_exc()
fdvTime = ((time.time()-verifyStartTime)*1000.0)
sendMessage("lfs.fdv.verify_time:%i|ms" % fdvTime)
if (exit_code == 0):
    sendMessage("lfs.fdv.succeeded:1|c")
    if (do_delete):
        delete( pfn )
        if (do_success):
            exit_code = 0
            print "FileDownloadVerify exit code (%s)" % exit_code
else:
    sendMessage("lfs.fdv.failed:1|c")
fh.close()
sys.exit( exit_code )


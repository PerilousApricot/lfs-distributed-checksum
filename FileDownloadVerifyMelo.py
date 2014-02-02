#!/usr/bin/env python


def print_usage():
    usage = 'Usage: FileDownloadVerify [-c] [-d] [-f] STATUS PFN SIZE CHECKSUM'
    print usage


import sys,os,time,re
import subprocess
import tempfile
#sys.exit(1)
import time
import commands

fh = open('/tmp/globallog.log','a')
print "DOING LFS VERISON OF FILE VERIFY"
mismatchListFH = open('/tmp/mismatchedChecksums-lfs.log','a')
def handleChecksumMismatch(lstorelfn,sumType,desiredSum,actualSum):
    newname = "/lio/lfs/cms/checksums/mismatch/" + lstorelfn.replace('/','___') + "----%s" % time.time()
    msg = "checksum on %s does not match (%s: desired=%s,actual=%s) moving file to %s for investigation" % (lstorelfn,sumType,desiredSum,actualSum,newname)
    print msg
    mismatchListFH.write( msg + "\n" )
    #cmdline = "lio_mv %s %s" % (lstorelfn,newname)
    #print "commandline is %s" % cmdline
    #cmdoutput = commands.getoutput( cmdline )
    #print cmdoutput

def doLog(input):
    sys.stdout.write("%s-%s\n" % (time.time(), input))
    fh.write("%s-%s\n" %(time.time(), input))

def fileVerifyLFS( status, pfn, phedex_size, checksum, do_checksum=False, do_delete=False):
    isLFS = False
    if status == "pre":
        print "Preverification! Go ahead and retransfer."
        return 1
    status = status.split()[-1]
    print "FileDownloadVerify STATUS = ", status
    print "FileDownloadVerify PFN = >%s<" % pfn
    print "FileDownloadVerify PHEDEX_SIZE = ", phedex_size
    print "FileDownloadVerify CHECKSUM = ", checksum
    checksumHash = {}
    for oneChecksum in checksum.split(','):
        checksumHash[ oneChecksum.split(':')[0] ] = oneChecksum.split(':')[1]
        print "FileDownloadVerify CHECKSUMHASH = %s" % checksumHash

    print "Phedex_size is (%s)" % phedex_size
    #						pfn = pfn.replace('SFN=/store/','SFN=/store/test/')
    # 5 drops the leading slash, 4 doesn't
    # we are at lio
    position = pfn.find('SFN=/lio/lfs/cms/store')
    assert position != -1, "Needs to be an LFS path: %s" % pfn
    myPfn = pfn[position + len('SFN='):]
    print "mangled myPfn to %s" % myPfn
    cmdLine = "ssh meloam@brazil.accre.vanderbilt.edu -oControlMaster=no -oProxyCommand=none -oControlPath=none time stat %s" % myPfn
    print "command line is %s" % cmdLine;
    p = subprocess.Popen(cmdLine,stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True, shell=True)
    (retval, reterr) = (p.stdout, p.stderr)
    print "Executing stat.."
    oneline = retval.readline()
    print oneline
    twoline = retval.readline()
    print twoline
    print retval.readlines()
    print "STDERR from stat:"
    print reterr.readlines()
    print "END STDERR"
    gridftp_size = twoline.split()[1]
    print "gridftp_size is (?) %s" % gridftp_size
    if (int(phedex_size) != int(gridftp_size)):
        print "Wrong file size on GridFTP"
        print "Phedex_size is (%s), GridFTP size is (%s)" % (int(phedex_size), int(gridftp_size))
        return 2
    # store checksums
    for (k,v) in checksumHash.items():
        cmdline = "lio_setattr -as phedex_%s:%s %s" %\
                ( k, v, myPfn)
        #print "Adding checksum: %s" % cmdline
        #output = commands.getoutput( cmdline )
        #print output

    doChecksums = True
    if doChecksums:
        print "Getting checksums, calculating if necessary, this may take a while\n"
        doLog("Getting checksums,calculating if necessary, this may take a while\n")
        cmdline = "python2.6 /nfs_mounts/home/meloam/checksummer/clientChecksum.py %s" % myPfn
        #fmy.write("%s  %s" %(time.asctime(), "%s\n" %cmdline))
        print "commandline is %s" % cmdline
        (status,output) = commands.getstatusoutput( cmdline )
        if(False and status != 0):
            raise Exception, "Error getting accre checksums, Output: %s" % output
        print output
        accreChecksumHash = {}
        for aChecksum in output.split('\n'):
            if len(aChecksum.split(' ')) == 2:
                accreChecksumHash[ aChecksum.split(' ')[0] ] = aChecksum.split(' ')[1]

        print "FileDownloadVerify ACCRE CHECKSUMHASH = %s" % accreChecksumHash
        hashFound = False
        for hashType in ('md5','sha1','adler32','cksum'):
            if not hashType in checksumHash or not hashType in accreChecksumHash:
                continue
            rawAccreHash = accreChecksumHash[hashType]
            rawPhedexHash = checksumHash[hashType]
            print "raw hash, accre %s, phedex %s" % (rawAccreHash, rawPhedexHash)
            accreHash = int(accreChecksumHash[hashType],16)
            phedexHash = int(checksumHash[hashType],16)
            print "%s equality test: phedex='%s', accre='%s'" % \
                            (hashType, phedexHash, accreHash)
            if hashType == 'cksum':
                print "Skipping cksum test, it doesn't seem to work"
                continue
            if hashType == 'adler32':
                print "Skipping adler32 test, it doens't seem to work"
                continue
            if (rawAccreHash == rawPhedexHash):
                print "%s checksums match (%s)\n" % (hashType, myPfn)
            else:
                handleChecksumMismatch(myPfn,hashType,accreHash,phedexHash)
                gridftp_size = -3
                return 3

    # blew away the try-except block, if it fails, we need a stacktrace -meloam
    if (int(phedex_size) != int(gridftp_size)):
        print "Wrong file size on GridFTP"
        print "Phedex_size is (%s), GridFTP size is (%s)" % (int(phedex_size), int(gridftp_size))
        return 2

    return 0

def delete( pfn ):
    print "Attempting Delete"
    doLog("VERIFYDELETE " + pfn + "\n")
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
    print "args are %s" % args
    status, pfn, phedex_size, checksum = args[:4]
except:
    print_usage()
    sys.exit(11)

doLog('VERIFY ' + pfn + '\n')
exit_code = fileVerifyLFS( status, pfn, phedex_size, checksum, do_checksum, do_delete )
if (exit_code == 0):
    doLog('VERIFYWIN ' + pfn + '\n')
    if (do_delete):
        delete( pfn )
        if (do_success):
            exit_code = 0
            print "FileDownloadVerify exit code (%s)" % exit_code

fh.close()
sys.exit( exit_code )


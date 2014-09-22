# Copyright 2011-2013 S J Consulting Ltd. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://www.apache.org/licenses/LICENSE-2.0.html
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

import argparse
#from opcode import op
import boto
import collections
import pprint
import os
import time
#import datetime
#import dateutil.parser
import hashlib

THIS_VERSION = "1.1.2"
RULER = "--------------------------------------------------------------------------------"

log = []

FileTuple = collections.namedtuple('filetuple', 'etag size')

def calculatemd5(files, path):
    if not path in files:
        return

    if files[path].etag:
        return files[path].etag

    file = files['__prefix__']  + path

    if not os.path.isfile(file):
            return;

    return  hashlib.md5(open(file,'rb').read()).hexdigest()

def cleanpath(path):
    if not path.endswith('/'):
        return path + '/'
    return path

def file_only_log_event(text):
    global log
    clock = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    log.append("%s %s" % (clock, text))

def console_only_log_event(text):
    print (text)

def log_event(text):
    file_only_log_event(text)
    console_only_log_event(text)

def commit_log(logfilename, position = None):
    if logfilename == '': return
    if position == 'top':
        if os.path.exists(logfilename):
            tempfilename = os.path.dirname(logfilename) + os.pathsep +  "log.tmp"

            tempfile = open(tempfilename, 'w')

            for text in log:
                tempfile.write(text + os.linesep)

            logfile = open(logfilename, 'r')
            for text in logfile:
                tempfile.write(text)

            logfile.close()
            tempfile.close()

            os.remove(logfilename)
            os.rename(tempfilename, logfilename)
            return

    logfile = open(logfilename, 'a')
    for text in log:
        logfile.write(text + os.linesep)

    logfile.close()

def cloud_with_prefix(bucket, cloud = None):
    if cloud == None:
        return

    for obj in bucket.list(cloud['__prefix__']):
	try:
            key = str(obj.name)[len(cloud['__prefix__']):]
        except:
            pass
        else:
            if not key.endswith('/'):
                data = bucket.get_key(obj.name)
                cloud[key] = FileTuple(size=data.size, etag=data.etag[1:-1])

def local_with_prefix(local = None, folder = ''):
    if local == None:
        return

    _folder = cleanpath(folder)
    path = cleanpath(local['__prefix__'] + folder)

    if not os.path.isdir(path):
        return

    for obj in os.listdir(path):
        if not obj.startswith('_') and obj != '.DS_Store' and obj != 'Thumbs.db':
            _file = path + obj

            if os.path.isdir(_file):
                local_with_prefix(local, _folder + obj)
            else:
                if os.path.isfile(_file):
                    local[_folder + obj] = FileTuple(size=os.path.getsize(_file), etag='')

def generate_actions(source, destination, direction, actions = None, maxactions = 0, md5 = False, delete = False):
    if source == None:
        return

    if destination == None:
        return

    if actions == None:
        actions = []

    ActionTuple = collections.namedtuple('actiontuple','operation object param reason')

    for key, obj in source.iteritems():
        if key != '__prefix__':
            if key in destination:
                if obj.size != destination[key].size:
                    actions.append(ActionTuple(operation=direction, object=key, param=None, reason='different size'))
                else:
                    if md5:
                        if calculatemd5(source, key) != calculatemd5(destination, key):
                            actions.append(ActionTuple(operation=direction, object=key, param=None, reason='different md5'))

                del(destination[key])
            else:
                actions.append(ActionTuple(operation=direction, object=key, param=None, reason='missing'))

            if len(actions) >= maxactions:
                return

    if delete:
        for key, obj in destination.iteritems():
            if key != '__prefix__':
                if direction == 'upload':
                    actions.append(ActionTuple(operation='deletecloud', object=key, param=None, reason='no local file'))
                else:
                    actions.append(ActionTuple(operation='deletelocal', object=key, param=None, reason='no cloud file'))

                if len(actions) >= maxactions:
                    return

def perform_actions(bucket, actions, localpath, cloudpath, errors = None, dryrun = False, maxretries = 3):
        if bucket == None or actions == None or localpath == '' or cloudpath == '':
            return

        global metrics

        if errors == None:
            errors = {}

        for action in actions:
            cloud = cloudpath  + action.object
            local = localpath + action.object
            localparent = os.path.dirname(local)

            reason = " (%s) " % action.reason

            retries = 0

            if action.operation == 'upload':
                operation = action.operation
                while retries < maxretries:
                    try:
                        log_event(operation + reason + local)
                        if not dryrun:
                            obj = boto.s3.key.Key(bucket)

                            obj.key = cloud
                            obj.set_contents_from_filename(local)

                        break
                    except:
                        operation = 're-' + action.operation
                        retries = retries + 1

                if retries < maxretries:
                    metrics['uploads'] = metrics['uploads'] + 1
                else:
                    metrics['errors'] = metrics['errors'] + 1

            elif action.operation == 'download':
                operation = action.operation
                if os.path.exists(local):
                    os.remove(local)
                else:
                    if not os.path.isdir(localparent):
                        os.makedirs(localparent)

                while retries < maxretries:
                    try:
                        log_event(operation + reason + local)
                        if not dryrun:
                            obj = boto.s3.key.Key(bucket)

                            obj.key = cloud
                            obj.get_contents_to_filename(local)
                        break
                    except:
                        operation = 're-' + action.operation
                        retries = retries + 1

                if retries < maxretries:
                    metrics['downloads'] = metrics['downloads'] + 1
                else:
                    metrics['errors'] = metrics['errors'] + 1

            elif action.operation == 'deletecloud':
                try:
                    log_event(action.operation + reason + cloud)
                    if not dryrun:
                        obj = boto.s3.key.Key(bucket)
                        obj.key = cloud
                        bucket.delete_key(obj)

                    metrics['deletes'] = metrics['deletes'] + 1
                except:
                    metrics['errors'] = metrics['errors'] + 1

            elif action.operation == 'deletelocal':
                try:
                    log_event(action.operation + reason + local)
                    if not dryrun:
                        os.remove(local)
                    metrics['deletes'] = metrics['deletes'] + 1
                except:
                    metrics['errors'] = metrics['errors'] + 1

console_only_log_event("Amazon S3 Synchroniser %s" % THIS_VERSION)
console_only_log_event("Copyright 2014 S. J. Consulting Ltd. All rights reserved")

parser = argparse.ArgumentParser()

parser.add_argument('-k','--awsaccesskeyid', help='AWS Access Key ID', required=True)
parser.add_argument('-s','--awssecretaccesskey', help='AWS Secret Access Key', required=True)
parser.add_argument('-b','--bucketname', help='AWS Bucket Name', required=True)
parser.add_argument('-c','--cloudpath', help='AWS cloud path', required=True)
parser.add_argument('-l','--localpath', help='local path', required=True)
parser.add_argument('-d','--direction', help='transfer direction (upload, download)', required=True)
parser.add_argument('--logfile', help='log file name')
parser.add_argument('--maxactions', help='maximum number of actions', default=0, type=int)
parser.add_argument('--md5', action='store_true', default=False, help='enable md5 hash file checking')
parser.add_argument('--dryrun',action='store_true', default=False, help='enable dryrun')
parser.add_argument('--delete', action='store_true', default=False, help='enable file deletion')

options = parser.parse_args()

console_only_log_event("awsaccesskeyid=%s" % options.awsaccesskeyid)
console_only_log_event("awssectaccesskey=%s" % options.awssecretaccesskey)
console_only_log_event("bucketname=%s" % options.bucketname)
console_only_log_event("localpath=%s" % options.localpath)
console_only_log_event("cloudpath=%s" %  options.cloudpath)
console_only_log_event("direction=%s" % options.direction)
console_only_log_event("logfile=%s" % options.logfile)
console_only_log_event("maxactions=%s" % options.maxactions)
console_only_log_event("md5=%s" % options.md5)
console_only_log_event("dryrun=%s" % options.dryrun)
console_only_log_event("delete=%s" % options.delete)

conn = boto.connect_s3(options.awsaccesskeyid, options.awssecretaccesskey)
bucket = conn.get_bucket(options.bucketname)

if bucket == None:
    console_only_log_event("Error: invalid bucket %s" % options.bucketname)
    conn.close()
    exit(0)

metrics ={'errors': 0, 'uploads': 0, 'downloads': 0, 'deletes': 0}

cloudfiles = {}
cloudfiles['__prefix__'] = options.cloudpath

cloud_with_prefix(bucket,cloudfiles)
#print "cloud"
#pprint.pprint(cloudfiles)

localfiles = {}
localfiles['__prefix__'] = options.localpath
local_with_prefix(localfiles)

#print "local"
#pprint.pprint(localfiles)

actions = []
if options.direction == 'upload':
    generate_actions(localfiles, cloudfiles, 'upload', actions, options.maxactions, options.md5, delete=options.delete)
else:
    generate_actions(cloudfiles , localfiles, 'download', actions, options.maxactions, options.md5, delete=options.delete)

#print "actions"
#pprint.pprint( actions)

errors = {}
perform_actions(bucket,actions,options.localpath, options.cloudpath, errors, dryrun=options.dryrun)
conn.close()

log_event("%s uploads, %s downloads, %s deletes, %s errors" % (metrics['uploads'], metrics['downloads'], metrics['deletes'], metrics['errors']))
log_event(RULER)

commit_log(options.logfile, 'top')

S3Sync
======
a command line Amazon S3 file/folder synchronization utility.

Syntax
======
s3sync.py -s secretaccesskey -b bucket name -c cloud path -l localpath -d [upload|download] [-logfile filename] [-maxactions n] [-md5] [-dryrun] [-delete]

Usage
=====
-s, --awssecretaccesskey , AWS Secret Access Key  
-b, --bucketname, AWS Bucket Name  
-c, --cloudpath, AWS cloud path  
-l,--localpath, local path  
-d, --direction, transfer direction (upload, download)  
--logfile , log file name  
--maxactions, maximum number of actions  
--md5, enable md5 hash file checking  
--dryrun, enable dryrun')  
--delete, enable file deletion')  

Example
=======
88s3sync.py -k AFIAKP0IQFWOEFCYAYEA -s JusNeBKWC/K6lUEeQiXg+nTPnnwxCXUT+CuIQ9C -b testbucket -c Backup -l /media/Backup -d upload --logfile /media/Backup/s3.log --maxactions 100

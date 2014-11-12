S3Sync
======
a command line Amazon S3 file/folder synchronization utility. I have included the (older) ruby version just for reference.

Syntax
======
s3sync.py -k awsacesskeyid -s awssecretaccesskey -b bucket name -c cloud path -l local path -d [upload|download] [-logfile log filename] [-maxactions n] [-md5] [-dryrun] [-delete]

Usage
=====
-k ,--awsaccesskeyid , AWS Access Key ID
-s, --awssecretaccesskey , AWS Secret Access Key  
-b, --bucketname, AWS Bucket Name  
-c, --cloudpath, AWS cloud path  
-l,--localpath, local path  
-d, --direction, transfer direction (upload, download)  
--logfile , log file name  
--maxactions, maximum number of actions  
--md5, enable md5 hash file checking  
--dryrun, enable dryrun'  
--delete, enable file deletion'  

Example
=======
s3sync.py -k AFIAKP0IQFWOEFCYAYEA -s JusNeBKWC/K6lUEeQiXg+nTPnnwxCXUT+CuIQ9C -b testbucket -c Backup -l /media/Backup -d upload --logfile /media/Backup/s3.log --maxactions 100

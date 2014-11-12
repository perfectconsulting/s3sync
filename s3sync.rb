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

require "aws-sdk"
require 'optparse'
require "digest/md5"
require "ap"
require "fileutils"
require "time"

THIS_VERSION = "1.0.4"
RULER = "--------------------------------------------------------------------------------"

$log = []
$metrics= {:uploads => 0, :downloads => 0, :deletes => 0, :errors => 0}
AWS.config(:http_open_timeout => nil, :http_read_timeout => nil, :http_idel_timeout => nil)

module Helper
  def Helper.cleanpath(path)
    return path.end_with?("/") ? path : path.dup() << "/"
  end

  def Helper.create_path_only(path)
    dir = File.dirname(path)

    unless File.directory?(dir)
      FileUtils.mkdir_p(dir)
    end
  end

  def Helper.shortentext(text, max)
    if max > 3
      return text.length > max ? "..." + text[-(max-3)..-1] : text
    end
    return text
  end
end

def file_only_log_event(text)
    time = Time.new.strftime("%Y-%m-%d %H:%M:%S")
    $log << "#{time} #{text}"
end

def console_only_log_event(text)
    puts(text)
end

def log_event(text)
  file_only_log_event(text)
  console_only_log_event(text)
end

def commit_log(logfilename, position)
  if position == :top
    if File.exists?(logfilename)
      tempname = "_temp.log"
      tempfile = File.new(tempname, "a")
      $log.each do |text|
        tempfile.puts(text)
      end
      logfile = File.new(logfilename, "r").each do |text|
        tempfile.puts(text)
      end

      logfile.close
      tempfile.close

      File.delete(logfilename)
      File.rename(tempname, logfilename)

      return
    end
  end

  logfile = File.new(logfilename, "a")
  $log.each do |text|
    logfile.puts(text)
  end

  logfile.close
end

def cloud_with_prefix(s3, bucketname, prefix, files = nill)
  files = {} if files.nil?
  file = Struct.new(:last_modified, :size, :etag)
  marker = nil
  params = { :bucket_name => bucketname }

  begin
    options = marker ? params.merge(:marker => marker) : params
    resp = s3.client.list_objects(options)

    resp[:contents].each do |obj|
      next if ! obj[:key].start_with?(prefix)
      files[obj[:key][prefix.length..-1]] = file.new(obj[:last_modified], obj[:size], obj[:etag])
    end

    marker = resp[:contents].last[:key]

  end while resp[:truncated]
end


def local_with_prefix(prefix, files = nil, folder = "")
  files = {} if files.nil?
  file = Struct.new(:last_modified, :size, :etag)
  _folder = Helper::cleanpath(folder)
  path = Helper::cleanpath(prefix + folder)

  Dir.foreach(path) do |obj|
    next if obj.start_with?("_") || obj.start_with?(".")
    if File.directory?(path + obj)
      local_with_prefix(prefix, files, _folder + obj)
    else
      if !obj.end_with?("Thumbs.db")
        #digest = Digest::MD5.file(path + file)
        #files[_folder + file] = "\"#{digest.hexdigest}\""
        files[_folder + obj] = file.new(File.mtime(path + obj).utc,File.size(path + obj),nil)
      end
    end
  end

  return files
end

def generate_actions!(localpath, localfiles, cloudfiles, direction, actions = nil, maxactions = 0, md5 = false)
  actions = [] if actions.nil?
  action = Struct.new(:operation, :object, :param)

  case direction
    when :upload
      cloudfiles.each do |key,file|
        if !key.end_with?("/")
          if localfiles.has_key?(key)
            if localfiles[key].last_modified == file.last_modified && localfiles[key].size == file.size
              localfiles.delete(obj.key)
            else
              if md5
                digest = Digest::MD5.file(localpath + key)
                localfiles.delete(key) if file.etag == "\"#{digest.hexdigest}\""
              else
                localfiles.delete(key)
              end
            end
          else
            actions << action.new(:deletecloud, key)
            break if maxactions > 0 && actions.count >= maxactions
          end
        end
      end

      localfiles.each do |key, md5|
        if !key.end_with?("/")
          actions << action.new(:upload, key, nil)
          break if maxactions > 0 && actions.count >= maxactions
        end
      end

    when :download
      localfiles.each do |key, file|
        if cloudfiles.has_key?(key)
          if cloudfiles[key].last_modified == file.last_modified && cloudfiles[key].size == file.size
            cloudfiles.delete(key)
          else
            if md5
              digest = Digest::MD5.file(localpath + key)
              cloudfiles.delete(key) if cloudfiles[key].etag == "\"#{digest.hexdigest}\""
            else
              cloudfiles.delete(key)
            end
          end
        else
          actions << action.new(:deletelocal, key)
          break if maxactions > 0 && actions.count >= maxactions
        end
      end

      cloudfiles.each do |key, file|
        if !key.end_with?("/")
          actions << action.new(:download, key, file.last_modified )
          break if maxactions > 0 && actions.count >= maxactions
        end
      end
  end

  return actions
end

def perform_actions(s3, bucketname, actions, localpath, cloudpath, errors = nil, dryrun = false, maxretries = 0)
  return if s3.nil? || bucketname.nil?
  errors = [] if errors.nil?
  maxretries = 3 if maxretries == 0

  actions.each do |action|
    case action.operation
      when :upload
        cloud = cloudpath  + action.object
        local = localpath + action.object

        retries = 0
        operation = "upload "
        while retries < maxretries

          console_only_log_event(operation + Helper::shortentext(local, 60))
          file_only_log_event (operation + local)

          begin
            s3.buckets[bucketname].objects[cloud].write(:file => local,  :single_request => true) if !dryrun
          rescue
            operation = "re-upload "
            retries =+ 1
          else
            break
          end
        end

        if retries < maxretries
          $metrics[:uploads] += 1
        else
          console_only_log_event("upload failed " + Helper::shortentext(local, 60))
          file_only_log_event ("upload failed " + local)
          $metrics[:errors] += 1
        end

      when :download
        cloud = cloudpath + action.object
        local = localpath + action.object


        retries = 0
        operation = "download "
        while retries < maxretries
          console_only_log_event(operation + Helper::shortentext(cloud, 60))
          file_only_log_event (operation + cloud)

          begin
            Helper::create_path_only(local)

            object = s3.buckets[bucketname].objects[cloud]
            File.open(local, 'wb') do |file|
              object.read do |chunk|
              file.write(chunk)
              end
            end

          rescue
            operation = "re-downlaod "
            retries =+ 1
          else
            break
          end
        end

        if retries < maxretries
          File.utime(File.atime(local), action.param, local)
          $metrics[:downloads] += 1
        else
          console_only_log_event("download failed " + Helper::shortentext(local, 60))
          file_only_log_event ("download failed " + local)
          $metrics[:errors] += 1
        end

      when :deletelocal
        local = localpath + action.object

        console_only_log_event("delete local " + Helper::shortentext(local, 60))
        file_only_log_event ("delete local" + local)

        if !dryrun
          File.delete(local)
        end
        $metrics[:deletes] += 1

      when :deletecloud
        cloud = cloudpath  +action.object

        console_only_log_event("delete cloud " + Helper::shortentext(cloud, 60))
        file_only_log_event ("delete cloud " + cloud)

        if !dryrun
          s3.buckets[bucketname].objects.delete(cloud)
        end
        $metrics[:deletes] += 1
    end
  end
end

console_only_log_event("Amazon S3 Synchroniser #{THIS_VERSION}")
console_only_log_event("Copyright 2013 S. J. Consulting Ltd. All rights reserved")

#ARGV = ["-k", "AKIAJPJIQXPOAFCHAYQA", "-s", "TJvNeBUxC/K6lOX7eQiXg+nTPnnwxCXUT+CuIQ9C", "-b", "galvanisedbucket", "-c", "Test", "-l", "c:/Temp/s3", "-d", "upload", "--logfile", "s3.log", "--maxactions", "10"]
#ARGV = ["-k", "AKIAJPJIQXPOAFCHAYQA", "-s", "TJvNeBUxC/K6lOX7eQiXg+nTPnnwxCXUT+CuIQ9C", "-b", "galvanisedbucket", "-c", "Backup", "-l", "M:/Backup", "-d", "upload", "--logfile", "s3.log", "--dryrun","--maxactions", "2", "--md5"]

options = {:awsaccesskeyid => nil,:awssecretaccesskey =>nil,:bucketname => nil, :direction => nil, :dryrun => false, :maxactions => 0, :maxretries => 0, :md5 => false}

optparse = OptionParser.new do|opts|
  opts.banner = "Usage: s3sync.rb ..."
  opts.on( '-h', '--help', 'Display this screen' ) do
    puts opts
    exit
  end
  opts.on('-k', '--awsaccesskey KEY', 'AWS Access Key ID') do |key|
    options[:awsaccesskeyid] = key
  end
  opts.on('-s', '--awssecretaccesskey KEY', 'AWS Secret Access Key') do |key|
    options[:awssecretaccesskey] = key
  end
  opts.on('-b', '--bucketname NAME', 'AWS Bucket Name') do |bucket|
    options[:bucketname] = bucket
  end

  opts.on('-c', '--cloudpath PREFIX', 'AWS cloud path') do |path|
    options[:cloudpath] = path
  end

  opts.on('-l', '--localpath PREFIX', 'local  path') do |path|
    options[:localpath] = path
  end

  opts.on('-d', '--direction [DIRECTION]', [:upload, :download], "transfer direction (upload, download)") do |direction|
    options[:direction] = direction
  end

  opts.on('--logfile FILE', 'log file name') do |file|
    options[:logfile] = file
  end

  opts.on('--dryrun', 'perform a dryrun only')  do
    options[:dryrun] = true
  end

  opts.on('--md5', 'perform a an md5 comparison on identical files only')  do
    options[:md5] = true
  end

  opts.on("--maxactions N", Integer, "maximum number of actions") do |n|
    options[:maxactions] = n
  end

  opts.on("--maxretries N", Integer, "maximum number of retries") do |n|
    options[:maxretries] = n
  end
end

optparse.parse!(ARGV)

missing = []
options.each {|a,b| missing << a if b.nil? }

if !missing.empty?
  console_only_log_event optparse
  console_only_log_event "Error: missing options: #{missing.join(', ')}"
  exit
end

console_only_log_event("awsaccesskeyid=#{options[:awsaccesskeyid]}")
console_only_log_event("awssecretaccesskey=#{options[:awssecretaccesskey]}")
console_only_log_event("bucketname=#{options[:bucketname]}")
console_only_log_event("localpath=#{options[:localpath]}")
console_only_log_event("cloudpath=#{options[:cloudpath]}")
console_only_log_event("direction=#{options[:direction]}")
console_only_log_event("logfile=#{options[:logfile]}") if !options[:logfile].nil?
console_only_log_event("maxactions=#{options[:maxactions]}") if options[:maxactions] > 0
console_only_log_event("maxretries=#{options[:maxretries]}")  if options[:maxretries] > 0
console_only_log_event("md5=#{options[:md5]}")
console_only_log_event("dryrun=#{options[:dryrun]}")


s3 = AWS::S3.new(:access_key_id => options[:awsaccesskeyid],:secret_access_key => options[:awssecretaccesskey])

if s3.buckets[options[:bucketname]].nil?
  console_only_log_event "Error: invalid bucket #{options[:bucketname]}"
  exit
end

console_only_log_event("Checking cloud file system #{options[:cloudpath]}")

cloudfiles = {}
cloud_with_prefix(s3, options[:bucketname],options[:cloudpath], cloudfiles)
#ap cloudfiles

console_only_log_event("Checking local file system #{options[:localpath]}")
localfiles = {}
local_with_prefix(options[:localpath], localfiles)
#ap localfiles

console_only_log_event("Analysing differences...");
actions = []
generate_actions!(options[:localpath],localfiles, cloudfiles, options[:direction], actions, options[:maxactions], options[:md5])
#ap actions

if actions.empty?
  log_event("Files are up-to-date")
else
  errors = []
  perform_actions(s3, options[:bucketname],actions,options[:localpath],options[:cloudpath],errors,options[:dryrun], options[:maxretries])
  log_event("#{$metrics[:uploads]} uploaded, #{$metrics[:downloads]} download, #{$metrics[:deletes]} deleted, #{$metrics[:errors]} errors")
  log_event(RULER)
end

commit_log(options[:logfile], :top)  if !options[:logfile].nil?


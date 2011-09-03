#!/bin/bash -eu
# -e: Exit immediately if a command exits with a non-zero status.
# -u: Treat unset variables as an error when substituting.
# 
# Custom backup script that follows conventions for script backups implemented
# by ScriptBackupAgent.  
#

usage() {
  echo "Usage: $0 {-backup|-restore} -properties file [-options opts]"
}

# Parse arguments. 
operation=
options=
properties=

user=root
password=
host=localhost
port=3306
directory=/tmp/innobackup
archive=/tmp/innobackup.tar.gz
mysql_service_command="/etc/init.d/mysql"
mysqldatadir=/var/lib/mysql
mysqluser=mysql
mysqlgroup=mysql

while [ $# -gt 0 ]
do
  case "$1" in
    -backup) 
      operation="backup";;
    -restore) 
      operation="restore";;
    -properties)
      properties="$2"; shift;;
    -options)
      options="$2"; shift;;
    *)  
      echo "unrecognized option: $1"
      usage;
      exit 1;
  esac
  shift
done

#
# Break apart the options and assign them to variables
#
for i in `echo $options | tr '&' '\n'`
do
  parts=(`echo $i | tr '=' '\n'`)
  eval $parts=${parts[1]}
done

# Note 1.
# Do not use 'which' to determine where the 'service' command is.
# The command will fail if 'service' is not in the PATH, and the whole script will fail.
# Moreover, a simple 'sudo' will not get '/sbin' in $PATH, and the comamnd is likely to fail.
#
# Note 2.
# Use '-x' to check for executables. Using '-f' will fail if the file is there but it is 
# not executable.
service_command=/sbin/service
if [ -x $service_command ]
then
    mysql_service_command="$service_command mysql"
elif [ -x "/etc/init.d/mysqld" ]
then
    mysql_service_command="/etc/init.d/mysqld"
elif [ -x "/etc/init.d/mysql" ]
then
    mysql_service_command="$/etc/init.d/mysql"
else
    echo "Unable to determine the service command to start/stop mysql" >&2
    exit 1
fi

# Handle operation. 
if [ "$operation" = "backup" ]; then
  # Echo backup file to properties. 
  echo "file=$archive" > $properties

  # Clean up the filesystem before starting
  rm -rf $directory
  rm -f $archive

  # Copy the database files and apply any pending log entries
  innobackupex-1.5.1 --user=$user --password=$password --host=$host --port=$port --no-timestamp $directory
  innobackupex-1.5.1 --apply-log --user=$user --password=$password --host=$host --port=$port $directory

  # Package up the files and remove the staging directory
  cd $directory
  tar -czf $archive *
  rm -rf $directory

  exit 0
elif [ "$operation" = "restore" ]; then
  # Get the name of the backup file and restore.  
  . $properties

  # Clean up the filesystem before starting
  rm -rf $directory
  mkdir $directory
  cd $directory
  
  # Unpack the backup files
  tar -xzf $file

  # Stop mysql and clear the mysql data directory
  $mysql_service_command stop 1>&2

  # We are expecting the exit code to be 3 so we have to turn off the 
  # error trapping
  set +e
  `mysql -u$user -p$password -h$host -P$port -e "select 1"` > /dev/null 2>&1
  if [ $? -ne 1 ]; then
    echo "Unable to properly shutdown the MySQL service" >&2
    exit 1
  fi
  set -e
  
  rm -rf $mysqldatadir/*

  # Copy the backup files to the mysql data directory
  innobackupex-1.5.1 --ibbackup=xtrabackup_51 --copy-back $directory

  # Fix the permissions and restart the service
  chown -RL $mysqluser:$mysqlgroup $mysqldatadir
  $mysql_service_command start 1>&2

  # Remove the staging directory
  rm -rf $directory
  
  exit 0
else
  echo "Must specify -backup or -restore"
  usage
  exit 1
fi

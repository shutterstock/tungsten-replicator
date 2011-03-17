#!/bin/bash
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

# Handle operation. 
if [ "$operation" = "backup" ]; then
  # Echo backup file to properties. 
  echo "file=/tmp/mysqldump.sql" > $properties

  # Important tip: DATA RESTORE MUST NOT BE LOGGED.  So we put in a 
  # statement at the start of the dump to ensure it is not. 
  echo "SET SESSION SQL_LOG_BIN=0;" > /tmp/mysqldump.sql
  
  # Now dump the rest of the database.  This version must run cold. 
  mysqldump -utungsten -psecret -hlocalhost -P3306 --all-databases --skip-lock-tables --skip-add-locks >> /tmp/mysqldump.sql

  # The following variation dumps only a couple of databases.  It can run hot.
  # (I.e., with hotBackupEnabled=true). 
  # mysqldump -utungsten -psecret -hlocalhost -P3306 --single-transaction --add-drop-database --databases tungsten test >> /tmp/mysqldump.sql

elif [ "$operation" = "restore" ]; then
  # Get the name of the backup file and restore.  
  . $properties
  mysql -utungsten -psecret -hlocalhost -P3306 < $file
else
  echo "Must specify -backup or -restore"
  usage
  exit 1
fi

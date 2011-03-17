@echo off
rem
rem Tungsten Replicator @VERSION@
rem (c) 2008 Continuent, Inc.  All rights reserved. 
rem
rem Routine used to append to class path from a DOS loop.  This is a 
rem time-honored Tomcat trick. 
rem

set CLASSPATH=%CLASSPATH%;%1

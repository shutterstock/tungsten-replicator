#!/usr/bin/env ruby
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# Named properties for Tungsten configuration.

# Generic parameters that control the entire installation.
DEPLOYMENT_TYPE = "deployment_type"
DEPLOYMENT_HOST = "deployment_host"
DEPLOYMENT_SERVICE = "deployment_service"
HOST = "host_name"
IP_ADDRESS = "ip_address"
DSNAME = "local_service_name"
CLUSTERNAME = "cluster_name"
USERID = "userid"
DBMS_TYPE = "dbms_type"
HOME_DIRECTORY = "home_directory"
CURRENT_RELEASE_DIRECTORY = 'current_release_directory'
TEMP_DIRECTORY = "temp_directory"
SANDBOX_COUNT = "sandbox_instance_count"
DEPLOY_CURRENT_PACKAGE = "deploy_current_package"
GLOBAL_DEPLOY_PACKAGE_URI = "global_deploy_package_uri"
DEPLOY_PACKAGE_URI = "deploy_package_uri"
SHELL_STARTUP_SCRIPT = "shell_startup_script"
OPTION_MONITOR_INTERNAL = "internal_monitor"

# Operating system service parameters.
SVC_INSTALL = "install_svc_scripts"
SVC_START = "start_svc_scripts"
SVC_REPORT = "report_services"
ROOT_PREFIX = "root_command_prefix"

# Network control parameters.
HOSTS = "hosts"
WITNESSES = "witnesses"
DATASERVERS =  "dataservers"

# Generic replication parameters.
REPL_SERVICES = "repl_services"
REPL_HOSTS = "repl_hosts"
REPL_ROLE = "repl_role"
REPL_RMI_PORT = "repl_rmi_port"
REPL_AUTOENABLE = "repl_auto_enable"
REPL_DATASERVER = 'repl_dataserver'
REPL_MASTERHOST = "repl_master_host"
REPL_MASTERPORT = "repl_master_port"
REPL_BACKUP_METHOD = "repl_backup_method"
REPL_BACKUP_DUMP_DIR = "repl_backup_dump_dir"
REPL_BACKUP_STORAGE_DIR = "repl_backup_storage_dir"
REPL_BACKUP_RETENTION = "repl_backup_retention"
REPL_BACKUP_AUTO_BACKUP = "repl_backup_auto_backup"
REPL_BACKUP_AUTO_PROVISION = "repl_backup_auto_provision"
REPL_BACKUP_SCRIPT = "repl_backup_script"
REPL_BACKUP_ONLINE = "repl_backup_online"
REPL_BACKUP_COMMAND_PREFIX = "repl_backup_command_prefix"
REPL_BOOT_SCRIPT = "repl_boot_script"
REPL_DBHOST = "repl_dbhost"
REPL_DBPORT = "repl_dbport"
REPL_LOG_TYPE = "repl_log_type"
REPL_LOG_DIR = "repl_log_dir"
REPL_DBLOGIN = "repl_admin_login"
REPL_DBPASSWORD = "repl_admin_password"
REPL_MONITOR_INTERVAL = "repl_monitor_interval_millisecs"
REPL_JAVA_MEM_SIZE = "repl_java_mem_size"
REPL_BUFFER_SIZE = "repl_buffer_size"
REPL_MASTER_VIP  = "repl_master_vip"
REPL_MASTER_VIP_DEVICE = "repl_master_vip_device"
REPL_MASTER_VIP_IFCONFIG = "repl_master_vip_ifconfig"
REPL_RELAY_LOG_DIR = "repl_relay_log_dir"
REPL_THL_DO_CHECKSUM = "repl_thl_do_checksum"
REPL_THL_LOG_CONNECTION_TIMEOUT = "repl_thl_log_connection_timeout"
REPL_THL_LOG_FILE_SIZE = "repl_thl_log_file_size"

REPL_EXTRACTOR_USE_RELAY_LOGS = "repl_extractor_use_relay_logs"
REPL_EXTRACTOR_DATASERVER = "repl_extractor_dataserver"
REPL_EXTRACTOR_DBHOST = "repl_extractor_dbhost"
REPL_EXTRACTOR_DBPORT = "repl_extractor_dbport"
REPL_EXTRACTOR_DBLOGIN = "repl_extractor_dblogin"
REPL_EXTRACTOR_DBPASSWORD = "repl_extractor_dbpassword"

REPL_THL_LOG_RETENTION = "repl_thl_log_retention"
REPL_CONSISTENCY_POLICY = "repl_consistency_policy"
REPL_SVC_START = "repl_svc_start"
REPL_SVC_MODE = "repl_svc_mode"
REPL_SVC_CHANNELS = "repl_svc_channels"
REPL_SVC_SERVICE_TYPE = "repl_svc_service_type"
REPL_SVC_THL_PORT = "repl_svc_thl_port"
REPL_SVC_SHARD_DEFAULT_DB = "repl_svc_shard_default_db"
REPL_SVC_ALLOW_BIDI_UNSAFE = "repl_svc_allow_bidi_unsafe"
REPL_SVC_ALLOW_ANY_SERVICE = "repl_svc_allow_any_remote_service"
REPL_SVC_CONFIG_FILE = "repl_svc_config_file"
REPL_SVC_PARALLELIZATION_TYPE = "repl_svc_parallelization_type"
REPL_SVC_NATIVE_SLAVE_TAKEOVER = "repl_svc_native_slave_takeover"

REPL_USE_DRIZZLE = "repl_use_drizzle"

REPL_API = "repl_api"
REPL_API_PORT = "repl_api_port"
REPL_API_HOST = "repl_api_host"
REPL_API_USER = "repl_api_user"
REPL_API_PASSWORD = "repl_api_password"

# MySQL-specific parameters
GLOBAL_REPL_MYSQL_CONNECTOR_PATH = "global_mysql_connector_path"
REPL_MYSQL_CONNECTOR_PATH = "mysql_connector_path"
REPL_MYSQL_DATADIR = "repl_mysql_data_dir"
REPL_MYSQL_BINLOGDIR = "repl_mysql_binlog_dir"
REPL_MYSQL_MYCNF = "repl_mysql_mycnf"
REPL_MYSQL_BINLOGPATTERN = "repl_mysql_binlog_pattern"
REPL_MYSQL_RO_SLAVE = "repl_mysql_ro_slave"
REPL_MYSQL_EXTRACT_METHOD = "repl_mysql_extract_method"
REPL_MYSQL_SERVER_ID = "repl_mysql_server_id"

# Oracle-specific parameters.
REPL_ORACLE_SERVICE = "repl_oracle_service"
REPL_ORACLE_DSPORT = "repl_oracle_dslisten_port"
REPL_ORACLE_HOME = "repl_oracle_home"
REPL_ORACLE_LICENSE = "repl_oracle_license"
REPL_ORACLE_SCHEMA = "repl_oracle_schema"
REPL_ORACLE_LICENSED_SLAVE = "repl_oracle_licensed_slave"

# PostgreSQL-specific parameters.
REPL_PG_REPLICATOR = "repl_pg_replicator"
REPL_PG_STREAMING = "repl_pg_streaming"
REPL_PG_LOND_DATABASE = "repl_pg_lond_database"
REPL_PG_HOME = "repl_pg_home"
REPL_PG_ROOT = "repl_pg_root"
REPL_PG_POSTGRESQL_CONF = "repl_pg_postgresql_conf"
REPL_PG_ARCHIVE = "repl_pg_archive"
REPL_PG_ARCHIVE_TIMEOUT = "repl_pg_archive_timeout"
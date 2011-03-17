#!/usr/bin/env ruby
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# Named properties for Tungsten configuration.

# Generic parameters that control the entire installation.
GLOBAL_DEPLOYMENT_TYPE = "global_deployment_type"
GLOBAL_HOST = "global_host_name"
GLOBAL_IP_ADDRESS = "global_ip_address"
DSNAME = "data_service_name"
GLOBAL_DSNAME = "global_data_service_name"
GLOBAL_CLUSTERNAME = "global_cluster_name"
GLOBAL_USERID = "global_userid"
GLOBAL_DBMS_TYPE = "global_dbms_type"
GLOBAL_HOME_DIRECTORY = "global_home_directory"
GLOBAL_TEMP_DIRECTORY = "global_temp_directory"
SANDBOX_COUNT = "sandbox_instance_count"
DEPLOY_CURRENT_PACKAGE = "deploy_current_package"
GLOBAL_DEPLOY_PACKAGE_URI = "global_deploy_package_uri"
DEPLOY_PACKAGE_URI = "deploy_package_uri"
SHELL_STARTUP_SCRIPT = "shell_startup_script"
OPTION_MONITOR_INTERNAL = "internal_monitor"

# Operating system service parameters.
GLOBAL_SVC_INSTALL = "global_install_svc_scripts"
GLOBAL_SVC_START = "global_start_svc_scripts"
GLOBAL_ROOT_PREFIX = "global_root_command_prefix"

# Network control parameters.
GLOBAL_GC_MEMBERSHIP = "global_gc_membership_protocol"
GLOBAL_GOSSIP_PORT = "global_gossip_port"
GLOBAL_GOSSIP_HOSTS = "global_gossip_hosts"
GLOBAL_HOSTS = "global_hosts"
GLOBAL_WITNESSES = "global_witnesses"

# Generic replication parameters.
REPL_HOSTS = "repl_hosts"
REPL_AUTOENABLE = "repl_auto_enable"
REPL_MASTERHOST = "repl_master_host"
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
REPL_EXTRACTOR_USE_RELAY_LOGS = "repl_extractor_use_relay_logs"
REPL_RELAY_LOG_DIR = "repl_relay_log_dir"
REPL_THL_DO_CHECKSUM = "repl_thl_do_checksum"
REPL_THL_LOG_CONNECTION_TIMEOUT = "repl_thl_log_connection_timeout"
REPL_THL_LOG_FILE_SIZE = "repl_thl_log_file_size"

REPL_THL_LOG_RETENTION = "repl_thl_log_retention"
REPL_CONSISTENCY_POLICY = "repl_consistency_policy"
REPL_SVC_BINLOG_MODE = "repl_svc_binlog_mode"
REPL_SVC_CHANNELS = "repl_svc_channels"
REPL_SVC_MASTERPORT = "repl_svc_masterport"
REPL_SVC_SERVICE_TYPE = "repl_svc_service_type"
REPL_SVC_THL_PORT = "repl_svc_thl_port"

REPL_USE_BYTES = "repl_use_bytes"
REPL_USE_DRIZZLE = "repl_use_drizzle"

# MySQL-specific parameters
REPL_MYSQL_CONNECTOR_PATH = "global_mysql_connector_path"
REPL_MYSQL_BINLOGDIR = "repl_mysql_binlog_dir"
REPL_MYSQL_MYCNF = "repl_mysql_mycnf"
REPL_MYSQL_BINLOGPATTERN = "repl_mysql_binlog_pattern"
REPL_MYSQL_RO_SLAVE = "repl_mysql_ro_slave"

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

ROUTER_WAITFOR_DISCONNECT = "router_waitfor_disconnect"

CONN_HOSTS = "conn_hosts"
CONN_ACTIVE = "conn_is_active"
CONN_CLIENTLOGIN = "conn_client_login"
CONN_CLIENTPASSWORD = "conn_client_password"
CONN_CLIENTDEFAULTDB = "conn_client_default_db"
CONN_RWSPLITTING = "conn_rw_splitting"
CONN_LISTEN_PORT = "conn_listen_port"
CONN_DELETE_USER_MAP = "conn_delete_user_map"
SQLR_USENEWPROTOCOL = "sqlr_use_new_protocol"
SQLR_DELAY_BEFORE_OFFLINE = "sqlr_delay_before_offline"
SQLR_KEEP_ALIVE_TIMEOUT = "sqlr_keep_alive_timeout"

POLICY_MGR_MODE = "policy_mgr_mode"

MGR_DB_PING_TIMEOUT = "mgr_db_ping_timeout"
MGR_HOST_PING_TIMEOUT = "mgr_host_ping_timeout"
MGR_IDLE_ROUTER_TIMEOUT = "mgr_idle_router_timeout"
MGR_POLICY_LIVENESS_SAMPLE_PERIOD_SECS = "mgr_policy_liveness_sample_period_secs"
MGR_POLICY_LIVENESS_SAMPLE_PERIOD_THRESHOLD = "mgr_policy_liveness_sample_period_threshold"
MGR_POLICY_FENCE_SLAVE_REPLICATOR = "mgr_policy_fence_slave_replicator"
MGR_POLICY_FENCE_MASTER_REPLICATOR = "mgr_policy_fence_master_replicator"
MGR_POLICY_FAIL_THRESHOLD = "mgr_policy_fail_threshold"
MGR_NOTIFICATIONS_TIMEOUT = "mgr_notifications_timeout"
MGR_NOTIFICATIONS_SEND = "mgr_notifications_send"

MON_REPLICATOR_CHECK_FREQUENCY = "mon_replicator_check_frequency"
MON_DB_CHECK_FREQUENCY = "mon_db_check_frequency"
MON_DB_QUERY_TIMEOUT = "mon_db_query_timeout"
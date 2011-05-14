/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2011 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 *
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Alex Yurchenko, Linas Virbalas, Stephane Giron
 */

package com.continuent.tungsten.replicator.conf;

/**
 * This class defines a ReplicatorConf
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class ReplicatorConf
{
    /** Applier name. */
    static public final String   OPEN_REPLICATOR                       = "replicator.plugin";

    /** Replicator role - slave or master. */
    static public final String   ROLE                                  = "replicator.role";
    static public final String   ROLE_MASTER                           = "master";
    static public final String   ROLE_SLAVE                            = "slave";

    /** URI to which we connect. */
    static public final String   MASTER_CONNECT_URI                    = "replicator.master.connect.uri";

    /** Port at which to start allocating master listeners */
    static public final String   MASTER_LISTEN_PORT_START              = "replicator.masterListenPortStart";

    /** Port at which to start allocating master listeners */
    static public final String   SERVICE_RMI_PORT_START                = "replicator.serviceRMIPortStart";

    /** URI on which we listen. */
    static public final String   MASTER_LISTEN_URI                     = "replicator.master.listen.uri";

    /**
     * Should the master checks that its THL is in sync with its database before
     * starting. By default, it is turned on
     */
    static public final String   MASTER_THL_CHECK                      = "replicator.master.thl_check";
    static public final String   MASTER_THL_CHECK_DEFAULT              = "true";

    /** Whether to go online automatically at startup time. */
    static public final String   AUTO_ENABLE                           = "replicator.auto_enable";
    static public final String   AUTO_ENABLE_DEFAULT                   = "false";

    /** Whether to automatically provision this server at startup time. */
    static public final String   AUTO_PROVISION                        = "replicator.auto_provision";
    static public final String   AUTO_PROVISION_DEFAULT                = "false";

    /** Whether to automatically backup this server at startup time. */
    static public final String   AUTO_BACKUP                           = "replicator.auto_backup";
    static public final String   AUTO_BACKUP_DEFAULT                   = "false";

    /** Whether to go online automatically at startup time. */
    static public final String   DETACHED                              = "replicator.detached";
    static public final String   DETACHED_DEFAULT                      = "false";

    /** Source Identifier for THL and ReplDBMSEvents */
    static public final String   SOURCE_ID                             = "replicator.source_id";
    static public final String   SOURCE_ID_DEFAULT                     = "tungsten";

    /** Site name to which replicator belongs */
    static public final String   SITE_NAME                             = "site.name";
    static public final String   SITE_NAME_DEFAULT                     = "default";

    /** Cluster name to which replicator belongs */
    static public final String   CLUSTER_NAME                          = "cluster.name";
    static public final String   CLUSTER_NAME_DEFAULT                  = "default";

    /** Service name to which replication service belongs */
    static public final String   SERVICE_NAME                          = "service.name";

    /** Host where the replicator is running */
    static public final String   REPLICATOR_HOST                       = "replicator.host";

    /** Type of replicator service: local or remote. */
    static public final String   SERVICE_TYPE                          = "replicator.service.type";
    static public final String   SERVICE_TYPE_DEFAULT                  = "local";

    /** Out of sequence policy */
    static public final String   OOS_POLICY                            = "replicator.oos_policy";

    /** Where Replicator stores metadata */
    static public final String   METADATA_SCHEMA                       = "replicator.schema";
    static public final String   METADATA_SCHEMA_DEFAULT               = "tungsten_default";
    public static final String   METADATA_TABLE_TYPE                   = "replicator.table.engine";
    public static final String   METADATA_TABLE_TYPE_DEFAULT           = "";

    /** Shard assignment policies. */
    static public final String   SHARD_DEFAULT_DB_USAGE                = "replicator.shard.default.db";
    static public final String   SHARD_DEFAULT_DB_USAGE_DEFAULT        = "stringent";

    /** Whether to log slave updates. */
    static public final String   LOG_SLAVE_UPDATES                     = "replicator.log.slave.updates";
    static public final String   LOG_SLAVE_UPDATES_DEFAULT             = "false";

    /** RMI port */
    static public final String   RMI_PORT                              = "replicator.rmi_port";
    static public final String   RMI_HOST                              = "replicator.rmi_host";

    /** What checksum algorithm should be used (md5|sha|<empty string for none>) */
    static public final String   EVENT_CHECKSUM                        = "replicator.event.checksum";
    static public final String   EVENT_CHECKSUM_DEFAULT                = "";

    /** Extension parameter names. */
    static public final String   EXTENSIONS                            = "replicator.extensions";
    static public final String   EXTENSION_ROOT                        = "replicator.extension";

    /** Pipeline and stage parameter names. */
    static public final String   PIPELINES                             = "replicator.pipelines";
    static public final String   PIPELINE_ROOT                         = "replicator.pipeline";
    static public final String   STAGE                                 = "stage";
    static public final String   STAGE_ROOT                            = "replicator.stage";

    /** Applier parameter names. */
    static public final String   APPLIER                               = "applier";
    static public final String   APPLIER_ROOT                          = "replicator.applier";

    /** Extractor parameter names. */
    static public final String   EXTRACTOR                             = "extractor";
    static public final String   EXTRACTOR_ROOT                        = "replicator.extractor";

    /** Prefix for filter property definitions. */
    static public final String   FILTER                                = "filter";
    static public final String   FILTERS                               = "filters";
    static public final String   FILTER_ROOT                           = "replicator.filter";

    /** Store parameter names. */
    static public final String   STORE                                 = "store";
    static public final String   STORE_ROOT                            = "replicator.store";

    /** Applier failure policy */
    static public final String   APPLIER_FAILURE_POLICY                = "replicator.applier.failure_policy";
    static public final String   APPLIER_FAILURE_POLICY_DEFAULT        = "stop";

    /** Policy when consistency check fails (stop|warn) */
    static public final String   APPLIER_CONSISTENCY_POLICY            = "replicator.applier.consistency_policy";
    static public final String   APPLIER_CONSISTENCY_POLICY_DEFAULT    = "stop";

    /** Extractor failure policy (stop|skip) */
    static public final String   EXTRACTOR_FAILURE_POLICY              = "replicator.extractor.failure_policy";
    static public final String   EXTRACTOR_FAILURE_POLICY_DEFAULT      = "stop";

    /** Should consistency check be sensitive to column names (true|false) */
    static public final String   APPLIER_CONSISTENCY_COL_NAMES         = "replicator.applier.consistency_column_names";
    static public final String   APPLIER_CONSISTENCY_COL_NAMES_DEFAULT = "true";

    /** Should consistency check be sensitive to column types (true|false) */
    static public final String   APPLIER_CONSISTENCY_COL_TYPES         = "replicator.applier.consistency_column_types";
    static public final String   APPLIER_CONSISTENCY_COL_TYPES_DEFAULT = "true";

    /** RMI Defaults */
    static public final String   RMI_DEFAULT_PORT                      = "10000";
    static public final String   RMI_DEFAULT_SERVICE_NAME              = "replicator";
    static public final String   RMI_DEFAULT_HOST                      = "localhost";

    static public final String   THL_DB_URL                            = "replicator.store.thl.url";
    static public final String   THL_DB_USER                           = "replicator.store.thl.user";
    static public final String   THL_DB_PASSWORD                       = "replicator.store.thl.password";

    static public final String   THL_URI                               = "replicator.thl.uri";
    static public final String   THL_URI_DEFAULT                       = "thl://0.0.0.0/";
    static public final String   THL_REMOTE_URI_DEFAULT                = "thl://localhost/";

    static public final String   THL_STORAGE                           = "replicator.thl.storage";

    static public final String   THL_APPLIER_BLOCK_COMMIT_SIZE         = "replicator.thl.applier_block_commit_size";
    static public final String   THL_APPLIER_BLOCK_COMMIT_SIZE_DEFAULT = "0";

    public static final String   THL_PROTOCOL                          = "replicator.thl.protocol";
    public static final String   THL_PROTOCOL_DEFAULT                  = "com.continuent.tungsten.replicator.thl.Connector";
    public static final String   THL_PROTOCOL_BUFFER_SIZE              = "replicator.thl.protocol.buffer_size";
    public static final String   THL_PROTOCOL_BUFFER_SIZE_DEFAULT      = "0";

    static public final String   MONITOR_DETAIL_ENABLED                = "replicator.monitor.detail_enabled";

    /**
     * This information will be used by the sql router to create data sources
     * dynamically
     */
    static public final String   RESOURCE_JDBC_URL                     = "replicator.resourceJdbcUrl";
    /** Default value provided to enable unit tests to run. */
    static public final String   RESOURCE_JDBC_URL_DEFAULT             = "jdbc:mysql://localhost/${DBNAME}";
    static public final String   RESOURCE_JDBC_DRIVER                  = "replicator.resourceJdbcDriver";
    static public final String   RESOURCE_PRECEDENCE                   = "replicator.resourcePrecedence";
    static public final String   RESOURCE_PRECEDENCE_DEFAULT           = "99";
    static public final String   RESOURCE_VENDOR                       = "replicator.resourceVendor";
    static public final String   RESOURCE_LOGDIR                       = "replicator.resourceLogDir";
    static public final String   RESOURCE_LOGPATTERN                   = "replicator.resourceLogPattern";
    static public final String   RESOURCE_DISKLOGDIR                   = "replicator.resourceDiskLogDir";
    static public final String   RESOURCE_PORT                         = "replicator.resourcePort";
    static public final String   RESOURCE_DATASERVER_HOST              = "replicator.resourceDataServerHost";

    static public final String   GLOBAL_DB_USER                        = "replicator.global.db.user";
    static public final String   GLOBAL_DB_PASSWORD                    = "replicator.global.db.password";

    /** script based replicator plugin properties */
    static public final String   SCRIPT_ROOT_DIR                       = "replicator.script.root_dir";
    static public final String   SCRIPT_CONF_FILE                      = "replicator.script.conf_file";
    static public final String   SCRIPT_PROCESSOR                      = "replicator.script.processor";

    /** Dynamically settable property names. */
    static public final String[] DYNAMIC_PROPERTIES                    = {ROLE,
            AUTO_ENABLE, MASTER_CONNECT_URI, AUTO_PROVISION, AUTO_BACKUP};

}

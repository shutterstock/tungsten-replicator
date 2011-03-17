/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2010 Continuent Inc.
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
 * Initial developer(s): Edward Archibald
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.commons.cluster.resource.physical;

/**
 * Defines a replicator resource. Among other things this class contains the
 * definitive reference to names that replicators must use for monitoring
 * properties.
 */
public class ReplicationService
{
    /**
     * Replication service name.  In full clustering this should correspond
     * with a Tungsten service. 
     */
    public static final String SERVICE_NAME                = "serviceName";
    /**
     * Last LSN (log sequence number) applied to slave database. On a master
     * this is the same as the last position read from the log.
     */
    public static final String APPLIED_LAST_SEQNO          = "appliedLastSeqno";

    /**
     * Last native transaction ID applied to slave database. On a master this is
     * the same as the last native transaction ID read from the log.
     */
    public static final String APPLIED_LAST_EVENT_ID       = "appliedLastEventId";

    /**
     * Lag in seconds between the time last event was applied and when it was
     * originally generated.
     */
    public static final String APPLIED_LATENCY             = "appliedLatency";

    /** Current epoch number used by replicator, if it has one. */
    public static final String LATEST_EPOCH_NUMBER         = "latestEpochNumber";

    /**
     * Lowest LSN stored in the replicator log. Value is null if there is no
     * log.
     */
    public static final String MIN_STORED_SEQNO            = "minimumStoredSeqNo";

    /**
     * Highest LSN stored in the replicator log. Value is null if there is no
     * log. This value is used for selecting slaves for failover.
     */
    public static final String MAX_STORED_SEQNO            = "maximumStoredSeqNo";

    /**
     * Current native transaction ID in the database. On a slave this value may
     * be null.
     */
    public static final String CURRENT_EVENT_ID            = "currentEventId";

    /** URI to which slave replicator connects. Undefined for master. */
    public static final String MASTER_CONNECT_URI          = "masterConnectUri";

    /** URI that slaves should use when connecting to this master. */
    public static final String MASTER_LISTEN_URI           = "masterListenUri";

    /** Name of the cluster to which this replicator belongs. */
    public static final String CLUSTERNAME                 = "clusterName";

    /**
     * Role of this replicator. By convention roles are either 'master' or
     * 'slave'.
     */
    public static final String ROLE                        = "role";

    /** Host name of this replicator. */
    public static final String HOST                        = "host";

    /** Seconds since replicator has started. */
    public static final String UPTIME_SECONDS              = "uptimeSeconds";

    /** Seconds that replicator has been in its current state. */
    public static final String TIME_IN_STATE_SECONDS       = "timeInStateSeconds";

    /** Current replicator state. */
    public static final String STATE                       = "state";

    /** Source ID used to mark events for this replicator. */
    public static final String SOURCEID                    = "sourceId";

    /** Current exception that caused error, if there is one. */
    public static final String PENDING_EXCEPTION_MESSAGE   = "pendingExceptionMessage";

    /** Current error code, if there is one. */
    public static final String PENDING_ERROR_CODE          = "pendingErrorCode";

    /** Current error, if there is one. */
    public static final String PENDING_ERROR               = "pendingError";

    /** Current failed log sequence number or -1 if there is none. */
    public static final String PENDING_ERROR_SEQNO         = "pendingErrorSeqno";

    /** Current failed event ID or null if there is none. */
    public static final String PENDING_ERROR_EVENTID       = "pendingErrorEventId";

    /** URL to connect to underlying data source replicator serves. */
    static public final String RESOURCE_JDBC_URL           = "resourceJdbcUrl";

    /** Class name of Java data source. */
    static public final String RESOURCE_JDBC_DRIVER        = "resourceJdbcDriver";

    /** DBMS vendor string for this data source */
    static public final String RESOURCE_VENDOR             = "resourceVendor";

    /**
     * Log sequence number type to allow managers to figure out how to sort
     * values.
     */
    public static final String SEQNO_TYPE                  = "seqnoType";

    /**
     * Denotes a numeric log sequence number type that is convertible to a Java
     * Long type.
     */
    public static final String SEQNO_TYPE_LONG             = "java.lang.Long";

    /**
     * Denotes a string log sequence number type whose values are comparable
     * strings.
     */
    public static final String SEQNO_TYPE_STRING           = "java.lang.String";

    /**
     * Obsolete values provided for compatibility with branched open replicator.
     * 
     * @deprecated
     */
    public static final String MASTER_URI                  = "masterUri";

    /**
     * Contains the names of enabled extensions (e.g., sharding) on this
     * replicator.
     */
    public static final String EXTENSIONS                  = "extensions";

    // Default values
    public static final long   DEFAULT_LATEST_EPOCH_NUMBER = -1;
    public static final String DEFAULT_LAST_EVENT_ID       = "0:0";
    
}

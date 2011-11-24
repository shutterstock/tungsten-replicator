/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
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
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.commons.cluster.resource;

/**
 * Lists command parameters accepted by the replicator for those JMX commands
 * that are parameterized.
 */
public class OpenReplicatorParams
{
    // Parameters for online2() JMX call.

    /** Set initial event ID when going online. */
    public static final String INIT_EVENT_ID         = "extractFromId";

    /** Skip applying first N events after going online. */
    public static final String SKIP_APPLY_EVENTS     = "skipApplyEvents";

    /** Stay online until sequence number has been processed. */
    public static final String ONLINE_TO_SEQNO       = "toSeqno";

    /** Stay online until event ID has been processed. */
    public static final String ONLINE_TO_EVENT_ID    = "toEventId";

    /** Stay online until source timestamp has been processed. */
    public static final String ONLINE_TO_TIMESTAMP   = "toTimestamp";

    /** Stay online until next heartbeat has been processed. */
    public static final String ONLINE_TO_HEARTBEAT   = "toHeartbeat";

    /** Skip events from a list. */
    public static final String SKIP_APPLY_SEQNOS     = "skipApplySeqnos";

    /** Whether to skip consistency checks when going online. */
    public static final String FORCE                 = "force";

    // Parameters for offlineDeferred() JMX call.

    /** Go offline safely after next transactional boundary. */
    public static final String OFFLINE_TRANSACTIONAL = "atTransaction";

    /** Go offline after sequence number has been processed. */
    public static final String OFFLINE_AT_SEQNO      = "atSeqno";

    /** Go offline after event ID has been processed. */
    public static final String OFFLINE_AT_EVENT_ID   = "atEventId";

    /** Go offline after source timestamp has been processed. */
    public static final String OFFLINE_AT_TIMESTAMP  = "atTimestamp";

    /** Go offline after next heartbeat has been processed. */
    public static final String OFFLINE_AT_HEARTBEAT  = "atHeartbeat";

    // Parameters for heartbeat() JMX call.

    /** Timeout in seconds to wait for an operation (-1 = forever) */
    public static final String HEARTBEAT_NAME        = "heartbeatName";
}
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
package com.continuent.tungsten.replicator.event;

import java.sql.Timestamp;

/**
 * Denotes header data used for replication.  This is the core information
 * used to remember the replication position so that restart is possible. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface ReplDBMSHeader
{
    /**
     * Returns the log sequence number, a monotonically increasing whole 
     * number starting at 0 that denotes a single transaction. 
     */
    public long getSeqno();

    /**
     * Returns the event fragment number, a monotonically increasing whole
     * number starting at 0. 
     */
    public short getFragno();

    /**
     * Returns true if this fragment is the last one. 
     */
    public boolean getLastFrag();

    /**
     * Returns the ID of the data source from which this event was originally
     * extracted.  
     */
    public String getSourceId();

    /** 
     * Returns the epoch number, a number that identifies a continuous sequence
     * of events from the time a master goes online until it goes offline. 
     */
    public long getEpochNumber();

    /**
     * Returns the native event ID corresponding to this log sequence number. 
     */
    public String getEventId();
    
    /**
     * Returns the shard ID for this transaction. 
     */
    public String getShardId();
    
    /**
     * Returns the extractedTstamp value.
     */
    public Timestamp getExtractedTstamp();
}
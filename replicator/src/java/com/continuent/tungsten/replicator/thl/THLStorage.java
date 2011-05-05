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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.thl;

import java.util.ArrayList;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * This class defines a THLStorage
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public interface THLStorage
{
    /**
     * Prepare storage for use.
     * 
     * @throws ReplicatorException Thrown if resource allocation fails
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException;

    /**
     * Release all resources used by storage
     * 
     * @throws ReplicatorException Thrown if resource deallocation fails
     */
    public void release() throws ReplicatorException, InterruptedException;

    /** Set connection URL (assumed to be JDBC). */
    public void setUrl(String url);

    /** Set the storage user. */
    public void setUser(String user);

    /** Set the storage password. */
    public void setPassword(String password);

    /**
     * Store THLEvent.
     * 
     * @param event THLEvent
     * @param doCommit If true, commit this and previous uncommitted events
     * @param syncTHL If true, sync to THL table to track commits
     * @throws THLException
     */
    public void store(THLEvent event, boolean doCommit, boolean syncTHL)
            throws THLException, InterruptedException;

    /**
     * Set status for previously stored THLEvent.
     * 
     * @param seqno Sequence number of the event
     * @param fragno Fragment number of the event
     * @param status New status for the event
     * @param msg User message
     * @throws THLException
     */
    public void setStatus(long seqno, short fragno, short status, String msg)
            throws THLException, InterruptedException;

    /**
     * Find THLEvent corresponding to sequence numer.
     * 
     * @param seqno Sequence number
     * @return THLEvent corresponding to seqno or null if not found
     * @throws THLException
     */
    public THLEvent find(long seqno) throws THLException, InterruptedException;

    /**
     * Find fragment of THLEvent correspondint to sequence number.
     * 
     * @param seqno Sequence number
     * @param fragno Fragment number
     * @return THLEvent fragment corresponding to seqno or null if not found
     * @throws THLException
     * @throws InterruptedException
     */
    public THLEvent find(long seqno, short fragno) throws THLException,
            InterruptedException;

    /**
     * Get smallest sequence number present in storage.
     * 
     * @return Smallest sequence number in storage or -1 if storage is empty
     * @throws THLException
     */
    public long getMinSeqno() throws THLException;

    /**
     * Get smallest and highest sequence number present in storage
     * 
     * @return an array with smallest and highest sequence number or an array of
     *         -1 if storage is empty
     * @throws THLException
     */
    public long[] getMinMaxSeqno() throws THLException;

    /**
     * Get highest sequence number present in storage.
     * 
     * @return Highest sequence number in storage or -1 if storage is empty
     * @throws THLException
     */
    public long getMaxSeqno() throws THLException;

    /**
     * Get sequence number of the last event which has been processed. Event is
     * taken to be processed if it has reached state COMPLETED or SKIPPED.
     * 
     * @return Sequence number of the last event which has been processed
     * @throws THLException
     */
    public long getMaxCompletedSeqno() throws THLException;

    /**
     * Get event identifier corresponding to given sequence number.
     * 
     * @param seqno Sequence number
     * @return Event identifier
     * @throws THLException
     */
    public String getEventId(long seqno) throws THLException;

    /**
     * Get event identifier corresponding to the max sequence number for which
     * eventId is not null. We return null if there are no recorded events,
     * which is the case for a master on a newly initialized cluster or for a
     * new master after a failover, in which case the events in the log belong
     * to another server.
     * 
     * @param sourceId The source ID of this replicator
     * @return Event identifier or null
     * @throws THLException if an error occurs
     */
    public String getMaxEventId(String sourceId) throws THLException;

    /**
     * updateFailedStatus updates the status of the given events to FAILED.
     * failedEvent is the event that truly failed. The list of events contains
     * events that were rolled back due to the failedEvent failure. These events
     * are declared as failed as they were not committed.
     * 
     * @param failedEvent is the events that failed
     * @param events is the list of events that were not applied (not committed)
     *            due to the failedEvent failure
     * @throws THLException if an error occurs
     */
    public void updateFailedStatus(THLEventStatus failedEvent,
            ArrayList<THLEventStatus> events) throws THLException;

    /**
     * updateSuccessStatus updates the status of the given succeeded events (if
     * any) to COMPLETED and the status of the given skipped events (if any) to
     * SKIPPED.
     * 
     * @param succeededEvents is a list of events that were successfully applied
     * @param skippedEvents is a list of events that were skipped following an
     *            exception
     * @throws THLException TODO
     */
    public void updateSuccessStatus(ArrayList<THLEventStatus> succeededEvents,
            ArrayList<THLEventStatus> skippedEvents) throws THLException;

    /**
     * Retrieve the max fragno corresponding to the given event seqno.
     * 
     * @param seqno an event seqno
     * @return the max fragno for the given seqno
     * @throws THLException if an error occurs
     */
    public short getMaxFragno(long seqno) throws THLException;

    public THLBinaryEvent findBinaryEvent(long seqno, short fragno)
            throws THLException;

    /**
     * Purge THL events in the given seqno interval.
     * 
     * @param low Sequence number specifying the beginning of the range. Leave
     *            null to start from the very beginning of the table.
     * @param high Sequence number specifying the end of the range. Leave null
     *            to end at the very end of the table.
     * @return Number of deleted events
     * @throws InterruptedException
     * @throws THLException
     */
    public int delete(Long low, Long high, String before) throws THLException,
            InterruptedException;

    /**
     * Get the last applied event as found in the CommitSeqnoTable
     * 
     * @return An event header, or null if nothing found in the database
     * @throws THLException
     */
    public ReplDBMSHeader getLastAppliedEvent() throws THLException;
}

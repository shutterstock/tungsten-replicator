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
 * Initial developer(s): Robert Hodges
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.applier;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;

/**
 * Denotes an applier that can process events with full metadata.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @see com.continuent.tungsten.replicator.applier.RawApplier
 */
public interface Applier extends ReplicatorPlugin
{
    /**
     * Apply the proffered event to the replication target.
     * 
     * @param event Event to be applied
     * @param doCommit Boolean flag indicating whether this is the last part of
     *            multipart event
     * @param doRollback Boolean flag indicating whether this transaction should
     *            rollback
     * @param syncTHL Should this applier synchronize the trep_commit_seqno
     *            table? This should be false for slave.
     * @throws ReplicatorException Thrown if applier processing fails
     * @throws ConsistencyException Thrown if the applier detects that a
     *             consistency check has failed
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public void apply(ReplDBMSEvent event, boolean doCommit,
            boolean doRollback, boolean syncTHL) throws ReplicatorException,
            ConsistencyException, InterruptedException;

    /**
     * Update current recovery position but do not apply an event.
     * 
     * @param header Header containing seqno, event ID, etc.
     * @param doCommit Boolean flag indicating whether this is the last part of
     *            multipart event
     * @param syncTHL Should this applier synchronize the trep_commit_seqno
     *            table? This should be false for slave.
     * @throws ReplicatorException Thrown if applier processing fails
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public void updatePosition(ReplDBMSHeader header, boolean doCommit,
            boolean syncTHL) throws ReplicatorException, InterruptedException;

    /**
     * Commits current open transaction to ensure data applied up to current
     * point are durable.
     * 
     * @throws ReplicatorException Thrown if applier processing fails
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public void commit() throws ReplicatorException, InterruptedException;

    /**
     * Rolls back any current work.
     * 
     * @throws InterruptedException
     */
    public void rollback() throws InterruptedException;

    /**
     * Return header information corresponding to last committed transaction.
     * 
     * @return Header data for last committed transaction
     * @throws ReplicatorException Thrown if getting sequence number fails
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public ReplDBMSHeader getLastEvent() throws ReplicatorException,
            InterruptedException;

}

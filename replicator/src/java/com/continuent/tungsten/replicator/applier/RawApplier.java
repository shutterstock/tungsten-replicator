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
 * Contributor(s): Robert Hodges, Stephane Giron
 */

package com.continuent.tungsten.replicator.applier;

import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;

/**
 * Denotes an applier, which is responsible for applying raw DBMS events to a
 * database or other replication target. Appliers must be prepared to be
 * interrupted, which mechanism is used to cancel processing.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public interface RawApplier extends ReplicatorPlugin
{
    /**
     * Sets the ID of the task using this raw applier.
     * 
     * @param id Task ID
     * @throws ApplierException Thrown if the ID exceeds the number of tasks
     *             allowed by the applier
     */
    public void setTaskId(int id) throws ApplierException;

    /**
     * Apply the proffered event to the replication target.
     * 
     * @param event Event to be applied
     * @param header Header data corresponding to event
     * @param doCommit Boolean flag indicating whether this is the last part of
     *            multi-part event
     * @param doRollback Boolean flag indicating whether this transaction should
     *            rollback
     * @throws ApplierException Thrown if applier processing fails
     * @throws ConsistencyException Thrown if the applier detects that a
     *             consistency check has failed
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit,
            boolean doRollback) throws ApplierException, ConsistencyException,
            InterruptedException;

    /**
     * Commits current open transaction to ensure data applied up to current
     * point are durable.
     * 
     * @throws ApplierException Thrown if applier processing fails
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public void commit() throws ApplierException, InterruptedException;

    /**
     * Rolls back any current work.
     * 
     * @throws InterruptedException Thrown if the applier is interrupted.
     */
    public void rollback() throws InterruptedException;

    /**
     * Return header information corresponding to last committed event.
     * 
     * @return Header data for last committed event.
     * @throws ApplierException Thrown if getting sequence number fails
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public ReplDBMSHeader getLastEvent() throws ApplierException,
            InterruptedException;
}

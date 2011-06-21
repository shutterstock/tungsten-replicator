/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010-2011 Continuent Inc.
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

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * This class wraps a basic Applier so that it handles ReplDBMSEvent values with
 * assigned sequence numbers.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ApplierWrapper implements ParallelApplier
{
    private static Logger logger = Logger.getLogger(ApplierWrapper.class);
    private RawApplier    applier;

    /**
     * Create a new instance to wrap a raw applier.
     * 
     * @param applier Extractor to be wrapped
     */
    public ApplierWrapper(RawApplier applier)
    {
        this.applier = applier;
    }

    /** Return wrapped applier. */
    public RawApplier getApplier()
    {
        return applier;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.ParallelApplier#setTaskId(int)
     */
    public void setTaskId(int id) throws ApplierException
    {
        applier.setTaskId(id);
    }

    /**
     * Apply the DBMSEvent in the ReplDBMSEvent. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#apply(com.continuent.tungsten.replicator.event.ReplDBMSEvent,
     *      boolean, boolean, boolean)
     */
    public void apply(ReplDBMSEvent event, boolean doCommit,
            boolean doRollback, boolean syncTHL) throws ApplierException,
            ConsistencyException, InterruptedException
    {
        DBMSEvent myEvent = event.getDBMSEvent();
        if (myEvent instanceof DBMSEmptyEvent)
        {
            // Handling empty events :
            // - if it is the first fragment, this is an empty
            // commit, it can then be safely ignored
            // - if it is the last fragment, it should commit
            if (event.getFragno() > 0)
            {
                applier.apply(myEvent, event, true, false);
            }
            else
            {
                // Empty commit : just ignore
                applier.apply(myEvent, event, false, false);
            }
        }
        else
            applier.apply(myEvent, event, doCommit, doRollback);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#updatePosition(com.continuent.tungsten.replicator.event.ReplDBMSHeader,
     *      boolean, boolean)
     */
    public void updatePosition(ReplDBMSHeader header, boolean doCommit,
            boolean syncTHL) throws ReplicatorException, InterruptedException
    {
        DBMSEmptyEvent empty = new DBMSEmptyEvent(null, null);
        applier.apply(empty, header, doCommit, false);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#commit()
     */
    public void commit() throws ApplierException, InterruptedException
    {
        applier.commit();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#rollback()
     */
    public void rollback() throws InterruptedException
    {
        applier.rollback();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#getLastEvent()
     */
    public ReplDBMSHeader getLastEvent() throws ApplierException,
            InterruptedException
    {
        return applier.getLastEvent();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        logger.info("Configuring raw applier");
        applier.configure(context);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        logger.info("Preparing raw applier");
        applier.prepare(context);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        logger.info("Releasing raw applier");
        applier.release(context);
    }
}

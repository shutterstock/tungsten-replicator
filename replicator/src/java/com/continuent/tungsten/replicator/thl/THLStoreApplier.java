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

package com.continuent.tungsten.replicator.thl;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.Applier;
import com.continuent.tungsten.replicator.applier.ApplierException;
import com.continuent.tungsten.replicator.event.ReplControlEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.thl.log.LogConnection;

/**
 * Implements Extractor and Applier interface for a transaction history log
 * (THL).
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLStoreApplier implements Applier
{
    private static Logger logger = Logger.getLogger(THLStoreApplier.class);
    private String        storeName;
    private THL           thl;
    private PluginContext context;
    private LogConnection client;

    /**
     * Instantiate the adapter.
     */
    public THLStoreApplier()
    {
    }

    public String getStoreName()
    {
        return storeName;
    }

    public void setStoreName(String storeName)
    {
        this.storeName = storeName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        this.context = context;
    }

    /**
     * Connect to underlying queue. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        try
        {
            thl = (THL) context.getStore(storeName);
            client = thl.connect(false);
        }
        catch (ClassCastException e)
        {
            throw new ReplicatorException(
                    "Invalid storage class; configuration may be in error: "
                            + context.getStore(storeName).getClass().getName());
        }
        if (thl == null)
            throw new ReplicatorException(
                    "Unknown storage name; configuration may be in error: "
                            + storeName);
        logger.info("Storage adapter is prepared: name=" + storeName);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        if (thl != null)
        {
            thl.disconnect(client);
            thl = null;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#apply(ReplDBMSEvent,
     *      boolean, boolean, boolean)
     */
    public void apply(ReplDBMSEvent event, boolean doCommit,
            boolean doRollback, boolean syncTHL) throws ReplicatorException,
            InterruptedException
    {
        THLEvent thlEvent = new THLEvent(event.getEventId(), event);
        try
        {
            client.store(thlEvent, doCommit);
            if (doCommit)
            {
                if (syncTHL)
                    thl.updateCommitSeqno(thlEvent);
                commit();
            }
            if (logger.isDebugEnabled())
                logger.debug("Stored event " + event.getSeqno());
        }
        catch (THLException e)
        {
            throw new ApplierException("Unable to store event: seqno="
                    + event.getSeqno(), e);
        }
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
        // This call does not mean anything for a THL adapter.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#commit()
     */
    public void commit() throws ReplicatorException, InterruptedException
    {
        try
        {
            // Commit the log records.
            client.commit();

            // While we are at it check the end of the pipeline and update the
            // log active sequence number to wherever we have committed.
            long lastCommittedSeqno = context.getCommittedSeqno();
            thl.updateActiveSeqno(lastCommittedSeqno);
        }
        catch (THLException e)
        {
            throw new ApplierException("Unable to commit to log", e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#rollback()
     */
    public void rollback() throws InterruptedException
    {
        // TODO: Implement.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#getLastEvent()
     */
    public ReplDBMSHeader getLastEvent() throws ReplicatorException,
            InterruptedException
    {
        long seqno = thl.getMaxStoredSeqno();
        if (seqno < 0)
        {
            // Nothing found in THL. As it could have been
            // purged, check if there is a stored synchronization point (i.e.
            // something stored in trep_commit_seqno table). If so, return this.
            try
            {
                return thl.getLastAppliedEvent();
            }
            catch (ReplicatorException e)
            {
                logger.warn("Failed to retrieve last applied event", e);
            }
        }
        else
        {
            LogConnection conn = null;
            try
            {
                conn = thl.connect(true);
                if (conn.seek(seqno))
                {
                    THLEvent event;
                    while ((event = conn.next(false)) != null)
                    {
                        if (event.getLastFrag())
                        {
                            ReplEvent replEvent = event.getReplEvent();
                            if (replEvent instanceof ReplDBMSEvent)
                                return (ReplDBMSEvent) replEvent;
                            else if (replEvent instanceof ReplControlEvent)
                                return ((ReplControlEvent) replEvent)
                                        .getHeader();
                            else
                            {
                                // Should be unreachable.
                                throw new ReplicatorException(
                                        "Invalid type found while searching for last event: seqno="
                                                + event.getSeqno()
                                                + " class="
                                                + replEvent.getClass()
                                                        .getCanonicalName());
                            }
                        }
                    }
                }
            }
            catch (THLException e)
            {
                throw new ReplicatorException(
                        "Unable to fetch last event from THL: seqno=" + seqno,
                        e);
            }
            finally
            {
                conn.release();
            }
        }

        // If we get here, there's nothing to find. Return null.
        return null;
    }
}
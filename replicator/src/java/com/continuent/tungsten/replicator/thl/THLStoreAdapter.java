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

package com.continuent.tungsten.replicator.thl;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.Applier;
import com.continuent.tungsten.replicator.applier.ApplierException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;
import com.continuent.tungsten.replicator.extractor.Extractor;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements Extractor and Applier interface for a transaction history log
 * (THL).
 *
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLStoreAdapter implements Extractor, Applier
{
    private static Logger logger = Logger.getLogger(THLStoreAdapter.class);
    private String        storeName;
    private THL           thl;

    // Pointers to track storage.
    private long          seqno;
    private short         fragno;

    /**
     * Instantiate the adapter.
     */
    public THLStoreAdapter()
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
        // Do nothing.
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
        thl = null;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.extractor.Extractor#extract()
     */
    public ReplDBMSEvent extract() throws ExtractorException,
            InterruptedException
    {
        // Fetch next event and update pointers.
        ReplDBMSEvent event;
        try
        {
            event = thl.fetchEvent(seqno, fragno, true);
        }
        catch (ReplicatorException e)
        {
            throw new ExtractorException("Unable to fetch next event: seqno="
                    + seqno + " fragno=" + fragno, e);
        }

        if (event != null)
        {
            if (event instanceof ReplDBMSFilteredEvent)
            {
                ReplDBMSFilteredEvent ev = (ReplDBMSFilteredEvent) event;
                if (event.getLastFrag())
                {
                    seqno = ev.getSeqnoEnd() + 1;
                    fragno = 0;
                }
                else
                {
                    fragno = (short) (ev.getFragnoEnd() + 1);
                }
            }
            else if (event.getLastFrag())
            {
                // Start at next full event following this one.
                seqno = event.getSeqno() + 1;
                fragno = 0;
            }
            else
            {
                // Start at next fragment in current event.
                seqno = event.getSeqno();
                fragno = (short) (event.getFragno() + 1);
            }
        }
        return event;
    }

    /**
     * Return the event ID for a flush; does not make sense for a store.
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.extractor.Extractor#getCurrentResourceEventId()
     */
    public String getCurrentResourceEventId() throws ExtractorException,
            InterruptedException
    {
        return null;
    }

    /**
     * Returns true if the queue has more events. {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.extractor.Extractor#hasMoreEvents()
     */
    public boolean hasMoreEvents()
    {
        // TODO: Clean up; latter predicate is approximate/off by one?
        return (fragno > 0 || thl.pollSeqno(seqno + 1));
    }

    /**
     * Stores the last event we have processed. {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEvent(com.continuent.tungsten.replicator.event.ReplDBMSHeader)
     */
    public void setLastEvent(ReplDBMSHeader event) throws ExtractorException
    {
        // Remember where we were.
        if (event == null)
        {
            // Start at beginning for next event.
            seqno = 0;
            fragno = 0;
        }
        else
        {
            if (event.getLastFrag())
            {
                // Start at next full event following this one.
                seqno = event.getSeqno() + 1;
                fragno = 0;
            }
            else
            {
                // Start at next fragment in current event.
                seqno = event.getSeqno();
                fragno = (short) (event.getFragno() + 1);
            }
        }
    }

    /**
     * Ignored for now as stores do not extract. {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEventId(java.lang.String)
     */
    public void setLastEventId(String eventId) throws ExtractorException
    {
        logger.warn("Attempt to set last event ID on THL storage: " + eventId);
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.applier.Applier#apply(ReplDBMSEvent,
     *      boolean, boolean, boolean)
     */
    public void apply(ReplDBMSEvent event, boolean doCommit, boolean doRollback, boolean syncTHL)
            throws ApplierException, InterruptedException
    {
        try
        {
            thl.storeEvent(event, doCommit, syncTHL);
        }
        catch (ReplicatorException e)
        {
            throw new ApplierException("Unable to insert event into storage", e);
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
        // This call does not mean anything for a store adapter.
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.applier.Applier#commit()
     */
    public void commit() throws ApplierException, InterruptedException
    {
        // TODO: Implement.
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
    public ReplDBMSHeader getLastEvent() throws ApplierException,
            InterruptedException
    {
        long seqno = thl.getMaxStoredSeqno(false);
        if (seqno < 0)
        {
            // Nothing found into THL history table. As it could have been
            // purged, check if there is a stored synchronization point (i.e.
            // something stored in trep_commit_seqno table). If so, no use to
            // start from older events than this one.
            try
            {
                return thl.getLastAppliedEvent();

            }
            catch (ReplicatorException e)
            {
                logger.warn("Failed to retrieve last applied event", e);
            }
            return null;
        }
        else
        {
            // TODO: This is kind of a hack; we really need the last fragment,
            // so we fake a header that assumes we are not fragmented.
            short fragno = thl.getMaxFragno(seqno);
            try
            {
                ReplDBMSEvent event = thl.fetchEvent(seqno, fragno, false);
                return new ReplDBMSHeaderData(seqno, fragno, true,
                        event.getSourceId(), event.getEpochNumber(),
                        event.getEventId());
            }
            catch (ReplicatorException e)
            {
                throw new ApplierException(
                        "Unable to fetch max event, frag 0: seqno=" + seqno, e);
            }
        }
    }
}
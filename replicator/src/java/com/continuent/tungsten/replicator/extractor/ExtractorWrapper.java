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

package com.continuent.tungsten.replicator.extractor;

/**
 * Denotes a Runnable task that implements stage processing. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.cluster.resource.OpenReplicatorParams;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.consistency.ConsistencyCheckFilter;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.EventMetadataFilter;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.filter.Filter;
import com.continuent.tungsten.replicator.heartbeat.HeartbeatFilter;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * This class wraps a basic Extractor so that it returns ReplDBMSEvent values
 * with assigned sequence numbers. It contains logic to recognize that we have
 * failed over; see {@link #setLastEvent(ReplDBMSHeader)} for more information.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ExtractorWrapper implements Extractor
{
    private static Logger logger      = Logger.getLogger(ExtractorWrapper.class);
    private PluginContext pluginContext;
    private RawExtractor  extractor;
    private String        sourceId;
    private long          seqno       = 0;
    private short         fragno      = 0;
    private long          epochNumber = 0;
    private List<Filter>  autoFilters = new ArrayList<Filter>();

    /**
     * Create a new instance to wrap Creates a new <code>ExtractorWrapper</code>
     * object
     * 
     * @param extractor Extractor to be wrapped
     */
    public ExtractorWrapper(RawExtractor extractor)
    {
        this.extractor = extractor;
        this.autoFilters.add(new EventMetadataFilter());
        this.autoFilters.add(new HeartbeatFilter());
        this.autoFilters.add(new ConsistencyCheckFilter());
    }

    /** Return wrapped extractor. */
    public RawExtractor getExtractor()
    {
        return extractor;
    }

    /**
     * Extracts a raw event and wraps it in a ReplDBMS complete with sequence
     * number, which increments each time we process the last fragment.
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#extract()
     */
    public ReplDBMSEvent extract() throws ReplicatorException,
            InterruptedException
    {
        DBMSEvent dbmsEvent = extractor.extract();

        if (dbmsEvent == null)
            return null;
        
        // Generate the event.
        Timestamp extractTimestamp = dbmsEvent.getSourceTstamp();
        ReplDBMSEvent replEvent = new ReplDBMSEvent(seqno, fragno,
                dbmsEvent.isLastFrag(), sourceId, epochNumber,
                extractTimestamp, dbmsEvent);
        if (logger.isDebugEnabled())
            logger.debug("Source timestamp = " + dbmsEvent.getSourceTstamp()
                    + " - Extracted timestamp = " + extractTimestamp);

        for (Filter filter : autoFilters)
        {
            try
            {
                replEvent = filter.filter(replEvent);
                if (replEvent == null)
                    return null;
            }
            catch (ReplicatorException e)
            {
                throw new ExtractorException(
                        "Auto-filter operation failed unexpectedly: "
                                + e.getMessage(), e);
            }
        }

        // See if this is the last fragment.
        if (dbmsEvent.isLastFrag())
        {
            seqno++;
            fragno = 0;
        }
        else
            fragno++;

        return replEvent;
    }

    /**
     * Delegates to underlying extractor. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#getCurrentResourceEventId()
     */
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        return extractor.getCurrentResourceEventId();
    }

    /**
     * Returns false until we implement caching.
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#hasMoreEvents()
     */
    public boolean hasMoreEvents()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEvent(com.continuent.tungsten.replicator.event.ReplDBMSHeader)
     */
    public void setLastEvent(ReplDBMSHeader header) throws ReplicatorException
    {
        String eventId;

        // Figure out if this is a failover and we need to reset the
        // event numbering.
        if (header == null)
        {
            // No previously stored event. Start from scratch.
            seqno = 0;
            eventId = null;
        }
        else if (sourceId.equals(header.getSourceId()))
        {
            // Continuing local extraction. Ask for next event.
            if (logger.isDebugEnabled())
                logger.debug("Source ID of max event verified: "
                        + header.getSourceId());
            seqno = header.getSeqno() + 1;
            eventId = header.getEventId();
        }
        else
        {
            // Master source ID has shifted; remember seqno but start local
            // extraction from scratch.
            logger.info("Local source ID differs from last stored source ID: local="
                    + sourceId + " stored=" + header.getSourceId());
            logger.info("Restarting replication from scratch");

            seqno = header.getSeqno() + 1;
            eventId = null;
        }

        // See if we have an override on the seqno. That takes priority over
        // any previous value.
        if (pluginContext.getOnlineOptions().get(
                OpenReplicatorParams.BASE_SEQNO) != null)
        {
            overrideBaseSeqno();
        }

        // Tell the extractor.
        setLastEventId(eventId);
        epochNumber = seqno;
    }
    
    // Override base sequence number if different from current base. 
    private void overrideBaseSeqno()
    {
        long newBaseSeqno = pluginContext.getOnlineOptions().getLong(
                OpenReplicatorParams.BASE_SEQNO) + 1;
        if (newBaseSeqno != seqno)
        {
            seqno = newBaseSeqno;
            logger.info("Overriding base sequence number; next seqno will be: "
                    + seqno);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEventId(java.lang.String)
     */
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        extractor.setLastEventId(eventId);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        logger.info("Configuring raw extractor and heartbeat filter");
        this.pluginContext = context;
        sourceId = context.getSourceId();
        extractor.configure(pluginContext);
        for (Filter filter : autoFilters)
            filter.configure(pluginContext);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Prepare sub-components.
        logger.info("Preparing raw extractor and heartbeat filter");
        extractor.prepare(context);
        for (Filter filter : autoFilters)
            filter.prepare(context);

        // See if we have an online option that overrides the initial seqno.
        if (pluginContext.getOnlineOptions().get(
                OpenReplicatorParams.BASE_SEQNO) != null)
        {
            overrideBaseSeqno();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        logger.info("Releasing raw extractor and heartbeat filter");
        extractor.release(context);
        for (Filter filter : autoFilters)
            filter.release(context);
    }
}

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
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.thl;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.cluster.resource.OpenReplicatorParams;
import com.continuent.tungsten.replicator.InSequenceNotification;
import com.continuent.tungsten.replicator.OutOfSequenceNotification;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.extractor.Extractor;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.plugin.PluginLoader;

/**
 * Implements an extractor to pull events from a remote THL.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class RemoteTHLExtractor implements Extractor
{
    private static Logger  logger             = Logger.getLogger(RemoteTHLExtractor.class);

    // Properties.
    private String         connectUri;
    private int            resetPeriod        = 1;
    private boolean        checkSerialization = true;
    private int            heartbeatMillis    = 3000;

    // Connection control variables.
    private PluginContext  pluginContext;
    private ReplDBMSHeader lastEvent;
    private Connector      conn;

    /**
     * Create Connector instance.
     */
    public RemoteTHLExtractor()
    {
    }

    public String getConnectUri()
    {
        return connectUri;
    }

    /**
     * Set the URI of the store to which we connect.
     * 
     * @param connectUri
     */
    public void setConnectUri(String connectUri)
    {
        this.connectUri = connectUri;
    }

    public int getResetPeriod()
    {
        return resetPeriod;
    }

    /**
     * Set the number of iterations before resetting the communications stream.
     * Higher values use more memory but are more efficient.
     */
    public void setResetPeriod(int resetPeriod)
    {
        this.resetPeriod = resetPeriod;
    }

    public boolean isCheckSerialization()
    {
        return checkSerialization;
    }

    /**
     * If true, check epoch number and sequence number of last event we have
     * received.
     * 
     * @param checkSerialization
     */
    public void setCheckSerialization(boolean checkSerialization)
    {
        this.checkSerialization = checkSerialization;
    }

    public int getHeartbeatInterval()
    {
        return heartbeatMillis;
    }

    /**
     * Sets the interval for sending heartbeat events from server to avoid
     * TCP/IP timeout on server connection.
     */
    public void setHeartbeatInterval(int heartbeatMillis)
    {
        this.heartbeatMillis = heartbeatMillis;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#extract()
     */
    public ReplDBMSEvent extract() throws ReplicatorException,
            InterruptedException
    {
        // Open the connector if it is not yet open.
        try
        {
            if (conn == null)
                openConnector();

            // Fetch the event.
            ReplEvent replEvent = null;
            while (replEvent == null)
            {
                long seqno = 0;
                try
                {
                    if (lastEvent != null)
                        if (lastEvent.getLastFrag())
                        {
                            if (lastEvent instanceof ReplDBMSFilteredEvent)
                            {
                                ReplDBMSFilteredEvent ev = (ReplDBMSFilteredEvent) lastEvent;
                                seqno = ev.getSeqnoEnd() + 1;
                            }
                            else
                                seqno = lastEvent.getSeqno() + 1;
                        }
                        else
                            seqno = lastEvent.getSeqno();
                    replEvent = conn.requestEvent(seqno);
                }
                catch (IOException e)
                {
                    reconnect();
                    // If the connection dropped in the middle of a fragmented
                    // transaction, we need to ignore events that were already
                    // stored, otherwise it will generate an integrity
                    // constraint violation
                    continue;
                }

                // Ensure we have the right *sort* of replication event.
                if (replEvent != null && !(replEvent instanceof ReplDBMSEvent))
                    throw new ExtractorException(
                            "Unexpected event type: seqno =" + seqno + " type="
                                    + replEvent.getClass().getName());
            }

            // Remember which event we just read and ask for the next one.
            lastEvent = (ReplDBMSEvent) replEvent;
            return (ReplDBMSEvent) replEvent;
        }
        catch (THLException e)
        {
            // THLException messages are user-readable so just pass 'em along.
            throw new ExtractorException(e.getMessage(), e);
        }

    }

    /** Does not make sense for this extractor type. */
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        return null;
    }

    public boolean hasMoreEvents()
    {
        return false;
    }

    public void setLastEvent(ReplDBMSHeader event) throws ReplicatorException
    {
        lastEvent = event;
    }

    /**
     * Ignored for now as this extractor is not for a data source. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEventId(java.lang.String)
     */
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        logger.warn("Attempt to set last event ID on remote THL extractor: "
                + eventId);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        // Store context for later.
        this.pluginContext = context;

        // Set the connect URI to a default if not already set.
        if (this.connectUri == null)
        {
            connectUri = context.getReplicatorProperties().get(
                    ReplicatorConf.MASTER_CONNECT_URI);
        }

        // See if we have an online option that overrides serialization
        // checking.
        if (pluginContext.getOnlineOptions().getBoolean(
                OpenReplicatorParams.FORCE))
        {
            if (checkSerialization)
            {
                logger.info("Force option enabled; log serialization checking is disabled");
                checkSerialization = false;
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        if (conn != null)
        {
            conn.close();
            try
            {
                conn.release(context);
            }
            catch (InterruptedException e)
            {
            }
        }
    }

    // Reconnect a failed connection.
    private void reconnect() throws InterruptedException, ReplicatorException
    {
        // Reconnect after lost connection.
        logger.info("Connection to remote thl lost; reconnecting");
        pluginContext.getEventDispatcher().put(new OutOfSequenceNotification());
        openConnector();
    }

    // Open up the connector here.
    private void openConnector() throws ReplicatorException,
            InterruptedException
    {
        // Connect to remote THL.
        logger.info("Opening connection to master: " + connectUri);
        long retryCount = 0;
        for (;;)
        {
            try
            {
                // If we need to check serialization we must supply the seqno
                // and epoch.
                try
                {
                    conn = (Connector) PluginLoader
                            .load(pluginContext
                                    .getReplicatorProperties()
                                    .getString(
                                            ReplicatorConf.THL_PROTOCOL,
                                            ReplicatorConf.THL_PROTOCOL_DEFAULT,
                                            false));
                    conn.setURI(connectUri);
                    conn.setResetPeriod(resetPeriod);
                    conn.setHeartbeatMillis(heartbeatMillis);
                    if (this.lastEvent == null
                            || this.checkSerialization == false)
                    {
                        conn.setLastSeqno(-1);
                        conn.setLastEpochNumber(-1);
                    }
                    else
                    {
                        conn.setLastSeqno(lastEvent.getSeqno());
                        conn.setLastEpochNumber(lastEvent.getEpochNumber());
                    }
                    conn.configure(pluginContext);
                    conn.prepare(pluginContext);
                }
                catch (ReplicatorException e)
                {
                    throw new THLException("Error while initializing plug-in ",
                            e);
                }

                conn.connect();
                break;
            }
            catch (IOException e)
            {
                // Sleep for 1 second per retry; report every 10 retries.
                if (conn != null)
                {
                    conn.close();
                    conn = null;
                }
                retryCount++;
                if ((retryCount % 10) == 0)
                {
                    logger.info("Waiting for master to become available: uri="
                            + connectUri + " retries=" + retryCount);
                }
                Thread.sleep(1000);
            }
        }

        // Announce the happy event.
        logger.info("Connected to master after " + retryCount + " retries");
        pluginContext.getEventDispatcher().put(new InSequenceNotification());
    }
}

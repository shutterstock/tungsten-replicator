/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011 Continuent Inc.
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
 * Initial developer(s): Stephane Giron, Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.storage.prefetch;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.storage.InMemoryQueueStore;
import com.continuent.tungsten.replicator.thl.CommitSeqnoTable;

/**
 * Implements a specialized store for handling slave prefetch from another
 * replicator. This store coordinates restart at the current slave position and
 * implements logic to drop events that are not far enough ahead of the slave
 * position or have already been executed.
 */
public class PrefetchStore extends InMemoryQueueStore
{
    private static Logger        logger           = Logger.getLogger(PrefetchStore.class);

    // Prefetch store parameters.
    private String               user;
    private String               url;
    private String               password;
    private String               slaveCatalogSchema;

    private long                 interval         = 1000;
    private int                  aheadMaxTime     = 3000;
    private int                  sleepTime        = 500;
    private int                  warmUpEventCount = 100;

    // Database connection information.
    private Database             conn             = null;
    private PreparedStatement    seqnoStatement   = null;

    // Prefetch coordination information.
    private long                 lastChecked      = 0;
    private long                 currentSeqno     = -1;
    private long                 initTime         = 0;
    private Map<Long, Timestamp> appliedTimes;
    private long                 totalEvents      = 0;
    private long                 prefetchEvents   = 0;

    // State information.
    enum PrefetchState
    {
        active, sleeping
    };

    private PrefetchState prefetchState;
    private long          startTimeMillis;
    private long          sleepTimeMillis;
    private long          slaveLatency;
    private long          prefetchLatency;

    /** Sets the JDBC URL to connect to the slave server. */
    public void setUrl(String url)
    {
        this.url = url;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    /** Sets the password of the database login to check slave position. */
    public void setPassword(String password)
    {
        this.password = password;
    }

    /**
     * Sets the catalog schema name of the slave for which we are prefetching.
     */
    public void setSlaveCatalogSchema(String slaveCatalogSchema)
    {
        this.slaveCatalogSchema = slaveCatalogSchema;
    }

    /**
     * Sets the number of milliseconds between slave position checks.
     * 
     * @param timeInMillis
     */
    public void setCheckInterval(long timeInMillis)
    {
        this.interval = timeInMillis;
    }

    /**
     * Sets the aheadMaxTime value. This is the maximum time that event should
     * be from the last applied event (based on master times
     * 
     * @param aheadMaxTime The aheadMaxTime to set.
     */
    public void setAheadMaxTime(int aheadMaxTime)
    {
        this.aheadMaxTime = aheadMaxTime;
    }

    /**
     * Sets the sleepTime value.
     * 
     * @param sleepTime The sleepTime to set.
     */
    public void setSleepTime(int sleepTime)
    {
        this.sleepTime = sleepTime;
    }

    /**
     * Sets the warmUpEventCount value.
     * 
     * @param warmUpEventCount The warmUpEventCount to set.
     */
    public void setWarmUpEventCount(int warmUpEventCount)
    {
        this.warmUpEventCount = warmUpEventCount;
    }

    /** Sets the last header processed. This is required for restart. */
    public void setLastHeader(ReplDBMSHeader header)
    {
        // Ignore last header from downstream stages.
    }

    /**
     * Returns the position of the slave on which we are handling prefetch.
     */
    public ReplDBMSHeader getLastHeader()
    {
        return this.getCurrentSlaveHeader();
    }

    /**
     * Puts an event in the queue, blocking if it is full.
     */
    public void put(ReplDBMSEvent event) throws InterruptedException,
            ReplicatorException
    {
        // See if we want the event and put it in the queue if so.
        if (filter(event) != null)
        {
            super.put(event);
        }
    }

    /**
     * Prepare prefetch store. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        // Perform super-class prepare.
        super.prepare(context);

        logger.info("Preparing PrefetchStore for slave catalog schema: "
                + slaveCatalogSchema);
        // Load defaults for connection
        if (url == null)
            url = context.getJdbcUrl("tungsten_" + context.getServiceName());
        if (user == null)
            user = context.getJdbcUser();
        if (password == null)
            password = context.getJdbcPassword();

        // Connect.
        try
        {
            conn = DatabaseFactory.createDatabase(url, user, password);
            conn.connect();

            seqnoStatement = conn
                    .prepareStatement("select seqno, fragno, last_Frag, source_id, epoch_number, eventid, applied_latency from "
                            + slaveCatalogSchema
                            + "."
                            + CommitSeqnoTable.TABLE_NAME);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }

        // Show that we have started.
        startTimeMillis = System.currentTimeMillis();
        prefetchState = PrefetchState.active;
    }

    /**
     * Release queue. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        queue = null;
        if (conn != null)
        {
            conn.close();
            conn = null;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.Store#status()
     */
    public TungstenProperties status()
    {
        // Get super class properties.
        TungstenProperties props = super.status();

        // Add properties for prefetch.
        props.setString("url", url);
        props.setString("slaveCatalogSchema", slaveCatalogSchema);
        props.setLong("interval", interval);
        props.setLong("aheadMaxTime", aheadMaxTime);
        props.setLong("sleepTime", sleepTime);
        props.setLong("warmUpEventCount", warmUpEventCount);

        // Add runtime properties.
        props.setLong("prefetchEvents", prefetchEvents);
        double prefetchRatio = 0.0;
        if (totalEvents > 0)
            prefetchRatio = ((double) prefetchEvents) / totalEvents;
        props.setString("prefetchRatio", formatDouble(prefetchRatio));
        props.setString("prefetchState", prefetchState.toString());
        props.setString("slaveLatency", formatDouble(slaveLatency));
        props.setString("prefetchLatency",
                formatDouble(prefetchLatency / 1000.0));
        props.setString("prefetchTimeAhead", formatDouble(slaveLatency
                - (prefetchLatency / 1000.0)));
        props.setString("prefetchState", prefetchState.toString());

        long duration = System.currentTimeMillis() - startTimeMillis;
        double timeActive = (duration - sleepTimeMillis) / 1000.0;
        double timeSleeping = sleepTimeMillis / 1000.0;
        props.setString("timeActive", formatDouble(timeActive));
        props.setString("timeSleeping", formatDouble(timeSleeping));

        return props;
    }

    // Format double values to 3 decimal places.
    private String formatDouble(double d)
    {
        return String.format("%-15.3f", d);
    }

    /**
     * Filter the event if it has already been executed.
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        totalEvents++;

        if (appliedTimes == null)
            appliedTimes = new TreeMap<Long, Timestamp>();

        Timestamp sourceTstamp = event.getDBMSEvent().getSourceTstamp();
        appliedTimes.put(event.getSeqno(), sourceTstamp);

        long currentTime = System.currentTimeMillis();
        prefetchLatency = currentTime - sourceTstamp.getTime();

        if (interval == 0 || lastChecked == 0
                || (currentTime - lastChecked >= interval))
        {
            // It is now time to check CommitSeqnoTable again
            checkSlavePosition(currentTime);
        }

        if (initTime == 0)
            initTime = sourceTstamp.getTime();

        if (event.getSeqno() <= currentSeqno)
        {
            if (logger.isDebugEnabled())
                logger.debug("Discarding event " + event.getSeqno()
                        + " as it is already applied");
            return null;
        }
        else
            while (sourceTstamp.getTime() - initTime > aheadMaxTime)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Event is too far ahead of current slave position... sleeping");
                // this event is too far ahead of the CommitSeqnoTable position:
                // sleep some time and continue
                long sleepStartMillis = System.currentTimeMillis();
                try
                {

                    prefetchState = PrefetchState.sleeping;
                    Thread.sleep(sleepTime);
                }
                catch (InterruptedException e)
                {
                    return null;
                }
                finally
                {
                    prefetchState = PrefetchState.active;
                    sleepTimeMillis += (System.currentTimeMillis() - sleepStartMillis);
                }
                // Check again CommitSeqnoTable
                checkSlavePosition(System.currentTimeMillis());
                // and whereas the event got applied while sleeping
                if (event.getSeqno() <= currentSeqno)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Discarding event " + event.getSeqno()
                                + " as it is already applied");
                    return null;
                }
            }

        prefetchEvents++;
        if (logger.isDebugEnabled() && totalEvents % 20000 == 0)
            logger.debug("Prefetched " + prefetchEvents + " events - Ratio "
                    + (100 * prefetchEvents / totalEvents) + "%");
        return event;
    }

    /**
     * Check slave position.
     */
    private void checkSlavePosition(long currentTime)
    {
        lastChecked = currentTime;
        ReplDBMSHeader header = getCurrentSlaveHeader();
        if (currentSeqno == -1)
            currentSeqno = header.getSeqno() + warmUpEventCount;
        else
            currentSeqno = header.getSeqno();

        // Drop every appliedTimes prior to currentSeqno and update time
        // accordingly (time from max known applied event)
        for (Iterator<Entry<Long, Timestamp>> iterator = appliedTimes
                .entrySet().iterator(); iterator.hasNext();)
        {
            Entry<Long, Timestamp> next = iterator.next();
            if (next.getKey() > currentSeqno)
            {
                break;
            }

            long time = next.getValue().getTime();
            initTime = time;

            if (next.getKey() < currentSeqno)
            {
                iterator.remove();
            }
            else
                break;
        }
    }

    // Fetch position data from slave.
    private ReplDBMSHeaderData getCurrentSlaveHeader()
    {
        ReplDBMSHeaderData header = null;
        ResultSet rs = null;
        try
        {
            rs = seqnoStatement.executeQuery();
            if (rs.next())
            {
                // Construct header data
                long seqno = rs.getLong("seqno");
                short fragno = rs.getShort("fragno");
                boolean lastFrag = rs.getBoolean("last_frag");
                String sourceId = rs.getString("source_id");
                long epochNumber = rs.getLong("epoch_number");
                String eventId = rs.getString("eventid");
                header = new ReplDBMSHeaderData(seqno, fragno, lastFrag,
                        sourceId, epochNumber, eventId, null, new Timestamp(0));

                // Record current slave latency.
                this.slaveLatency = rs.getLong("applied_latency");
            }
        }
        catch (SQLException e)
        {
            logger.warn(e);
        }
        finally
        {
            if (rs != null)
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                }
        }
        return header;
    }
}

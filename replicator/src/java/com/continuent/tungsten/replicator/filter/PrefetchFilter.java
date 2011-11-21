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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.filter;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.thl.CommitSeqnoTable;

/**
 * This class defines a PrefetchFilter
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class PrefetchFilter implements Filter
{
    private static Logger        logger           = Logger.getLogger(PrefetchFilter.class);

    private Database             conn             = null;
    private PreparedStatement    seqnoStatement   = null;

    private String               user;
    private String               url;
    private String               password;

    private long                 interval         = 1000;
    private int                  aheadMaxTime     = 3000;
    private int                  sleepTime        = 500;
    private int                  warmUpEventCount = 100;

    private long                 lastChecked      = 0;
    private long                 currentSeqno     = -1;
    private long                 initTime         = 0;

    private Map<Long, Timestamp> appliedTimes;

    private long                 totalEvents      = 0;
    private long                 prefetchEvents   = 0;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        logger.info("Preparing PrefetchFilter");
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
                    .prepareStatement("select seqno, fragno, last_Frag, source_id, epoch_number, eventid from "
                            + context.getReplicatorSchemaName()
                            + "."
                            + CommitSeqnoTable.TABLE_NAME);

        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }
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
            conn = null;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
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
                try
                {
                    Thread.sleep(sleepTime);
                }
                catch (InterruptedException e)
                {
                    return null;
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
     * TODO: checkSlavePosition definition.
     * 
     * @param currentTime
     */
    private void checkSlavePosition(long currentTime)
    {
        lastChecked = currentTime;
        if (currentSeqno == -1)
            currentSeqno = getCurrentSeqno() + warmUpEventCount;
        else
            currentSeqno = getCurrentSeqno();

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

    public void setUser(String user)
    {
        this.user = user;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

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

    private long getCurrentSeqno()
    {
        long seqno = -1;
        ResultSet rs = null;
        try
        {
            rs = seqnoStatement.executeQuery();
            if (rs.next())
            {
                seqno = rs.getLong("seqno");
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
        return seqno;
    }
}

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

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * This class defines a MySQLTHLStorage
 *
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class JdbcTHLStorage implements THLStorage
{
    private static Logger       logger            = Logger.getLogger(JdbcTHLStorage.class);
    private ReplicatorRuntime   runtime           = null;
    protected String            driver            = null;
    protected String            vendor            = null;
    protected String            url               = null;
    protected String            user              = null;
    protected String            password          = null;
    protected String            metadataSchema    = null;
    protected boolean           storeLocalData    = true;
    private static final String storeLocalDataKey = JdbcTHLStorage.class
                                                          .getName()
                                                          + ".storeLocalData";
    private JdbcTHLDatabase     database          = null;

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.thl.THLStorage#setPassword(java.lang.String)
     */
    public void setPassword(String password)
    {
        this.password = password;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.thl.THLStorage#setUrl(java.lang.String)
     */
    public void setUrl(String url)
    {
        this.url = url;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.thl.THLStorage#setUser(java.lang.String)
     */
    public void setUser(String user)
    {
        this.user = user;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.thl.THLStorage#find(long)
     */
    public THLEvent find(long seqno) throws THLException
    {
        return database.find(seqno);
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.thl.THLStorage#find(long, short)
     */
    public THLEvent find(long seqno, short fragno) throws THLException
    {
        return database.find(seqno, fragno);
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getEventId(long)
     */
    public String getEventId(long seqno) throws THLException
    {
        return database.getEventId(seqno, null);
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getMaxEventId(String)
     */
    public String getMaxEventId(String sourceId) throws THLException
    {
        return database.getMaxEventId(sourceId);
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getMaxSeqno()
     */
    public long getMaxSeqno() throws THLException
    {
        return database.getMaxSeqno();
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getMaxCompletedSeqno()
     */
    public long getMaxCompletedSeqno() throws THLException
    {
        return database.getMaxCompletedSeqno();
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getMinSeqno()
     */
    public long getMinSeqno() throws THLException
    {
        return database.getMinSeqno();
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getMinMaxSeqno()
     */
    public long[] getMinMaxSeqno() throws THLException
    {
        return database.getMinMaxSeqno();
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.thl.THLStorage#store(com.continuent.tungsten.replicator.thl.THLEvent,
     *      boolean)
     */
    public void store(THLEvent event, boolean syncCommitSeqno)
            throws THLException
    {
        database.store(event, syncCommitSeqno);
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.thl.THLStorage#setStatus(long,
     *      short, short, java.lang.String)
     */
    public void setStatus(long seqno, short fragno, short status, String msg)
            throws THLException
    {
        database.setStatus(seqno, fragno, status, msg);
    }

    /**
     * Prepare the THL storage for use.
     *
     * @param context Plugin context for THL to which this instance connects
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        TungstenProperties conf = context.getReplicatorProperties();

        this.runtime = (ReplicatorRuntime) context;

        if (vendor == null)
            vendor = conf.getString(ReplicatorConf.RESOURCE_VENDOR);
        if (url == null)
            url = conf.getString(ReplicatorConf.THL_DB_URL);
        if (user == null)
            user = conf.getString(ReplicatorConf.THL_DB_USER);
        if (password == null)
            password = conf.getString(ReplicatorConf.THL_DB_PASSWORD);

        // A null metadata schema value is bad so don't let it happen. We
        // may want to consider omitting this in future and always writing only
        // to the URL. However that means changes to Table handling.
        metadataSchema = conf.getString(ReplicatorConf.METADATA_SCHEMA, null,
                true);
        storeLocalData = conf.getBoolean(storeLocalDataKey, "true", false);
        if (storeLocalData == false)
            logger.info("Disabled local data storing");

        database = new JdbcTHLDatabase(runtime, driver);
        database.connect(url, user, password, metadataSchema, vendor);
        database.prepareSchema();
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(PluginContext)
     */
    public void release() throws ReplicatorException
    {
        if (database != null)
        {
            database.close();
            database = null;
        }
    }

    public void updateFailedStatus(THLEventStatus failedEvent,
            ArrayList<THLEventStatus> events) throws THLException
    {
        database.updateFailedStatus(failedEvent, events);
    }

    public void updateSuccessStatus(ArrayList<THLEventStatus> succeededEvents,
            ArrayList<THLEventStatus> skippedEvents) throws THLException
    {
        database.updateSuccessStatus(succeededEvents, skippedEvents);
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getMaxFragno(long)
     */
    public short getMaxFragno(long seqno) throws THLException
    {
        return database.getMaxFragno(seqno);
    }

    public THLBinaryEvent findBinaryEvent(long seqno, short fragno)
            throws THLException
    {
        throw new THLException("Not yet implemented");
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.thl.THLStorage#delete(Long, Long,
     *      String)
     */
    public int delete(Long low, Long high, String before) throws THLException,
            InterruptedException
    {
        return database.delete(low, high, before);
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getLastAppliedEvent()
     */
    public ReplDBMSHeader getLastAppliedEvent() throws THLException
    {
        return database.getLastEvent();
    }
}

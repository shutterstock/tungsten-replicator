/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2009 Continuent Inc.
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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.filter;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.patterns.order.HighWaterResource;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Filter to support session specific temp tables and variables
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class MySQLSessionSupportFilter implements Filter
{
    private static Logger       logger                = Logger.getLogger(LoggingFilter.class);

    private String              lastSessionId         = "";
    private static final String SET_PTHREAD_STATEMENT = "set @@session.pseudo_thread_id=";

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException
    {
        String eventId = event.getEventId();
        String sessionId = HighWaterResource.getSessionId(eventId);

        if (sessionId == null && logger.isDebugEnabled())
            logger.debug(String.format("Found null sessionId for eventId=%s",
                    eventId));

        if (sessionId != null && !sessionId.equals(lastSessionId))
        {
            ArrayList<DBMSData> data = event.getData();
            if (data != null)
            {
                StatementData ins = new StatementData(SET_PTHREAD_STATEMENT
                        + sessionId);
                data.add(0, ins);
                lastSessionId = sessionId;
                if (logger.isDebugEnabled())
                    logger.debug(String.format("%s%s for eventId=%s",
                            SET_PTHREAD_STATEMENT, sessionId, eventId));
            }
        }
        return event;
    }

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
        if (logger.isDebugEnabled())
            logger.debug(String.format("Filter %s loaded successfully",
                    getClass().getSimpleName()));
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
    }
}

/**
 * Tungsten: An Application Server for uni/cluster.
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
 * Initial developer(s): Marcus Eriksson
 * INITIAL CODE DONATED UNDER TUNGSTEN CODE CONTRIBUTION AGREEMENT
 */

package com.continuent.tungsten.replicator.applier;

import java.sql.SQLException;
import java.sql.SQLWarning;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Stub applier class that automatically constructs url from Drizzle-specific
 * properties like host, port, and service.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @author Marcus Eriksson
 * @version 1.0
 */
public class DrizzleApplier extends JdbcApplier
{
    private static Logger logger     = Logger.getLogger(DrizzleApplier.class);

    protected String      host       = "localhost";
    protected int         port       = 4427;
    protected String      urlOptions = null;

    /**
     * Host name or IP address.
     */
    public void setHost(String host)
    {
        this.host = host;
    }

    /**
     * TCP/IP port number, a positive integer.
     */
    public void setPort(String portAsString)
    {
        this.port = Integer.parseInt(portAsString);
    }

    /**
     * JDBC URL options with a leading ?.
     */
    public void setUrlOptions(String urlOptions)
    {
        this.urlOptions = urlOptions;
    }

    /**
     * Generate URL suitable for Drizzle and then delegate remaining
     * configuration to superclass.
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext
     *      context)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        if (url == null)
        {
            StringBuffer sb = new StringBuffer();
            sb.append("jdbc:drizzle://");
            sb.append(host);
            if (port > 0)
            {
                sb.append(":");
                sb.append(port);
            }
            sb.append("/");
            if (urlOptions != null)
                sb.append(urlOptions);

            url = sb.toString();
        }
        else if (logger.isDebugEnabled())
            logger.debug("Property url already set; ignoring host and port properties");
        super.configure(context);
    }

    protected void applyRowIdData(RowIdData data) throws ReplicatorException
    {
        String query = "SET INSERT_ID = " + data.getRowId();
        try
        {
            try
            {
                statement.execute(query);
            }
            catch (SQLWarning e)
            {
                String msg = "While applying SQL event:\n" + data.toString()
                        + "\nWarning: " + e.getMessage();
                logger.warn(msg);
            }
            statement.clearBatch();

            if (logger.isDebugEnabled())
            {
                logger.debug("Applied event: " + query);
            }
        }
        catch (SQLException e)
        {
            logFailedStatementSQL(query, e);
            throw new ApplierException(e);
        }
    }

}
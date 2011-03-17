/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2008 Continuent Inc.
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
 * Initial developer(s): Linas Virbalas
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.applier;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.database.DBMS;
import com.continuent.tungsten.replicator.database.JdbcURL;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.plugin.PluginContext;

public class PostgreSQLApplier extends JdbcApplier
{
    private static Logger logger  = Logger.getLogger(PostgreSQLApplier.class);

    protected String      host    = "localhost";
    protected int         port    = 5432;

    public void setHost(String host)
    {
        this.host = host;
    }

    public void setPort(String portAsString)
    {
        this.port = Integer.parseInt(portAsString);
    }

    /**
     * Generate URL suitable for PostgreSQL and then delegate remaining configuration
     * to superclass.
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(PluginContext
     *      context)
     */
    public void configure(PluginContext context) throws ApplierException
    {
        if (url == null)
        {
            url = JdbcURL.generate(DBMS.POSTGRESQL, host, port, null);
        }
        else
            logger
                    .info("Property url already set; ignoring host, port, and service properties");

        super.configure(context);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.JdbcApplier#setObject(java.sql.PreparedStatement,
     *      int, com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal,
     *      com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec)
     */
    @Override
    protected void setObject(PreparedStatement prepStatement, int bindLoc,
            ColumnVal value, ColumnSpec columnSpec) throws SQLException
    {
        // TODO: add type conversions, if needed.
        prepStatement.setObject(bindLoc, value.getValue());
    }
}
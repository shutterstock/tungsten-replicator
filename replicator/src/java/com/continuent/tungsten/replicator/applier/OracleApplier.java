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
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.applier;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Types;
import java.sql.Timestamp;
import java.io.Writer;

import oracle.jdbc.OraclePreparedStatement;
import oracle.sql.CLOB;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.database.DBMS;
import com.continuent.tungsten.replicator.database.AdditionalTypes;
import com.continuent.tungsten.replicator.database.JdbcURL;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.plugin.PluginContext;

public class OracleApplier extends JdbcApplier
{
    private static Logger logger  = Logger.getLogger(OracleApplier.class);

    protected String      host    = "localhost";
    protected int         port    = 1521;
    protected String      service = "ORCL";

    public void setHost(String host)
    {
        this.host = host;
    }

    public void setPort(String portAsString)
    {
        this.port = Integer.parseInt(portAsString);
    }

    public void setService(String service)
    {
        this.service = service;
    }

    /**
     * Generate URL suitable for MySQL and then delegate remaining configuration
     * to superclass.
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(PluginContext
     *      context)
     */
    public void configure(PluginContext context) throws ApplierException
    {
        if (url == null)
        {
            url = JdbcURL.generate(DBMS.ORACLE, host, port, service);
        }
        else
            logger
                    .info("Property url already set; ignoring host, port, and service properties");

        super.configure(context);
    }

    private CLOB getCLOB(String xmlData) throws SQLException
    {
        CLOB tempClob = null;
        Connection dbConn = conn.getConnection();
        try
        {
            // If the temporary CLOB has not yet been created, create new
            tempClob = CLOB
                    .createTemporary(dbConn, true, CLOB.DURATION_SESSION);

            // Open the temporary CLOB in readwrite mode to enable writing
            tempClob.open(CLOB.MODE_READWRITE);
            // Get the output stream to write
            // Writer tempClobWriter = tempClob.getCharacterOutputStream();
            Writer tempClobWriter = tempClob.setCharacterStream(0);
            // Write the data into the temporary CLOB
            tempClobWriter.write(xmlData);

            // Flush and close the stream
            tempClobWriter.flush();
            tempClobWriter.close();

            // Close the temporary CLOB
            tempClob.close();
        }
        catch (SQLException sqlexp)
        {
            tempClob.freeTemporary();
            sqlexp.printStackTrace();
        }
        catch (Exception exp)
        {
            tempClob.freeTemporary();
            exp.printStackTrace();
        }
        return tempClob;
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
        int type = columnSpec.getType();
        if (value.getValue() == null)
            prepStatement.setNull(bindLoc, type);
        else if (type == Types.FLOAT)
            ((OraclePreparedStatement) prepStatement).setBinaryFloat(bindLoc,
                    ((Float) value.getValue()).floatValue());
        else if (type == Types.DOUBLE)
            ((OraclePreparedStatement) prepStatement).setBinaryDouble(bindLoc,
                    ((Double) value.getValue()).doubleValue());
        else if (type == AdditionalTypes.XML)
        {
            CLOB clob = getCLOB((String) (value.getValue()));
            ((OraclePreparedStatement) prepStatement).setObject(bindLoc, clob);
        }
        else if (type == Types.DATE)
        {
            Timestamp ts = new Timestamp((Long)(value.getValue()));
            ((OraclePreparedStatement) prepStatement).setObject(bindLoc, ts);
        }
        else
            prepStatement.setObject(bindLoc, value.getValue());
    }
}
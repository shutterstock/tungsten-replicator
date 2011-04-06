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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.applier;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Types;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.jmx.JmxManager;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.dbms.LoadDataFileQuery;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.event.ReplOption;
import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Stub applier class that automatically constructs url from Oracle-specific
 * properties like host, port, and service.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class MySQLApplier extends JdbcApplier
{
    private static Logger             logger              = Logger.getLogger(MySQLApplier.class);

    private static final int          TINYINT_MAX_VALUE   = 255;
    private static final int          SMALLINT_MAX_VALUE  = 65535;
    private static final int          MEDIUMINT_MAX_VALUE = 16777215;
    private static final long         INTEGER_MAX_VALUE   = 4294967295L;
    protected static final BigInteger BIGINT_MAX_VALUE    = new BigInteger(
                                                                  "18446744073709551615");

    protected String                  host                = "localhost";
    protected int                     port                = 3306;
    protected String                  urlOptions          = null;

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
            StringBuffer sb = new StringBuffer();
            sb.append("jdbc:mysql://");
            sb.append(host);
            if (port > 0)
            {
                sb.append(":");
                sb.append(port);
            }
            sb.append("/");
            if (context.getReplicatorSchemaName() != null)
                sb.append(context.getReplicatorSchemaName());
            if (urlOptions != null)
                sb.append(urlOptions);

            url = sb.toString();
        }
        else
            logger.info("Property url already set; ignoring host and port properties");
        super.configure(context);
    }

    protected void applyRowIdData(RowIdData data) throws ApplierException
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
            logFailedStatementSQL(query);
            throw new ApplierException(e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.JdbcApplier#addColumn(java.sql.ResultSet,
     *      java.lang.String)
     */
    @Override
    protected Column addColumn(ResultSet rs, String columnName)
            throws SQLException
    {
        String typeDesc = rs.getString("TYPE_NAME").toUpperCase();
        boolean isSigned = !typeDesc.contains("UNSIGNED");
        int dataType = rs.getInt("DATA_TYPE");

        if (logger.isDebugEnabled())
            logger.debug("Adding column " + columnName + " (TYPE " + dataType
                    + " - " + (isSigned ? "SIGNED" : "UNSIGNED") + ")");

        Column column = new Column(columnName, dataType, false, isSigned);
        column.setTypeDescription(typeDesc);
        return column;
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
        if (type == Types.INTEGER)
        {
            boolean isNegative = false;
            Object valToInsert = null;
            Long extractedVal = null;
            if (value.getValue() instanceof Integer)
            {
                int val = (Integer) value.getValue();
                isNegative = val < 0;
                extractedVal = Long.valueOf(val);
            }
            else if (value.getValue() instanceof Long)
            {
                long val = (Long) value.getValue();
                isNegative = val < 0;
                extractedVal = Long.valueOf(val);
            }

            if (columnSpec.isUnsigned() && isNegative)
            {
                switch (columnSpec.getLength())
                {
                    case 1 :
                        valToInsert = TINYINT_MAX_VALUE + 1 + extractedVal;
                        logger.debug("Inserting " + valToInsert);
                        break;
                    case 2 :
                        valToInsert = SMALLINT_MAX_VALUE + 1 + extractedVal;
                        break;
                    case 3 :
                        valToInsert = MEDIUMINT_MAX_VALUE + 1 + extractedVal;
                        break;
                    case 4 :
                        valToInsert = INTEGER_MAX_VALUE + 1 + extractedVal;
                        break;
                    case 8 :
                        valToInsert = BIGINT_MAX_VALUE.add(BigInteger
                                .valueOf(1 + extractedVal));
                        break;
                    default :
                        break;
                }
                setInteger(prepStatement, bindLoc, valToInsert);
            }
            else
                prepStatement.setObject(bindLoc, value.getValue());
        }
        else if (type == java.sql.Types.BLOB
                && value.getValue() instanceof SerialBlob)
        {
            SerialBlob val = (SerialBlob) value.getValue();
            prepStatement
                    .setBytes(bindLoc, val.getBytes(1, (int) val.length()));
        }
        else
            prepStatement.setObject(bindLoc, value.getValue());
    }

    protected void setInteger(PreparedStatement prepStatement, int bindLoc,
            Object valToInsert) throws SQLException
    {
        prepStatement.setObject(bindLoc, valToInsert);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.JdbcApplier#applyStatementData(com.continuent.tungsten.replicator.dbms.LoadDataFileQuery,
     *      java.io.File)
     */
    @Override
    protected void applyLoadDataLocal(LoadDataFileQuery data, File temporaryFile)
            throws ApplierException
    {
        try
        {
            int[] updateCount;
            String schema = data.getDefaultSchema();
            Long timestamp = data.getTimestamp();
            List<ReplOption> options = data.getOptions();

            applyUseSchema(schema);

            applySetTimestamp(timestamp);

            applySessionVariables(options);

            try
            {
                updateCount = statement.executeBatch();
            }
            catch (SQLWarning e)
            {
                String msg = "While applying SQL event:\n" + data.toString()
                        + "\nWarning: " + e.getMessage();
                logger.warn(msg);
                updateCount = new int[1];
                updateCount[0] = statement.getUpdateCount();
            }
            statement.clearBatch();
        }
        catch (SQLException e)
        {
            logFailedStatementSQL(data.getQuery());
            throw new ApplierException(e);
        }

        try
        {
            FileInputStream fis = new FileInputStream(temporaryFile);
            ((com.mysql.jdbc.Statement) statement)
                    .setLocalInfileInputStream(fis);

            int cnt = statement.executeUpdate(data.getQuery());

            if (logger.isDebugEnabled())
                logger.debug("Applied event (update count " + cnt + "): "
                        + data.toString());
        }
        catch (SQLException e)
        {
            logFailedStatementSQL(data.getQuery());
            throw new ApplierException(e);
        }
        catch (FileNotFoundException e)
        {
            logFailedStatementSQL(data.getQuery());
            throw new ApplierException(e);
        }
        finally
        {
            ((com.mysql.jdbc.Statement) statement)
                    .setLocalInfileInputStream(null);
        }
    }

    protected String hexdump(byte[] buffer)
    {
        StringBuffer dump = new StringBuffer();
        for (int i = 0; i < buffer.length; i++)
        {
            dump.append(String.format("%02x", buffer[i]));
        }

        return dump.toString();
    }

}
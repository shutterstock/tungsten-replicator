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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.applier;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Types;
import java.util.List;

import org.apache.log4j.Logger;
import org.drizzle.jdbc.DrizzleStatement;

import com.continuent.tungsten.replicator.dbms.LoadDataFileQuery;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplOption;
import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class MySQLDrizzleApplier extends MySQLApplier
{

    static Logger   logger        = Logger.getLogger(MySQLDrizzleApplier.class);

    private boolean alreadyLogged = false;

    @Override
    public void configure(PluginContext context) throws ApplierException
    {
        if (url == null)
        {
            StringBuffer sb = new StringBuffer();
            sb.append("jdbc:mysql:thin://");
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
        else
            logger.info("Property url already set; ignoring host and port properties");
        super.configure(context);
    }

    @Override
    protected void applyStatementData(StatementData data)
            throws ApplierException
    {
        if (!(statement instanceof DrizzleStatement))
        {
            // Check if the right driver is in use
            if (!alreadyLogged)
                logger.warn("Using MySQLDrizzleApplier with the wrong driver."
                        + " Check the driver.");
            super.applyStatementData(data);
            return;
        }
        else if (data.getQueryAsBytes() == null)
        {
            // Use the old code path if the new one is not required
            super.applyStatementData(data);
            return;
        }

        DrizzleStatement drizzleStatement = (DrizzleStatement) statement;

        try
        {
            int[] updateCount = null;
            String schema = data.getDefaultSchema();
            Long timestamp = data.getTimestamp();
            List<ReplOption> options = data.getOptions();

            applyUseSchema(schema);

            applySetTimestamp(timestamp);

            applySessionVariables(options);

            // Using drizzle driver specific method to send bytes directly to
            // mysql
            drizzleStatement.addBatch(data.getQueryAsBytes());

            try
            {
                updateCount = drizzleStatement.executeBatch();
            }
            catch (SQLWarning e)
            {
                String msg = "While applying SQL event:\n" + data.toString()
                        + "\nWarning: " + e.getMessage();
                logger.warn(msg);
                updateCount = new int[1];
                updateCount[0] = drizzleStatement.getUpdateCount();
            }
            catch (SQLException e)
            {
                if (data.getErrorCode() == 0)
                {
                    SQLException sqlException = new SQLException(
                            "Statement failed on slave but succeeded on master");
                    sqlException.initCause(e);
                    throw sqlException;
                }
                // Check if the query produced the same error on master
                if (e.getErrorCode() == data.getErrorCode())
                {
                    logger.info("Ignoring statement failure as it also failed "
                            + "on master with the same error code ("
                            + e.getErrorCode() + ")");
                }
                else
                {
                    SQLException sqlException = new SQLException(
                            "Statement failed on slave with error code "
                                    + e.getErrorCode()
                                    + " but failed on master with a different one ("
                                    + data.getErrorCode() + ")");
                    sqlException.initCause(e);
                    throw sqlException;
                }
            }

            while (drizzleStatement.getMoreResults())
            {
                drizzleStatement.getResultSet();
            }
            drizzleStatement.clearBatch();

            if (logger.isDebugEnabled() && updateCount != null)
            {
                int cnt = 0;
                for (int i = 0; i < updateCount.length; cnt += updateCount[i], i++)
                    ;

                logger.debug("Applied event (update count " + cnt + "): "
                        + data.toString());
            }
        }
        catch (SQLException e)
        {
            // logFailedStatementSQL(data.getQuery());
            throw new ApplierException(e);
        }

    }

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
            ((DrizzleStatement) statement).setLocalInfileInputStream(fis);

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
            ((DrizzleStatement) statement).setLocalInfileInputStream(null);
        }
    }

    @Override
    protected void setInteger(PreparedStatement prepStatement, int bindLoc,
            Object valToInsert) throws SQLException
    {
        if (valToInsert instanceof BigInteger)
            prepStatement.setString(bindLoc, valToInsert.toString());
        else
            super.setInteger(prepStatement, bindLoc, valToInsert);
    }

    @Override
    protected void setObject(PreparedStatement prepStatement, int bindLoc,
            ColumnVal value, ColumnSpec columnSpec) throws SQLException
    {
        if (value.getValue() == null)
        {
            super.setObject(prepStatement, bindLoc, value, columnSpec);
        }
        else if (columnSpec.getType() == Types.TIME)
        {
            Time t = (Time) value.getValue();
            prepStatement.setString(bindLoc, t.toString());
        }
        else if (columnSpec.getType() == Types.DOUBLE)
        {
            BigDecimal dec = new BigDecimal((Double) value.getValue());
            prepStatement.setBigDecimal(bindLoc, dec);
        }
        else if (columnSpec.getType() == Types.FLOAT)
        {
            BigDecimal dec = new BigDecimal((Float) value.getValue());
            prepStatement.setBigDecimal(bindLoc, dec);
        }
        else if (columnSpec.getType() == Types.VARCHAR
                && value.getValue() instanceof byte[])
        {
            prepStatement
                    .setString(bindLoc, hexdump((byte[]) value.getValue()));
        }
        else if (columnSpec.getType() == Types.BLOB
                && value.getValue() instanceof SerialBlob
                && columnSpec.getTypeDescription() != null
                && columnSpec.getTypeDescription().contains("TEXT"))
        {
            SerialBlob val = (SerialBlob) value.getValue();
            byte[] bytes = val.getBytes(1, (int) val.length());
            prepStatement.setString(bindLoc, hexdump(bytes));
        }
        else
            super.setObject(prepStatement, bindLoc, value, columnSpec);
    }

}

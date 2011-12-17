/**
 * Tungsten: An Application Server for uni/cluster.
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
 * Initial developer(s): Scott Martin
 * Contributor(s): Robert Hodges, Stephane Giron
 */

package com.continuent.tungsten.replicator.database;

import java.io.BufferedWriter;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.csv.CsvWriter;
import com.continuent.tungsten.commons.csv.NullPolicy;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.OneRowChange;

/**
 * Implements DBMS-specific operations for MySQL.
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 */
public class MySQLDatabase extends AbstractDatabase
{
    private static Logger logger                        = Logger.getLogger(MySQLDatabase.class);

    private boolean       sessionLevelLoggingSuppressed = false;

    public MySQLDatabase() throws SQLException
    {
        dbms = DBMS.MYSQL;
        // Hard code the driver so it gets loaded correctly.
        dbDriver = "com.mysql.jdbc.Driver";
    }

    protected String columnToTypeString(Column c)
    {
        switch (c.getType())
        {
            case Types.TINYINT :
                return "TINYINT";

            case Types.SMALLINT :
                return "SMALLINT";

            case Types.INTEGER :
                return "INT";

            case Types.BIGINT :
                return "BIGINT";

            case Types.CHAR :
                return "CHAR(" + c.getLength() + ")";

            case Types.VARCHAR :
                return "VARCHAR(" + c.getLength() + ")";

            case Types.DATE :
                return "DATETIME";

            case Types.TIMESTAMP :
                return "TIMESTAMP";

            case Types.CLOB :
                return "LONGTEXT";

            case Types.BLOB :
                return "LONGBLOB";

            default :
                return "UNKNOWN";
        }
    }

    /**
     * Connect to a MySQL database, which includes setting the wait_timeout to a
     * very high value so we don't lose our connection. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#connect()
     */
    public void connect() throws SQLException
    {
        connect(false);
    }

    /**
     * Connect to a MySQL database, which includes setting the wait_timeout to a
     * very high value so we don't lose our connection. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#connect(boolean)
     */
    public void connect(boolean binlog) throws SQLException
    {
        // Use superclass method to avoid missing things like loading the
        // driver class.
        super.connect(binlog);

        // set connection timeout to maximum to prevent timeout on the
        // server side
        // TREP-285 - Need to trap SQL error as some MySQL versions don't accept
        // an out of bounds number.
        try
        {
            executeUpdate("SET wait_timeout = 2147483");
        }
        catch (SQLException e)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Unable to set wait_timeout to maximum value of 2147483");
                logger.debug("Please consider using an explicit JDBC URL setting to avoid connection timeouts");
            }
        }
    }

    public void createTable(Table t, boolean replace) throws SQLException
    {
        createTable(t, replace, null);
    }

    public boolean supportsReplace()
    {
        return true;
    }

    public boolean supportsUseDefaultSchema()
    {
        return true;
    }

    public void useDefaultSchema(String schema) throws SQLException
    {
        execute(getUseSchemaQuery(schema));
        this.defaultSchema = schema;
    }

    public String getUseSchemaQuery(String schema)
    {
        return "USE " + getDatabaseObjectName(schema);
    }

    public boolean supportsCreateDropSchema()
    {
        return true;
    }

    public void createSchema(String schema) throws SQLException
    {
        String SQL = "CREATE DATABASE IF NOT EXISTS " + schema;
        execute(SQL);
    }

    public void dropSchema(String schema) throws SQLException
    {
        String SQL = "DROP DATABASE IF EXISTS " + schema;
        execute(SQL);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsControlSessionLevelLogging()
     */
    public boolean supportsControlSessionLevelLogging()
    {
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#controlSessionLevelLogging(boolean)
     */
    public void controlSessionLevelLogging(boolean suppressed)
            throws SQLException
    {
        if (suppressed != this.sessionLevelLoggingSuppressed)
        {
            if (suppressed)
                executeUpdate("SET SQL_LOG_BIN=0");
            else
                executeUpdate("SET SQL_LOG_BIN=1");

            this.sessionLevelLoggingSuppressed = suppressed;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#supportsNativeSlaveSync()
     */
    @Override
    public boolean supportsNativeSlaveSync()
    {
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#syncNativeSlave(java.lang.String)
     */
    @Override
    public void syncNativeSlave(String eventId) throws SQLException
    {
        // Parse the event ID, which has the following format:
        // <file>:<offset>[;<session id>]
        int colonIndex = eventId.indexOf(':');
        String binlogFile = eventId.substring(0, colonIndex);

        int semicolonIndex = eventId.indexOf(";");
        int binlogOffset;
        if (semicolonIndex != -1)
            binlogOffset = Integer.valueOf(eventId.substring(colonIndex + 1,
                    semicolonIndex));
        else
            binlogOffset = Integer.valueOf(eventId.substring(colonIndex + 1));

        // Create a CHANGE MASTER TO command.
        String changeMaster = String.format(
                "CHANGE MASTER TO master_log_file = '%s', master_log_pos = %s",
                binlogFile, binlogOffset);
        executeUpdate(changeMaster);
    }

    public boolean supportsControlTimestamp()
    {
        return true;
    }

    /**
     * MySQL supports the 'set timestamp' command, which is what we return.
     */
    public String getControlTimestampQuery(Long timestamp)
    {
        return "SET TIMESTAMP=" + (timestamp / 1000);
    }

    /**
     * MySQL supports session variables.
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsSessionVariables()
     */
    public boolean supportsSessionVariables()
    {
        return true;
    }

    /**
     * Sets a variable on the current session using MySQL SET command.
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#setSessionVariable(java.lang.String,
     *      java.lang.String)
     */
    public void setSessionVariable(String name, String value)
            throws SQLException
    {
        String escapedValue = value.replaceAll("'", "\'");
        execute("SET @" + name + "='" + escapedValue + "'");
    }

    /**
     * Gets a variable on the current session.
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getSessionVariable(java.lang.String)
     */
    public String getSessionVariable(String name) throws SQLException
    {
        Statement s = null;
        ResultSet rs = null;
        String value = null;
        try
        {
            s = dbConn.createStatement();
            rs = s.executeQuery("SELECT @" + name);
            while (rs.next())
            {
                value = rs.getString(1);
            }
            rs.close();
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                }
            }
            if (s != null)
            {
                try
                {
                    s.close();
                }
                catch (SQLException e)
                {
                }
            }
        }
        return value;
    }

    public ArrayList<String> getSchemas() throws SQLException
    {
        ArrayList<String> schemas = new ArrayList<String>();

        try
        {
            DatabaseMetaData md = this.getDatabaseMetaData();
            ResultSet rs = md.getCatalogs();
            while (rs.next())
            {
                schemas.add(rs.getString("TABLE_CAT"));
            }
            rs.close();
        }
        finally
        {
        }

        return schemas;
    }

    public ResultSet getColumnsResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException
    {
        return md.getColumns(schemaName, null, tableName, null);
    }

    protected ResultSet getPrimaryKeyResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException
    {
        return md.getPrimaryKeys(schemaName, null, tableName);
    }

    protected ResultSet getTablesResultSet(DatabaseMetaData md,
            String schemaName, boolean baseTablesOnly) throws SQLException
    {
        String types[] = null;
        if (baseTablesOnly)
            types = new String[]{"TABLE"};

        return md.getTables(schemaName, null, null, types);
    }

    /**
     * getTimeDiff returns the database-specific way of subtracting two "dates"
     * and return the result in seconds complete with space for the two bind
     * variables. E.g. in MySQL it might be "time_to_sec(timediff(?, ?))". If
     * either of the string variables are null, replace with the bind character
     * (e.g. "?") else use the string given. For example getTimeDiff(null,
     * "myTimeCol") -> time_to_sec(timediff(?, myTimeCol))
     */
    public String getTimeDiff(String string1, String string2)
    {
        String retval = "time_to_sec(timediff(";
        if (string1 == null)
            retval += "?";
        else
            retval += string1;
        retval += ",";
        if (string2 == null)
            retval += "?";
        else
            retval += string2;
        retval += "))";

        return retval;
    }

    public String getNowFunction()
    {
        return "now()";
    }

    public String getPlaceHolder(OneRowChange.ColumnSpec col, Object colValue,
            String typeDesc)
    {
        return " ? ";
    }

    public boolean nullsBoundDifferently(OneRowChange.ColumnSpec col)
    {
        return false;
    }

    public boolean nullsEverBoundDifferently()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#prepareOptionSetStatement(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public String prepareOptionSetStatement(String optionName,
            String optionValue)
    {
        return "set @@session." + optionName + "=" + optionValue;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#createTable(com.continuent.tungsten.replicator.database.Table,
     *      boolean, java.lang.String)
     */
    @Override
    public void createTable(Table t, boolean replace, String tableType)
            throws SQLException
    {
        boolean comma = false;
        String SQL;

        if (replace)
        {
            this.dropTable(t);
        }
        String temporary = t.isTemporary() ? "TEMPORARY " : "";

        SQL = "CREATE " + temporary + "TABLE ";
        SQL += (replace ? "" : "IF NOT EXISTS ");
        SQL += t.getSchema() + "." + t.getName();
        SQL += " (";

        Iterator<Column> i = t.getAllColumns().iterator();
        while (i.hasNext())
        {
            Column c = i.next();
            SQL += (comma ? ", " : "") + c.getName() + " "
                    + columnToTypeString(c)
                    + (c.isNotNull() ? " NOT NULL" : " NULL");

            comma = true;
        }
        Iterator<Key> j = t.getKeys().iterator();

        while (j.hasNext())
        {
            Key key = j.next();
            SQL += ", ";
            switch (key.getType())
            {
                case Key.Primary :
                    SQL += "PRIMARY KEY (";
                    break;
                case Key.Unique :
                    SQL += "UNIQUE KEY (";
                    break;
                case Key.NonUnique :
                    SQL += "KEY (";
                    break;
            }
            i = key.getColumns().iterator();
            comma = false;
            while (i.hasNext())
            {
                Column c = i.next();
                SQL += (comma ? ", " : "") + c.getName();
                comma = true;
            }
            SQL += ")";
        }
        SQL += ")";
        if (tableType != null && tableType.length() > 0)
            SQL += " ENGINE=" + tableType;
        SQL += " CHARSET=utf8";
        execute(SQL);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#getDatabaseObjectName(java.lang.String)
     */
    @Override
    public String getDatabaseObjectName(String name)
    {
        return "`" + name + "`";
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#getSqlNameMatcher()
     */
    @Override
    public SqlOperationMatcher getSqlNameMatcher() throws ReplicatorException
    {
        return new MySQLOperationMatcher();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getCsvWriter(java.io.BufferedWriter)
     */
    public CsvWriter getCsvWriter(BufferedWriter writer)
    {
        CsvWriter csv = new CsvWriter(writer);
        csv.setQuoteChar('"');
        csv.setQuoted(true);
        csv.setEscapeChar('\\');
        csv.setEscapedChars("\\");
        csv.setNullPolicy(NullPolicy.nullValue);
        csv.setNullValue("\\N");
        csv.setWriteHeaders(false);
        return csv;
    }
}
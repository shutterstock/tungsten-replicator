/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2012 Continuent Inc.
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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.database;

import java.io.BufferedWriter;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.csv.CsvWriter;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.heartbeat.HeartbeatTable;

/**
 * Defines an interface to the Oracle database
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 */
public class OracleDatabase extends AbstractDatabase
{
    private static Logger             logger = Logger.getLogger(OracleDatabase.class);
    private Hashtable<Integer, Table> tablesCache;
    private String                    colList;

    public OracleDatabase()
    {
        dbms = DBMS.ORACLE;
        // Hard code the driver so it gets loaded correctly.
        dbDriver = "oracle.jdbc.driver.OracleDriver";
        tablesCache = new Hashtable<Integer, Table>();
    }

    /**
     * In Oracle, to support timestamp with local time zone replication we need
     * to set the session level time zone to be the same as the database time
     * zone. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#getSqlNameMatcher()
     */
    @Override
    public SqlOperationMatcher getSqlNameMatcher() throws ReplicatorException
    {
        // TODO: Develop matcher for Oracle dialect.
        return new MySQLOperationMatcher();
    }

    /**
     * In Oracle, to support timestamp with local time zone replication we need
     * to set the session level time zone to be the same as the database time
     * zone. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#connect(boolean)
     */
    @Override
    public synchronized void connect(boolean binlog) throws SQLException
    {
        super.connect(binlog);
        ResultSet res = null;
        Statement statement = createStatement();
        String timeZone = "00:00";
        try
        {
            res = statement.executeQuery("select dbtimezone from dual");

            if (res.next())
            {
                // Get consistency check parameters:
                timeZone = res.getString("dbtimezone");
            }
            String SQL = "alter session set TIME_ZONE='" + timeZone + "'";
            if (logger.isDebugEnabled())
            {
                logger.debug("Setting timezone to " + timeZone);
                logger.debug("With the following SQL : " + SQL);
            }
            executeUpdate(SQL);
        }
        catch (SQLException e)
        {
            logger.warn("Unable to set timezone");
        }
    }

    public String getPlaceHolder(OneRowChange.ColumnSpec col, Object colValue,
            String typeDesc)
    {
        if (col.getType() == AdditionalTypes.XML)
        {
            if (colValue == null)
                return " NULL ";
            // else return " XMLTYPE(?) ";
            else
                return " XMLTYPE(?, NULL, 1, 1) ";
        }
        else
            return " ? ";
    }

    /**
     * Return TRUE IFF NULL values are bound differently in SQL statement from
     * non null values for the given column type. For example, in Oracle, the
     * datatype XML must look like "XMLTYPE(?)" in most SQL statements, but in
     * the case of a NULL value, it would look simply like "?".
     */
    public boolean nullsBoundDifferently(OneRowChange.ColumnSpec col)
    {
        if (col.getType() == AdditionalTypes.XML)
            return true;
        else
            return false;
    }

    public boolean nullsEverBoundDifferently()
    {
        return true;
    }

    protected String columnToTypeString(Column c)
    {
        switch (c.getType())
        {
            case Types.INTEGER :
                return "NUMBER";

            case Types.BIGINT :
                return "NUMBER";

            case Types.SMALLINT :
                return "NUMBER";

            case Types.TINYINT :
                return "NUMBER";

            case Types.CHAR :
                return "CHAR(" + c.getLength() + ")";

            case Types.VARCHAR :
                return "VARCHAR(" + c.getLength() + ")";

            case Types.DATE :
                return "DATE";

            case Types.TIMESTAMP :
                return "TIMESTAMP";

                /*
                 * we currently need to use BLOB data type here since in
                 * SQLEventLog.java we do Blob trxBlob = resultSet.getBlob(2);
                 * and this call hangs if the Oracle data type is CLOB or
                 * varchar2
                 */
            case Types.CLOB :
                return "CLOB";

            case Types.BLOB :
                return "BLOB";

            default :
                return "UNKNOWN";
        }
    }

    private void createNonUnique(Table t) throws SQLException
    {
        boolean comma = false;
        String SQL;
        int indexNumber = 1;

        SQL = "CREATE TABLE " + t.getSchema() + "." + t.getName() + " (";

        Iterator<Key> j = t.getKeys().iterator();

        while (j.hasNext())
        {
            Key key = j.next();
            SQL += ", ";
            if (key.getType() != Key.NonUnique)
                continue;
            SQL = "CREATE INDEX " + t.getSchema() + "." + t.getName() + "_"
                    + indexNumber;
            SQL += " on " + t.getSchema() + "." + t.getName() + "(";

            Iterator<Column> i = key.getColumns().iterator();
            comma = false;
            while (i.hasNext())
            {
                Column c = i.next();
                SQL += (comma ? ", " : "") + c.getName();
                comma = true;
            }
            indexNumber++;
            SQL += ")";
            try
            {
                execute(SQL);
            }
            catch (SQLException e)
            {
                /*
                 * 955 = "Error: ORA-00955: name is already used by an existing
                 * object"
                 */
                if (e.getErrorCode() != 955)
                    throw e;
            }
        }
    }

    public void createTable(Table t, boolean replace) throws SQLException
    {
        boolean comma = false;
        boolean haveNonUnique = false;
        String SQL;
        colList = "";

        if (replace)
        {
            SQL = "DROP TABLE " + t.getSchema() + "." + t.getName();
            try
            {
                execute(SQL);
            }
            catch (SQLException e)
            {
            }
        }

        SQL = "CREATE TABLE " + t.getSchema() + "." + t.getName() + " (";

        Iterator<Column> i = t.getAllColumns().iterator();
        while (i.hasNext())
        {
            Column c = i.next();
            SQL += (comma ? ", " : "") + c.getName() + " "
                    + columnToTypeString(c)
                    + (c.isNotNull() ? " NOT NULL" : "");
            colList += (comma ? ", " : "") + c.getName() + " "
                    + columnToTypeString(c);
            comma = true;
        }
        Iterator<Key> j = t.getKeys().iterator();

        while (j.hasNext())
        {
            Key key = j.next();
            if (key.getType() == Key.NonUnique)
            {
                // non-unique keys will be handled outside of create table
                // statement
                haveNonUnique = true;
                continue;
            }
            SQL += ", ";
            switch (key.getType())
            {
                case Key.Primary :
                    SQL += "PRIMARY KEY (";
                    break;
                case Key.Unique :
                    SQL += "UNIQUE (";
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
        try
        {
            execute(SQL);
        }
        catch (SQLException e)
        {
            /*
             * 955 = "Error: ORA-00955: name is already used by an existing
             * object"
             */
            if (e.getErrorCode() != 955)
                throw e;
        }

        if (haveNonUnique)
            createNonUnique(t);
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
        return "ALTER SESSION SET CURRENT_SCHEMA=" + schema;
    }

    public int nativeTypeToJavaSQLType(int nativeType) throws SQLException
    {
        /* these types can be found in $ORACLE_HOME/rdbms/public/ocidfn.h */
        switch (nativeType)
        {
            case 1 :
                return java.sql.Types.VARCHAR; /* character string */
            case 2 :
                return java.sql.Types.NUMERIC; /* oracle number */
            case 12 :
                return java.sql.Types.DATE; /* oracle date */
            case 58 :
                return AdditionalTypes.XML; /* Oracle XML. */
            case 96 :
                return java.sql.Types.CHAR; /* oracle char */
            case 100 :
                return java.sql.Types.FLOAT; /* oracle BINARY_DOUBLE */
            case 101 :
                return java.sql.Types.DOUBLE; /* oracle BINARY_DOUBLE */
            case 112 :
                return java.sql.Types.CLOB;
            case 113 :
                return java.sql.Types.BLOB;
            case 180 :
                return java.sql.Types.TIMESTAMP;
            case 231 : /* Oracle timestamp with local time zone */
                return java.sql.Types.TIMESTAMP;
            default :
                throw new SQLException("Unsupported Oracle type " + nativeType);
        }
    }

    public int javaSQLTypeToNativeType(int javaSQLType) throws SQLException
    {
        switch (javaSQLType)
        {
            case java.sql.Types.VARCHAR :
                return 1; /* character string */
            case java.sql.Types.NUMERIC :
                return 2; /* oracle number */
            case java.sql.Types.DATE :
                return 12; /* oracle date */
            case AdditionalTypes.XML :
                return 58; /* oracle XML */
            case java.sql.Types.CHAR :
                return 96; /* oracle char */
            case java.sql.Types.FLOAT :
                return 100; /* oracle binary_float */
            case java.sql.Types.DOUBLE :
                return 101; /* oracle binary_double */
            case java.sql.Types.CLOB :
                return 112;
            case java.sql.Types.BLOB :
                return 113;
            case java.sql.Types.TIMESTAMP :
                return 180;
            default :
                throw new SQLException("Unsupported java type in Oracle "
                        + javaSQLType);
        }
    }

    /**
     * Return the Table with all it's accompanying Columns that matches tableID.
     * tableID is meant to be some sort of unique "object number" interpretted
     * within the current connection. The exact nature of of what this tableID
     * will likely vary from rdbms to rdbms but in Oracle parlance it is an
     * object number. Returns null if no such table exists.
     */
    public Table findTable(int tableID) throws SQLException
    {
        return findTable(tableID, null);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#findTable(int,
     *      java.lang.String)
     */
    public Table findTable(int tableID, String scn) throws SQLException
    {
        Table t = null;
        Column c;
        StringBuffer sb = new StringBuffer();

        // Check if the table already exists in the table cache
        t = tablesCache.get(Integer.valueOf(tableID));
        if (t != null && t.getSCN() != null && t.getSCN().equals(scn))
        {
            // Cache hit
            if (logger.isDebugEnabled())
                logger.debug("Table " + tableID + "@" + scn
                        + " found in cache (" + t.getSchema() + "."
                        + t.getName() + ")");
            return t;
        }
        else
        {
            // Either table not found at all in the cache, or found at a
            // different scn
            if (logger.isDebugEnabled())
            {
                if (t == null)
                {
                    logger.debug("Table " + tableID + "@" + scn
                            + " not found in cache");
                }
                else
                {
                    logger.debug("Table "
                            + tableID
                            + "@"
                            + scn
                            + " not found in cache, replacing old table definition ("
                            + tableID + "@" + t.getSCN() + ")");
                }
            }
            t = null;
        }
        /* currently not selecting for is Nullableness */
        sb.append("select uname, oname, col#, cname, type#");
        sb.append("  from dictionary");
        if (scn != null)
        {
            sb.append("  as of scn " + scn);
        }
        sb.append(" where obj# = " + tableID);

        String sql = sb.toString();
        if (logger.isDebugEnabled())
        {
            logger.debug("sql = \"" + sql + "\"");
        }

        ResultSet res = null;
        Statement stmt = null;
        try
        {
            stmt = dbConn.createStatement();
            res = stmt.executeQuery(sql);

            while (res.next())
            {
                if (t == null)
                {
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Table : " + res.getString(1) + "."
                                + res.getString(2));
                    }
                    t = new Table(res.getString(1), res.getString(2));
                }

                if (logger.isDebugEnabled())
                {
                    logger.debug("   - col #" + res.getString(3) + " / "
                            + res.getString(4) + " : "
                            + nativeTypeToJavaSQLType(res.getInt(5)));
                }
                c = new Column(res.getString(4),
                        nativeTypeToJavaSQLType(res.getInt(5)));

                t.AddColumn(c);
            }
        }
        finally
        {
            if (res != null)
                try
                {
                    res.close();
                }
                catch (SQLException ignore)
                {
                }
            if (stmt != null)
                try
                {
                    stmt.close();
                }
                catch (SQLException ignore)
                {
                }
        }
        // Update the cache with the definition of the table (add or replace old
        // definition from the cache)
        t.setSCN(scn);
        updateTableCache(tableID, t);

        return t;
    }

    private void updateTableCache(int tableID, Table t)
    {
        // Eventually remove old table definition from the cache
        tablesCache.remove(tableID);
        // and add the "new" definition
        tablesCache.put(Integer.valueOf(tableID), t);
    }

    public boolean supportsCreateDropSchema()
    {
        /*
         * clearly we could return TRUE here when/if we implement create/drop
         * Schema
         */
        return false;
    }

    public void createSchema(String schema) throws SQLException
    {
        throw new SQLException("createSchema not supported");
    }

    public void dropSchema(String schema) throws SQLException
    {
        throw new SQLException("dropSchema not supported");
    }

    public ArrayList<String> getSchemas() throws SQLException
    {
        ArrayList<String> schemas = new ArrayList<String>();

        try
        {
            DatabaseMetaData md = this.getDatabaseMetaData();
            ResultSet rs = md.getSchemas();
            while (rs.next())
            {
                schemas.add(rs.getString("TABLE_SCHEM"));
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
        return md.getColumns(null, schemaName, tableName, null);
    }

    protected ResultSet getPrimaryKeyResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException
    {
        return md.getPrimaryKeys(null, schemaName, tableName);
    }

    protected ResultSet getTablesResultSet(DatabaseMetaData md,
            String schemaName, boolean baseTablesOnly) throws SQLException
    {
        // TODO: Implement ability to fetch base tables properly.
        return md.getTables(schemaName, null, null, null);
    }

    public String getNowFunction()
    {
        return "SYSDATE";
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
        String retval = "((";
        if (string1 == null)
            retval += "?";
        else
            retval += string1;
        retval += " - ";
        if (string2 == null)
            retval += "?";
        else
            retval += string2;
        retval += ") * 60 * 60 * 24)";

        return retval;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getCsvWriter(java.io.BufferedWriter)
     */
    public CsvWriter getCsvWriter(BufferedWriter writer)
    {
        // Need to implement in order to support CSV.
        throw new UnsupportedOperationException(
                "CSV output is not supported for this database type");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#createTable(com.continuent.tungsten.replicator.database.Table,
     *      boolean, java.lang.String)
     */
    @Override
    public void createTable(Table table, boolean replace,
            String tungstenSchema, String tungstenTableType)
            throws SQLException
    {
        createTable(table, replace);

        String tableName = HeartbeatTable.TABLE_NAME.toUpperCase();

        if (tungstenTableType.startsWith("CDC")
                && table.getSchema().equals(tungstenSchema)
                && table.getName().equalsIgnoreCase(tableName))
        {

            Statement statement = dbConn.createStatement();
            ResultSet rs = null;
            boolean changeTableAlreadyDefined = false;
            try
            {
                rs = statement
                        .executeQuery("SELECT * FROM USER_TABLES WHERE table_name='CT_"
                                + tableName + "'");

                changeTableAlreadyDefined = rs.next();
            }
            finally
            {
                if (rs != null)
                    rs.close();
                statement.close();

            }

            if (changeTableAlreadyDefined)
            {
                logger.info("Tungsten Heartbeat change table already defined. Skipping");
                // We are done, just exit.
                return;
            }

            logger.info("Creating Tungsten Heartbeat change table");

            if (!tungstenTableType.equals("CDCSYNC"))
            {
                // Disable Tungsten Change Set 
                execute("BEGIN DBMS_CDC_PUBLISH.ALTER_CHANGE_SET(change_set_name=>'TUNGSTEN_CHANGE_SET',enable_capture=>'N'); END;");
                // Or prepare table for asynchronous capture.
                // This should not be done for synchronous capture as this would
                // not work on standard edition.
                execute("ALTER TABLE " + table.getSchema() + "."
                        + table.getName()
                        + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

                execute("BEGIN DBMS_CAPTURE_ADM.PREPARE_TABLE_INSTANTIATION('"
                        + table.getSchema() + "." + tableName
                        + "', 'all');END;");
            }

            String cdcSQL;
            if (tungstenTableType.equals("CDCSYNC")
                    && dbConn.getMetaData().getDatabaseMajorVersion() >= 11)
            {
                logger.info("Setting up synchronous data capture with version > "
                        + dbConn.getMetaData().getDatabaseMajorVersion());
                cdcSQL = "BEGIN "
                        + "DBMS_CDC_PUBLISH.CREATE_CHANGE_TABLE(owner=>'"
                        + table.getSchema()
                        + "', change_table_name=> '"
                        + "CT_"
                        + tableName
                        + "', change_set_name=>'TUNGSTEN_CHANGE_SET', source_schema=>'"
                        + table.getSchema()
                        + "', source_table=>'"
                        + tableName
                        + "', column_type_list => '"
                        + colList
                        + "', capture_values => 'both', rs_id => 'y', row_id => 'n', "
                        + "user_id => 'n', timestamp => 'n', object_id => 'n', "
                        + "target_colmap => 'y', source_colmap => 'n', ddl_markers=>'n', "
                        + "options_string=>'TABLESPACE " + table.getSchema()
                        + "'); END;";

            }
            else
                cdcSQL = "BEGIN "
                        + "DBMS_CDC_PUBLISH.CREATE_CHANGE_TABLE(owner=>'"
                        + table.getSchema()
                        + "', change_table_name=> '"
                        + "CT_"
                        + tableName
                        + "', change_set_name=>'TUNGSTEN_CHANGE_SET', source_schema=>'"
                        + table.getSchema()
                        + "', source_table=>'"
                        + tableName
                        + "', column_type_list => '"
                        + colList
                        + "', capture_values => 'both', rs_id => 'y', row_id => 'n', "
                        + "user_id => 'n', timestamp => 'n', object_id => 'n', "
                        + "target_colmap => 'y', source_colmap => 'n', "
                        + "options_string=>'TABLESPACE " + table.getSchema()
                        + "'); END;";

            execute(cdcSQL);

            if (!tungstenTableType.equals("CDCSYNC"))
                // Enable Tungsten Change Set back
                execute("BEGIN DBMS_CDC_PUBLISH.ALTER_CHANGE_SET(change_set_name=>'TUNGSTEN_CHANGE_SET',enable_capture=>'Y'); END;");

        }

    }
}

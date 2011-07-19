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

import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.consistency.ConsistencyCheck;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.consistency.ConsistencyTable;
import com.continuent.tungsten.replicator.dbms.OneRowChange;

/**
 * Provides a generic implementation for Database interface. Subclasses must
 * supply at least the implementation for method columnToTypeString(), which
 * converts values from java.sql.Types to DBMS-specific column specifications.
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @version 1.0
 */
/*
 * public abstract class Database implements Runnable
 */
public abstract class AbstractDatabase implements Database
{
    private static Logger    logger        = Logger.getLogger(AbstractDatabase.class);

    protected DBMS           dbms;
    protected String         dbDriver      = null;
    protected String         dbUri         = null;
    protected String         dbUser        = null;
    protected String         dbPassword    = null;
    protected Connection     dbConn        = null;
    protected boolean        autoCommit    = false;
    protected String         defaultSchema = null;

    protected static boolean driverLoaded  = false;
    protected boolean        connected     = false;

    /**
     * Create a new database instance. To use the database instance you must at
     * minimum set the url, host, and password properties.
     */
    public AbstractDatabase()
    {
    }

    public Connection getConnection()
    {
        return dbConn;
    }

    public DBMS getType()
    {
        return dbms;
    }

    public abstract SqlOperationMatcher getSqlNameMatcher()
            throws DatabaseException;

    public void setUrl(String dbUri)
    {
        this.dbUri = dbUri;
    }

    public void setUser(String dbUser)
    {
        this.dbUser = dbUser;
    }

    public void setPassword(String dbPassword)
    {
        this.dbPassword = dbPassword;
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
     * Return a properly constructed type specification for the column. Concrete
     * Database subclasses must implement at least this method if no others.
     * 
     * @param c Column for which specification is required
     * @return String containing specification
     */
    abstract protected String columnToTypeString(Column c);

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#connect()
     */
    public synchronized void connect() throws SQLException
    {
        connect(false);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#connect(boolean)
     */
    public synchronized void connect(boolean binlog) throws SQLException
    {
        if (dbConn == null)
        {
            if (!driverLoaded && dbDriver != null)
            {
                try
                {
                    logger.info("Loading database driver: " + dbDriver);
                    Class.forName(dbDriver);
                    driverLoaded = true;
                }
                catch (Exception e)
                {
                    throw new RuntimeException("Unable to load driver: "
                            + dbDriver, e);
                }
            }

            dbConn = DriverManager.getConnection(dbUri, dbUser, dbPassword);
            connected = (dbConn != null);

            if (connected)
            {
                if (supportsControlSessionLevelLogging())
                {
                    this.controlSessionLevelLogging(!binlog);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#disconnect()
     */
    public synchronized void disconnect()
    {
        if (dbConn != null)
        {
            try
            {
                dbConn.close();
            }
            catch (SQLException e)
            {
                logger.warn("Unable to close connection", e);
            }
            dbConn = null;
            connected = false;
        }
    }

    public DatabaseMetaData getDatabaseMetaData() throws SQLException
    {
        return dbConn.getMetaData();
    }

    /**
     * Returns false by default as only some database types allow schema to be
     * created dynamically.
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsCreateDropSchema()
     */
    public boolean supportsCreateDropSchema()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#createSchema(java.lang.String)
     */
    public void createSchema(String schema) throws SQLException
    {
        throw new UnsupportedOperationException(
                "Creating schema is not supported");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#dropSchema(java.lang.String)
     */
    public void dropSchema(String schema) throws SQLException
    {
        throw new UnsupportedOperationException(
                "Dropping schema is not supported");
    }

    /**
     * Returns false by default as only some database types allow schema to
     * change.
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsUseDefaultSchema()
     */
    public boolean supportsUseDefaultSchema()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#useDefaultSchema(java.lang.String)
     */
    public void useDefaultSchema(String schema) throws SQLException
    {
        throw new UnsupportedOperationException(
                "Setting the default schema is not supported");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getUseSchemaQuery(java.lang.String)
     */
    public String getUseSchemaQuery(String schema)
    {
        throw new UnsupportedOperationException(
                "Getting the default schema is not supported");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsControlSessionLevelLogging()
     */
    public boolean supportsControlSessionLevelLogging()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#controlSessionLevelLogging(boolean)
     */
    public void controlSessionLevelLogging(boolean suppressed)
            throws SQLException
    {
        throw new UnsupportedOperationException(
                "Controlling session level logging is not supported");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsNativeSlaveSync()
     */
    public boolean supportsNativeSlaveSync()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#syncNativeSlave(java.lang.String)
     */
    public void syncNativeSlave(String eventId) throws SQLException
    {
        throw new UnsupportedOperationException(
                "Native slave synchronization is not supported");
    }

    /**
     * By default we do not support controlling the timestamp. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsControlTimestamp()
     */
    public boolean supportsControlTimestamp()
    {
        return false;
    }

    /**
     * Returns a query that can be used to set the timestamp.
     * 
     * @param timestamp Time in milliseconds according to Java standard
     * @see #supportsControlTimestamp()
     */
    public String getControlTimestampQuery(Long timestamp)
    {
        throw new UnsupportedOperationException(
                "Controlling session level logging is not supported");
    }

    /**
     * By default we do not support setting session variables. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsSessionVariables()
     */
    public boolean supportsSessionVariables()
    {
        return false;
    }

    /**
     * Sets a variable on the current session. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#setSessionVariable(java.lang.String,
     *      java.lang.String)
     */
    public void setSessionVariable(String name, String value)
            throws SQLException
    {
        throw new UnsupportedOperationException(
                "Session variables are not supported");
    }

    /**
     * Gets a variable on the current session. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getSessionVariable(java.lang.String)
     */
    public String getSessionVariable(String name) throws SQLException
    {
        throw new UnsupportedOperationException(
                "Session variables are not supported");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#execute(java.lang.String)
     */
    public void execute(String SQL) throws SQLException
    {
        Statement sqlStatement = null;
        try
        {
            sqlStatement = dbConn.createStatement();
            logger.debug(SQL);
            sqlStatement.execute(SQL);
        }
        finally
        {
            if (sqlStatement != null)
                sqlStatement.close();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#executeUpdate(java.lang.String)
     */
    public void executeUpdate(String SQL) throws SQLException
    {
        Statement sqlStatement = null;

        try
        {
            sqlStatement = dbConn.createStatement();
            logger.debug(SQL);
            sqlStatement.executeUpdate(SQL);
        }
        finally
        {
            sqlStatement.close();
        }
    }

    private String buildWhereClause(ArrayList<Column> columns)
    {
        if (columns.size() == 0)
            return "";

        StringBuffer retval = new StringBuffer(" WHERE ");

        Iterator<Column> i = columns.iterator();
        boolean comma = false;
        while (i.hasNext())
        {
            Column c = i.next();
            if (comma)
                retval.append(" AND ");
            comma = true;
            retval.append(assignString(c));
        }
        return retval.toString();
    }

    private String buildCommaAssign(ArrayList<Column> columns)
    {
        StringBuffer retval = new StringBuffer();
        Iterator<Column> i = columns.iterator();
        boolean comma = false;
        while (i.hasNext())
        {
            Column c = i.next();
            if (comma)
                retval.append(", ");
            comma = true;
            retval.append(assignString(c));
        }
        return retval.toString();
    }

    private String buildCommaValues(ArrayList<Column> columns)
    {
        StringBuffer retval = new StringBuffer();
        for (int i = 0; i < columns.size(); i++)
        {
            if (i > 0)
                retval.append(", ");
            retval.append("?");
        }
        return retval.toString();
    }

    private String assignString(Column c)
    {
        return c.getName() + "= ?";
    }

    private int executePrepareStatement(List<Column> columns,
            PreparedStatement statement) throws SQLException
    {
        int bindNo = 1;

        for (Column c : columns)
        {
            statement.setObject(bindNo++, c.getValue());
        }
        // System.out.format("%s (%d binds)\n", SQL, bindNo - 1);
        return statement.executeUpdate();
    }

    private int executePrepareStatement(Table table, PreparedStatement statement)
            throws SQLException
    {
        return executePrepareStatement(table.getAllColumns(), statement);
    }

    private int executePrepare(Table table, List<Column> columns, String SQL)
            throws SQLException
    {
        return executePrepare(table, columns, SQL, false, -1);
    }

    private int executePrepare(Table table, String SQL) throws SQLException
    {
        return executePrepare(table, table.getAllColumns(), SQL, false, -1);
    }

    private int executePrepare(Table table, List<Column> columns, String SQL,
            boolean keep, int type) throws SQLException
    {
        int bindNo = 1;

        PreparedStatement statement = null;
        int affectedRows = 0;

        try
        {
            statement = dbConn.prepareStatement(SQL);

            for (Column c : columns)
            {
                statement.setObject(bindNo++, c.getValue());
            }
            affectedRows = statement.executeUpdate();
        }
        finally
        {
            if (statement != null && !keep)
            {
                statement.close();
                statement = null;
            }
        }
        if (keep && type > -1)
            table.setStatement(type, statement);

        return affectedRows;
    }

    /**
     * {@inheritDoc}
     * 
     * @return
     * @see com.continuent.tungsten.replicator.database.Database#insert(com.continuent.tungsten.replicator.database.Table)
     */
    public int insert(Table table) throws SQLException
    {
        String SQL = "";
        PreparedStatement statement = null;
        boolean caching = table.getCacheStatements();
        ArrayList<Column> allColumns = table.getAllColumns();

        if (caching && (statement = table.getStatement(Table.INSERT)) != null)
        {
            return executePrepareStatement(table, statement);
        }
        else
        {
            SQL += "INSERT INTO " + table.getSchema() + "." + table.getName()
                    + " VALUES (";
            SQL += buildCommaValues(allColumns);
            SQL += ")";
        }

        return executePrepare(table, allColumns, SQL, caching, Table.INSERT);
    }

    /**
     * {@inheritDoc}
     * 
     * @return
     * @see com.continuent.tungsten.replicator.database.Database#update(com.continuent.tungsten.replicator.database.Table,
     *      java.util.ArrayList, java.util.ArrayList)
     */
    public int update(Table table, ArrayList<Column> whereClause,
            ArrayList<Column> values) throws SQLException
    {
        StringBuffer sb = new StringBuffer("UPDATE ");
        sb.append(table.getSchema());
        sb.append(".");
        sb.append(table.getName());
        sb.append(" SET ");
        sb.append(buildCommaAssign(values));
        if (whereClause != null)
        {
            sb.append(" ");
            sb.append(buildWhereClause(whereClause));
        }
        String SQL = sb.toString();

        ArrayList<Column> allColumns = new ArrayList<Column>(values);
        if (whereClause != null)
        {
            allColumns.addAll(whereClause);
        }
        return this.executePrepare(table, allColumns, SQL);
    }

    public boolean supportsReplace()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#replace(com.continuent.tungsten.replicator.database.Table)
     */
    public void replace(Table table) throws SQLException
    {
        if (supportsReplace())
        {
            String SQL = "";
            SQL += "REPLACE INTO " + table.getSchema() + "." + table.getName()
                    + " VALUES (";
            SQL += buildCommaValues(table.getAllColumns());
            SQL += ")";

            executePrepare(table, SQL);
        }
        else
        {
            try
            {
                delete(table, false);
            }
            catch (SQLException e)
            {
            }
            insert(table);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @return
     * @see com.continuent.tungsten.replicator.database.Database#delete(com.continuent.tungsten.replicator.database.Table,
     *      boolean)
     */
    public int delete(Table table, boolean allRows) throws SQLException
    {
        String SQL = "DELETE FROM " + table.getSchema() + "." + table.getName()
                + " ";
        if (!allRows)
        {
            SQL += buildWhereClause(table.getPrimaryKey().getColumns());
            return executePrepare(table, table.getPrimaryKey().getColumns(),
                    SQL);
        }
        else
            return executePrepare(table, new ArrayList<Column>(), SQL);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#prepareStatement(java.lang.String)
     */
    public PreparedStatement prepareStatement(String statement)
            throws SQLException
    {
        logger.debug("prepareStatement" + statement);
        return dbConn.prepareStatement(statement);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#createStatement()
     */
    public Statement createStatement() throws SQLException
    {
        logger.debug("createStatement");
        return dbConn.createStatement();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#commit()
     */
    public void commit() throws SQLException
    {
        logger.debug("commit");
        dbConn.commit();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#rollback()
     */
    public void rollback() throws SQLException
    {
        logger.debug("rollback");
        dbConn.rollback();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#setAutoCommit(boolean)
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        this.autoCommit = autoCommit;
        if (logger.isDebugEnabled())
            logger.debug("setAutoCommit = " + autoCommit);
        if (dbConn.getAutoCommit() != autoCommit)
            dbConn.setAutoCommit(autoCommit);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#createTable(com.continuent.tungsten.replicator.database.Table,
     *      boolean)
     */
    public void createTable(Table t, boolean replace) throws SQLException
    {
        boolean comma = false;

        if (replace)
            dropTable(t);

        String SQL = "CREATE TABLE " + t.getSchema() + "." + t.getName() + " (";

        Iterator<Column> i = t.getAllColumns().iterator();
        while (i.hasNext())
        {
            Column c = i.next();
            SQL += (comma ? ", " : "") + c.getName() + " "
                    + columnToTypeString(c)
                    + (c.isNotNull() ? " NOT NULL" : "");
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
                    SQL += "UNIQUE (";
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

        // Create the table.
        execute(SQL);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#dropTable(com.continuent.tungsten.replicator.database.Table)
     */
    public void dropTable(Table table)
    {
        String SQL = "DROP TABLE " + table.getSchema() + "." + table.getName()
                + " ";

        try
        {
            execute(SQL);
        }
        catch (SQLException e)
        {
            logger.debug("Unable to drop table; this may be expected", e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#close()
     */
    public void close()
    {
        disconnect();
    }

    public int nativeTypeToJavaSQLType(int nativeType) throws SQLException
    {
        return nativeType;
    }

    public int javaSQLTypeToNativeType(int javaSQLType) throws SQLException
    {
        return javaSQLType;
    }

    public Table findTable(int tableID) throws SQLException
    {
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#findTable(int,
     *      java.lang.String)
     */
    public Table findTable(int tableID, String scn) throws SQLException
    {
        return null;
    }

    /**
     * This function should be implemented in concrete class.
     * 
     * @param md DatabaseMetaData object
     * @param schemaName schema name
     * @param tableName table name
     * @return ResultSet as produced by DatabaseMetaData.getColumns() for a
     *         given schema and table
     * @throws SQLException
     */
    public abstract ResultSet getColumnsResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException;

    /**
     * This function should be implemented in concrete class.
     * 
     * @param md DatabaseMetaData object
     * @param schemaName schema name
     * @param tableName table name
     * @return ResultSet as produced by DatabaseMetaData.getPrimaryKeys() for a
     *         given schema and table
     * @throws SQLException
     */
    protected abstract ResultSet getPrimaryKeyResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException;

    /**
     * This function should be implemented in concrete class.
     * 
     * @param md DatabaseMetaData object
     * @param schemaName schema name
     * @param baseTablesOnly If true, return only base tables, not catalogs or
     *            views
     * @return ResultSet as produced by DatabaseMetaData.getTables() for a given
     *         schema
     * @throws SQLException
     */
    protected abstract ResultSet getTablesResultSet(DatabaseMetaData md,
            String schemaName, boolean baseTablesOnly) throws SQLException;

    public Table findTable(String schemaName, String tableName)
            throws SQLException
    {
        DatabaseMetaData md = this.getDatabaseMetaData();
        Table table = null;

        ResultSet rsc = getColumnsResultSet(md, schemaName, tableName);
        if (rsc.isBeforeFirst())
        {
            // found columns
            Map<String, Column> cm = new HashMap<String, Column>();
            table = new Table(schemaName, tableName);
            while (rsc.next())
            {
                String colName = rsc.getString("COLUMN_NAME");
                int colType = rsc.getInt("DATA_TYPE");
                long colLength = rsc.getLong("COLUMN_SIZE");
                boolean isNotNull = rsc.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls;
                String valueString = rsc.getString("COLUMN_DEF");

                Column column = new Column(colName, colType, colLength,
                        isNotNull, valueString);
                column.setPosition(rsc.getInt("ORDINAL_POSITION"));
                column.setTypeDescription(rsc.getString("TYPE_NAME")
                        .toUpperCase());
                table.AddColumn(column);
                cm.put(column.getName(), column);
            }

            ResultSet rsk = getPrimaryKeyResultSet(md, schemaName, tableName);
            if (rsk.isBeforeFirst())
            {
                // primary key found
                Key pKey = new Key(Key.Primary);
                while (rsk.next())
                {
                    String colName = rsk.getString("COLUMN_NAME");
                    Column column = cm.get(colName);
                    pKey.AddColumn(column);
                }
                table.AddKey(pKey);
            }
            rsk.close();
        }
        rsc.close();

        return table;
    }

    /**
     * Implement ability to fetch tables. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getTables(java.lang.String,
     *      boolean)
     */
    public ArrayList<Table> getTables(String schemaName, boolean baseTablesOnly)
            throws SQLException
    {
        DatabaseMetaData md = this.getDatabaseMetaData();
        ArrayList<Table> tables = new ArrayList<Table>();

        try
        {
            ResultSet rst = getTablesResultSet(md, schemaName, baseTablesOnly);
            if (rst.isBeforeFirst())
            {
                while (rst.next())
                {
                    String tableName = rst.getString("TABLE_NAME");
                    Table table = findTable(schemaName, tableName);
                    if (table != null)
                    {
                        tables.add(table);
                    }
                }
            }
            rst.close();
        }
        finally
        {
        }

        return tables;
    }

    // this is part of TREP-232 workaround
    static final String insertColumnsValues = " ("
                                                    + ConsistencyTable.idColumnName
                                                    + ", "
                                                    + ConsistencyTable.dbColumnName
                                                    + ", "
                                                    + ConsistencyTable.tblColumnName
                                                    + ", "
                                                    + ConsistencyTable.offsetColumnName
                                                    + ", "
                                                    + ConsistencyTable.limitColumnName
                                                    + ", "
                                                    + ConsistencyTable.methodColumnName
                                                    + ") VALUES (";

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#consistencyCheck(com.continuent.tungsten.replicator.database.Table,
     *      com.continuent.tungsten.replicator.consistency.ConsistencyCheck)
     */
    public void consistencyCheck(Table ct, ConsistencyCheck cc)
            throws SQLException, ConsistencyException
    {
        String tableName = cc.getTableName();
        String schemaName = cc.getSchemaName();
        int id = cc.getCheckId();

        ArrayList<Column> ctColumns = ct.getAllColumns();

        // Initialize primary key columns - used in WHERE clauses
        ctColumns.get(ConsistencyTable.dbColumnIdx).setValue(schemaName);
        ctColumns.get(ConsistencyTable.tblColumnIdx).setValue(tableName);
        ctColumns.get(ConsistencyTable.idColumnIdx).setValue(id);

        // Initialize row defaults
        ctColumns.get(ConsistencyTable.offsetColumnIdx).setValue(
                cc.getRowOffset());
        ctColumns.get(ConsistencyTable.limitColumnIdx).setValue(
                cc.getRowLimit());
        ctColumns.get(ConsistencyTable.methodColumnIdx)
                .setValue(cc.getMethod());

        // if (supportsUseDefaultSchema())
        // {
        // /*
        // * on MySQL this circumvents the replication protection on tungsten
        // * schema. Thus, updates to tungsten.consistency table will get into
        // * binlog
        // */
        // logger.info("Consistency check: switching to schema '" + schemaName
        // + "'...");
        // useDefaultSchema(schemaName);
        // }

        // Start consistency check transaction
        setAutoCommit(false);
        // Prepare row that will hold consistency check values
        // TENT-134: Delete holds a lock that causes LOCK WAIT TIMEOUT on
        // InnoDB. Commented out for now.
        // delete(ct); // WHERE is taken from prim key

        // Database.insert() does not work with TIMESTAMP fields on MySQL: I'm
        // getting
        // com.mysql.jdbc.MysqlDataTruncation: Data truncation: Incorrect
        // datetime value: 'CURRENT_TIMESTAMP' for column 'ts' at row 1
        // Looks like JDBC puts this magic symbol there which MySQL refuses to
        // recognize. Very sad.
        // Have to compose own INSERT statement as a workaround which might be
        // not DBMS-portable.
        StringBuffer insert = new StringBuffer(256);
        insert.append("INSERT INTO ");
        insert.append(ct.getSchema());
        insert.append('.');
        insert.append(ct.getName());
        insert.append(insertColumnsValues);
        insert.append(id);
        insert.append(", '");
        insert.append(schemaName);
        insert.append("', '");
        insert.append(tableName);
        insert.append("', ");
        insert.append(cc.getRowOffset());
        insert.append(", ");
        insert.append(cc.getRowLimit());
        insert.append(", '");
        insert.append(cc.getMethod());
        insert.append("')");

        Statement st = dbConn.createStatement();
        try
        {
            st.execute(insert.toString());
        }
        catch (Exception e)
        {
            String msg = insert.toString() + " failed: " + e.getMessage();
            logger.error(msg);
            throw new ConsistencyException(msg, e);
        }

        // Perform consistency check
        try
        {
            ResultSet rs = cc.performConsistencyCheck(this);

            if (rs.next())
            {
                // Create SET array
                Column col;
                ArrayList<Column> setColumns = new ArrayList<Column>();
                col = ctColumns.get(ConsistencyTable.masterCrcColumnIdx);
                col.setValue(rs.getString(ConsistencyTable.thisCrcColumnName));
                setColumns.add(col);
                col = ctColumns.get(ConsistencyTable.masterCntColumnIdx);
                col.setValue(rs.getInt(ConsistencyTable.thisCntColumnName));
                setColumns.add(col);
                rs.close();

                // record CC values obtained on master
                update(ct, ct.getPrimaryKey().getColumns(), setColumns);
                // commit consistency check transaction
            }
            else
            {
                rs.close();
                String msg = "Consistency check returned empty ResultSet.";
                logger.warn(msg);
                // throw new ConsistencyException(msg);
            }
            commit();
        }
        finally
        {
            st.close();
            // Ensure rollback after an error to release locks.
            try
            {
                rollback();
            }
            catch (Exception e)
            {
            }
        }
    }

    public void createTable(Table table, boolean replace,
            String tungstenTableType) throws SQLException
    {
        createTable(table, replace);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#prepareOptionSetStatement(java.lang.String,
     *      java.lang.String)
     */
    public String prepareOptionSetStatement(String optionName,
            String optionValue)
    {
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getBlobAsBytes(ResultSet,
     *      int)
     */
    public byte[] getBlobAsBytes(ResultSet resultSet, int column)
            throws SQLException
    {
        Blob blob = resultSet.getBlob(column);
        return blob.getBytes(1L, (int) blob.length());
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getDatabaseObjectName(java.lang.String)
     */
    public String getDatabaseObjectName(String name)
    {
        return name;
    }

}

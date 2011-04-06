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
 * Initial developer(s): Linas Virbalas
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.database;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.dbms.OneRowChange;

/**
 * Implements DBMS-specific operations for MySQL.
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 */
public class GreenplumDatabase extends AbstractDatabase
{
    private static Logger logger = Logger.getLogger(GreenplumDatabase.class);

    public GreenplumDatabase() throws SQLException
    {
        dbms = DBMS.GREENPLUM;
        // Hard code the driver so it gets loaded correctly.
        dbDriver = "org.postgresql.Driver";
    }

    protected String columnToTypeString(Column c)
    {
        switch (c.getType())
        {
            case Types.TINYINT :
                return "SMALLINT";

            case Types.SMALLINT :
                return "SMALLINT";

            case Types.INTEGER :
                return "INTEGER";

            case Types.BIGINT :
                return "BIGINT";

            case Types.CHAR :
            {
                if (c.getLength() == 1)
                    // TODO: remove this dirty hack, written to support storing
                    // boolean values into "character(1)" type "last_frag" field
                    // of "trep_commit_seqno" and "history" tables.
                    return "BOOLEAN";
                else
                    return "CHAR(" + c.getLength() + ")";
            }

            case Types.VARCHAR :
                return "VARCHAR(" + c.getLength() + ")";

            case Types.DATE :
                return "DATE";

            case Types.TIMESTAMP :
                return "TIMESTAMP";

            case Types.CLOB :
                return "TEXT";

            case Types.BLOB :
                return "BYTEA";

            default :
                return "UNKNOWN";
        }
    }

    /**
     * Connect to a PostgreSQL database. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#connect()
     */
    public void connect() throws SQLException
    {
        connect(false);
    }

    /**
     * Connect to a PostgreSQL database. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#connect(boolean)
     */
    public void connect(boolean binlog) throws SQLException
    {
        // Use superclass method to avoid missing things like loading the
        // driver class.
        super.connect(binlog);
    }

    public boolean supportsReplace()
    {
        return false;
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
        // TODO: we might want to retrieve the search_path first and then use it
        // in the new path, i.e.: $search_path = $schema, $search_path
        return "SET search_path TO " + schema + ", \"$user\"";
    }
    
    public boolean supportsCreateDropSchema()
    {
        return true;
    }

    /**
     * Checks whether the schema exists and, if not, creates it. This mimics
     * MySQLDatabase's behavior "CREATE DATABASE IF NOT EXISTS". {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#createSchema(java.lang.String)
     */
    public void createSchema(String schema) throws SQLException
    {
        if (!getSchemas().contains(schema))
        {
            String SQL = "CREATE SCHEMA " + schema;
            execute(SQL);
        }
        else
            logger.debug("Schema already exists, thus not recreating it: "
                    + schema);
    }

    /**
     * Checks whether the schema exists and, if it does, drops it. This mimics
     * MySQLDatabase's behavior "DROP DATABASE IF EXISTS". {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#dropSchema(java.lang.String)
     */
    public void dropSchema(String schema) throws SQLException
    {
        if (getSchemas().contains(schema))
        {
            String SQL = "DROP SCHEMA " + schema;
            execute(SQL);
        }
        else
            logger.debug("Schema does not exist, thus not dropping it: "
                    + schema);
    }
    
    /**
     * Checks whether the given table exists in the currently connected database. Check is made against "schema.table".
     * @return true, if table exists, false, if not.
     */
    private boolean tableExists(Table t) throws SQLException
    {
        String sql = "SELECT * FROM pg_tables WHERE schemaname='"
                + t.getSchema() + "' and tablename='" + t.getName() + "'";
        Statement stmt = dbConn.createStatement();
        ResultSet res = stmt.executeQuery(sql);
        if (res.next())
            return true;
        else
            return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#createTable(com.continuent.tungsten.replicator.database.Table,
     *      boolean, java.lang.String)
     */
    @Override
    public void createTable(Table t, boolean replace) throws SQLException
    {
        boolean comma = false;
        boolean hasNonUnique = false;

        if (replace)
            dropTable(t);
        else
        {
            // If table already exists, do nothing. This behavior is a mimic of
            // MySQLDatabase for initial configuration to work. For some reason,
            // Replicator is trying to create Tungsten tables more than once.
            if(tableExists(t))
                return;
        }

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
            if (key.getType() == Key.NonUnique)
            {
                // Non-unique keys will be created with a separate CREATE
                // INDEX statement.
                hasNonUnique = true;
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

        // Create the table.
        execute(SQL);

        // Create non-unique keys.
        if (hasNonUnique)
            createNonUnique(t);
    }

    /**
     * Creates a non unique index for the given table.
     */
    private void createNonUnique(Table t) throws SQLException
    {
        boolean comma = false;
        int indexNumber = 1;

        Iterator<Key> j = t.getKeys().iterator();

        while (j.hasNext())
        {
            Key key = j.next();
            if (key.getType() == Key.NonUnique)
            {
                String SQL = "CREATE INDEX " + t.getName() + "_" + indexNumber;
                SQL += " ON " + t.getSchema() + "." + t.getName() + "(";

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
                execute(SQL);
            }
        }
    }

    /**
     * Before using session variables in PostgreSQL one needs to define them in
     * postgresql.conf
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsSessionVariables()
     */
    public boolean supportsSessionVariables()
    {
        return false;
    }

    /**
     * Returns schemas in the currently connected database. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getSchemas()
     */
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
        // TENT-288: the following call is different from MySQL's, because of
        // getColumns(catalog, schema, table, col) definition and because in
        // PostgreSQL schema~eventSchema (not catalog~eventSchema as in MySQL).
        return md.getColumns(null, schemaName, tableName, null);
    }

    protected ResultSet getPrimaryKeyResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException
    {
        return md.getPrimaryKeys(schemaName, null, tableName);
    }

    protected ResultSet getTablesResultSet(DatabaseMetaData md,
            String schemaName, boolean baseTablesOnly) throws SQLException
    {
        // TODO:  Implement ability to return base tables only. 
        return md.getTables(schemaName, null, null, null);
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
        String retval = "FLOOR(EXTRACT(EPOCH FROM (";
        if (string1 == null)
            retval += "?::timestamp";
        else
            retval += string1;
        retval += " - ";
        if (string2 == null)
            retval += "?::timestamp";
        else
            retval += string2;
        retval += ")))";

        return retval;
    }

    public String getNowFunction()
    {
        return "NOW()";
    }

    public String getPlaceHolder(OneRowChange.ColumnSpec col, Object colValue)
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
     * @see com.continuent.tungsten.replicator.database.Database#getBlobAsBytes(ResultSet,
     *      int)
     */
    @Override
    public byte[] getBlobAsBytes(ResultSet resultSet, int column)
            throws SQLException
    {
        return resultSet.getBytes(column);
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#getDatabaseObjectName(java.lang.String)
     */
    @Override
    public String getDatabaseObjectName(String name)
    {
        return "\"" + name + "\"";
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#getSqlNameMatcher()
     */
    @Override
    public SqlOperationMatcher getSqlNameMatcher() throws DatabaseException
    {
        // TODO: Develop matcher for Drizzle dialect.
        return new MySQLOperationMatcher();
    }

}

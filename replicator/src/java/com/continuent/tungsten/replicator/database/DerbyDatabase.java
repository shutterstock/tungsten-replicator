/**
 * Tungsten: An Application Server for uni/cluster.
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

package com.continuent.tungsten.replicator.database;

import java.io.BufferedWriter;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

import com.continuent.tungsten.commons.csv.CsvWriter;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Implements DBMS-specific operations for the Derby database.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class DerbyDatabase extends AbstractDatabase
{
    /** Create a new instance. */
    public DerbyDatabase()
    {
        dbms = DBMS.DERBY;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#getSqlNameMatcher()
     */
    @Override
    public SqlOperationMatcher getSqlNameMatcher() throws ReplicatorException
    {
        // TODO: Develop matcher for Drizzle dialect.
        return new MySQLOperationMatcher();
    }

    /**
     * Provide column specifications that work in Derby, which hews very closely
     * to the SQL-92 standard. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#columnToTypeString(com.continuent.tungsten.replicator.database.Column)
     */
    protected String columnToTypeString(Column c)
    {
        switch (c.getType())
        {
            case Types.INTEGER :
                return "INTEGER";

            case Types.BIGINT :
                return "BIGINT";

            case Types.CHAR :
                return "CHAR(" + c.getLength() + ")";

            case Types.VARCHAR :
                return "VARCHAR(" + c.getLength() + ")";

            case Types.DATE :
                return "DATE";

            case Types.TIMESTAMP :
                return "TIMESTAMP";

            case Types.CLOB :
                return "CLOB";

            case Types.BLOB :
                return "BLOB";

            default :
                return "UNKNOWN";
        }
    }

    /**
     * Derby does not support REPLACE. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#supportsReplace()
     */
    public boolean supportsReplace()
    {
        return false;
    }

    public ArrayList<String> getSchemas() throws SQLException
    {
        throw new UnsupportedOperationException("Not implemented.");
    }

    public ResultSet getColumnsResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException
    {
        throw new UnsupportedOperationException("Not implemented.");
    }

    protected ResultSet getPrimaryKeyResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException
    {
        throw new UnsupportedOperationException("Not implemented.");
    }

    protected ResultSet getTablesResultSet(DatabaseMetaData md,
            String schemaName, boolean baseTablesOnly) throws SQLException
    {
        throw new UnsupportedOperationException("Not implemented.");
    }

    public String getNowFunction()
    {
        return "CURRENT_TIMESTAMP";
    }

    /**
     * getTimeDiff returns the database-specific way of subtracting two "dates"
     * and return the result in seconds complete with space for the two bind
     * variables. I, Scott, am not sure how to express the differences between
     * two dates in derby in seconds. This function is currently only called by
     * the time based "thl purge" command. For now, simply subtract the two
     * dates.
     */
    public String getTimeDiff(String string1, String string2)
    {
        String retval = "";
        if (string1 == null)
            retval += "?";
        else
            retval += string1;
        retval += " - ";
        if (string2 == null)
            retval += "?";
        else
            retval += string2;
        retval += "";

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
}

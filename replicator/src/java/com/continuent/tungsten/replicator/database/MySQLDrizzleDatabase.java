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

package com.continuent.tungsten.replicator.database;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class MySQLDrizzleDatabase extends MySQLDatabase
{
    public MySQLDrizzleDatabase() throws SQLException
    {
        super();
        dbDriver = "org.drizzle.jdbc.DrizzleDriver";
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.MySQLDatabase#getColumnsResultSet(java.sql.DatabaseMetaData,
     *      java.lang.String, java.lang.String)
     */
    public ResultSet getColumnsResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException
    {
        // Drizzle driver uses schema argument for MySQL database name vs.
        // catalog name for Connector/J. Unclear who is right...
        return md.getColumns(null, schemaName, tableName, null);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.MySQLDatabase#getTablesResultSet(java.sql.DatabaseMetaData,
     *      java.lang.String, boolean)
     */
    protected ResultSet getTablesResultSet(DatabaseMetaData md,
            String schemaName, boolean baseTablesOnly) throws SQLException
    {
        String types[] = null;
        if (baseTablesOnly)
            types = new String[]{"TABLE"};

        return md.getTables(null, schemaName, null, types);
    }

    @Override
    protected ResultSet getPrimaryKeyResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException
    {
        return md.getPrimaryKeys(null, schemaName, tableName);
    }

    @Override
    public String getPlaceHolder(ColumnSpec col, Object colValue, String typeDesc)
    {
        if (col.getType() == Types.BLOB && colValue instanceof SerialBlob
                && typeDesc != null && typeDesc.contains("TEXT"))
            return " UNHEX ( ? ) ";
        else if (col.getType() == Types.VARCHAR && colValue instanceof byte[])
            return " UNHEX ( ? ) ";
        return super.getPlaceHolder(col, colValue, typeDesc);
    }

}

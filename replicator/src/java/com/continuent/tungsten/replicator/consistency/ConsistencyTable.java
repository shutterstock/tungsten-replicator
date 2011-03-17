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
 * Initial developer(s): Alex Yurchenko
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.consistency;

import java.sql.Types;

import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.Table;

/**
 * This class defines common parameters for consistency checks
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alex Yurchenko</a>
 * @version 1.0
 */
public class ConsistencyTable
{
    public static final String TABLE_NAME          = "consistency";

    /**
     * NOTE: ...ColumnIdx values are indexes in <code>ArrayList<Column></code>
     * returned by Table.getAllColumns(), not in ResultSet row. For ResultSet
     * row those should be incremented by 1.
     */
    public static final int    dbColumnIdx         = 0;
    public static final String dbColumnName        = "db";
    public static final int    tblColumnIdx        = 1;
    public static final String tblColumnName       = "tbl";
    public static final int    idColumnIdx         = 2;
    public static final String idColumnName        = "id";
    public static final int    offsetColumnIdx     = 3;
    public static final String offsetColumnName    = "row_offset";
    public static final int    limitColumnIdx      = 4;
    public static final String limitColumnName     = "row_limit";
    public static final int    thisCrcColumnIdx    = 5;
    public static final String thisCrcColumnName   = "this_crc";
    public static final int    thisCntColumnIdx    = 6;
    public static final String thisCntColumnName   = "this_cnt";
    public static final int    masterCrcColumnIdx  = 7;
    public static final String masterCrcColumnName = "master_crc";
    public static final int    masterCntColumnIdx  = 8;
    public static final String masterCntColumnName = "master_cnt";
    public static final int    tsColumnIdx         = 9;
    public static final String tsColumnName        = "ts";
    public static final int    methodColumnIdx     = 10;
    public static final String methodColumnName    = "method";

    public static final int    ROW_UNSET           = -1;

    public static Table getConsistencyTableDefinition(String schema)
    {
        Table t = new Table(schema, TABLE_NAME);

        t.AddColumn(new Column(dbColumnName, Types.CHAR, 64));
        t.AddColumn(new Column(tblColumnName, Types.CHAR, 64));
        t.AddColumn(new Column(idColumnName, Types.INTEGER));
        t.AddColumn(new Column(offsetColumnName, Types.INTEGER, 0, true,
                ROW_UNSET));
        t.AddColumn(new Column(limitColumnName, Types.INTEGER, 0, true,
                ROW_UNSET));
        t.AddColumn(new Column(thisCrcColumnName, Types.CHAR, 40));
        t.AddColumn(new Column(thisCntColumnName, Types.INTEGER));
        t.AddColumn(new Column(masterCrcColumnName, Types.CHAR, 40));
        t.AddColumn(new Column(masterCntColumnName, Types.INTEGER));
        t.AddColumn(new Column(tsColumnName, Types.TIMESTAMP));
        t.AddColumn(new Column(methodColumnName, Types.CHAR, 32));

        Key pk = new Key(Key.Primary);
        pk.AddColumn(t.getAllColumns().get(dbColumnIdx));
        pk.AddColumn(t.getAllColumns().get(tblColumnIdx));
        pk.AddColumn(t.getAllColumns().get(idColumnIdx));

        t.AddKey(pk);

        return t;
    }
}

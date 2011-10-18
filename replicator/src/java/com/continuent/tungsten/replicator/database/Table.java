/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2007-2011 Continuent Inc.
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

import java.util.ArrayList;
import java.util.Iterator;
import java.sql.PreparedStatement;

/**
 * This class defines a table
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @version 1.0
 */
public class Table
{
    String                  schema          = null;
    String                  name            = null;
    boolean                 temporary       = false;
    ArrayList<Column>       allColumns      = null;
    ArrayList<Column>       nonKeyColumns   = null;
    ArrayList<Key>          keys            = null;
    Key                     primaryKey      = null;
    PreparedStatement       statements[];
    // Cache of prepared statements
    boolean                 cacheStatements = false;

    static public final int INSERT          = 0;
    static public final int UPDATE1         = 1;
    static public final int UPDATE2         = 2;
    static public final int DELETE          = 3;
    static public final int NPREPPED        = 4;

    // scn is eventually used for caching purpose. This table object will be
    // cached and reused if possible.
    private String          scn;

    // tableId as found in MySQL binlog can be used to detect schema changes.
    // Here, it has the same purpose as previous scn field
    private long            tableId;

    /**
     * Creates a new <code>Table</code> object
     */
    public Table(String schema, String name)
    {
        int i;

        this.schema = schema;
        this.name = name;
        this.allColumns = new ArrayList<Column>();
        this.nonKeyColumns = new ArrayList<Column>();
        this.keys = new ArrayList<Key>();
        this.scn = null;
        this.tableId = -1;
        this.statements = new PreparedStatement[Table.NPREPPED];
        this.cacheStatements = false;
        // Following probably not needed
        for (i = 0; i < Table.NPREPPED; i++)
            this.statements[i] = null;
    }

    public Table(String schema, String name, boolean cacheStatements)
    {
        this(schema, name);
        this.cacheStatements = cacheStatements;
    }

    public boolean getCacheStatements()
    {
        return this.cacheStatements;
    }

    void purge(ArrayList<Column> purgeValues, ArrayList<Column> fromList)
    {
        int idx;
        Iterator<Column> i = purgeValues.iterator();
        while (i.hasNext())
        {
            Column c1 = i.next();
            if ((idx = fromList.indexOf(c1)) == -1)
                continue;
            fromList.remove(idx);
        }
    }

    public PreparedStatement getStatement(int statementNumber)
    {
        return this.statements[statementNumber];
    }

    public void setStatement(int statementNumber, PreparedStatement statement)
    {
        // This will leak prepared statements if a statement already
        // exists in the slot but I currently do not want to
        // have a "Table" know about a "Database" which is what we would need
        // to close these statements.
        this.statements[statementNumber] = statement;
    }

    public void AddColumn(Column column)
    {
        allColumns.add(column);
        nonKeyColumns.add(column);
    }

    public void AddKey(Key key)
    {
        keys.add(key);
        if (key.getType() == Key.Primary)
        {
            primaryKey = key;
            purge(key.getColumns(), nonKeyColumns);
        }
    }

    public void Dump()
    {
        System.out.format("%s.%s\n", this.schema, this.name);
        Iterator<Column> i = allColumns.iterator();
        while (i.hasNext())
        {
            Column c = i.next();
            c.Dump();
        }
    }

    public String getSchema()
    {
        return schema;
    }

    public String getName()
    {
        return name;
    }

    public String fullyQualifiedName()
    {
        return schema + "." + name;
    }

    public synchronized boolean isTemporary()
    {
        return temporary;
    }

    public synchronized void setTemporary(boolean temporary)
    {
        this.temporary = temporary;
    }

    public ArrayList<Column> getAllColumns()
    {
        return allColumns;
    }

    public ArrayList<Column> getNonKeyColumns()
    {
        return nonKeyColumns;
    }

    public ArrayList<Key> getKeys()
    {
        return keys;
    }

    public Key getPrimaryKey()
    {
        return primaryKey;
    }

    /*
     * columnNumbers here are one based. perhaps we should record the column
     * number in the Column class as well.
     */
    public Column findColumn(int columnNumber)
    {
        /* This assumes column were added in column number order */
        return allColumns.get(columnNumber - 1);
    }

    public int getColumnCount()
    {
        return allColumns.size();
    }

    /*
     * Following methods are used for tables cache management
     */

    /**
     * getSCN returns the scn associated to this table, if any.
     * 
     * @return the scn value
     */
    public String getSCN()
    {
        return scn;
    }

    /**
     * setSCN stores a scn value associated to this table
     * 
     * @param scn the scn that is associated with this table
     */
    public void setSCN(String scn)
    {
        this.scn = scn;
    }

    /**
     * Sets the tableId value.
     * 
     * @param tableId The tableId to set.
     */
    public void setTableId(long tableId)
    {
        this.tableId = tableId;
    }

    /**
     * Returns the tableId value.
     * 
     * @return Returns the tableId.
     */
    public long getTableId()
    {
        return tableId;
    }

    @Override
    public String toString()
    {
        return this.schema + "." + this.name;
    }
}

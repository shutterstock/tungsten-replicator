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
 * Initial developer(s): Scott Martin
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.database;

import java.util.ArrayList;

/**
 * This class defines a Key
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @version 1.0
 */
public class Key 
{
    public static final int IllegalType = 0;
    public static final int NonUnique   = 1;
    public static final int Primary     = 2;
    public static final int Unique      = 3;

    int               type    = Key.NonUnique;
    ArrayList<Column> columns = null;
  

    /**
     * Creates a new <code>Key</code> object
     */

    public Key(int type)
    {
       this.type    = type;
       this.columns = new ArrayList<Column>();
    }

    public void AddColumn(Column column)
    {
        columns.add(column);
    }

    public int getType()
    {
       return this.type;
    }

    public ArrayList<Column> getColumns()
    {
       return this.columns;
    }

}

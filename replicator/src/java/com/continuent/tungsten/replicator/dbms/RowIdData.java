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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.dbms;

/**
 * Defines a SQL statement that must be replicated.  
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class RowIdData extends DBMSData
{
    private static final long serialVersionUID = 1L;
    
    private long rowId;
  
    public RowIdData()
    {
    	super();
        this.rowId = -1;
    }

    public RowIdData(long rowId)
    {
    	super();
        this.rowId = rowId;
    }
    
    /**
     * Returns the SQL statement that must be replicated. 
     */
    public long getRowId()
    {
        return rowId;
    }

    public void setRowId(long rowId)
    {
        this.rowId = rowId;
    }   
}

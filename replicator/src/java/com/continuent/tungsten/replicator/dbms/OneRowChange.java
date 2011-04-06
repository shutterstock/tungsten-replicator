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
 * Initial developer(s): Seppo Jaakola
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.dbms;

import java.io.Serializable;
import java.util.ArrayList;

import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;

/**
 * Holds information from a
 */
public class OneRowChange implements Serializable
{
    private static final long serialVersionUID = 1L;

    /*
     * following types make it possible to apply changes by prepared statements.
     * One RowChangeData corresponds with one prepared statement. Binding to
     * variables are made from keys and columnvals lists.
     */

    /* ColumnSpec is a "header" for columnVal arrays */
    public class ColumnSpec implements Serializable
    {
        private static final long serialVersionUID = 1L;
        private int               index;
        private String            name;
        private int               type;                 // Type assignment from
                                                         // java.sql.Types
        private boolean           signed;
        private int               length;
        private boolean           notNull;              // Is the column a NOT
                                                         // NULL column
        private boolean           blob;
        private String            typeDescription;

        public boolean isBlob()
        {
            return blob;
        }

        public void setBlob(boolean blob)
        {
            this.blob = blob;
        }

        public ColumnSpec()
        {
            this.name = null;
            this.type = java.sql.Types.NULL;
            this.length = 0;
            this.notNull = false;
            this.signed = true;
            this.blob = false;
        }

        public int getIndex()
        {
            return this.index;
        }

        public void setName(String name)
        {
            this.name = name;
        }

        public void setType(int type)
        {
            this.type = type;

        }

        public int getLength()
        {
            return length;
        }

        public void setLength(int length)
        {
            this.length = length;
        }

        public boolean isNotNull()
        {
            return notNull;
        }

        public void setNotNull(boolean notNull)
        {
            this.notNull = notNull;
        }

        public String getName()
        {
            return name;
        }

        public int getType()
        {
            return type;
        }

        public void setIndex(int index)
        {
            this.index = index;
        }

        public void setSigned(boolean signed)
        {
            this.signed = signed;
        }

        public boolean isUnsigned()
        {
            return !signed;
        }

        public String getTypeDescription()
        {
            return typeDescription;
        }

        public void setTypeDescription(String typeDescription)
        {
            this.typeDescription = typeDescription;
        }
    }
    public class ColumnVal implements Serializable
    {
        private static final long serialVersionUID = 1L;
        private Serializable      value;

        public void setValueNull()
        {
            value = null;
        }

        public void setValue(Serializable value)
        {
            this.value = value;
        }

        public Object getValue()
        {
            return value;
        }
    }

    private String                          schemaName;
    private String                          tableName;
    private ActionType                      action;

    /* column specifications for key and data columns */
    private ArrayList<ColumnSpec>           keySpec;
    private ArrayList<ColumnSpec>           columnSpec;

    /* values for key components (may be empty) */
    private ArrayList<ArrayList<ColumnVal>> keyValues;

    /* values for data column components */
    private ArrayList<ArrayList<ColumnVal>> columnValues;
    private long                            tableId;

    public ArrayList<ColumnSpec> getColumnSpec()
    {
        return columnSpec;
    }

    public void setColumnSpec(ArrayList<ColumnSpec> columnSpec)
    {
        this.columnSpec = columnSpec;
    }

    public ArrayList<ArrayList<ColumnVal>> getColumnValues()
    {
        return columnValues;
    }

    public void setColumnValues(ArrayList<ArrayList<ColumnVal>> columnValues)
    {
        this.columnValues = columnValues;
    }

    public ArrayList<ColumnSpec> getKeySpec()
    {
        return keySpec;
    }

    public void setKeySpec(ArrayList<ColumnSpec> keySpec)
    {
        this.keySpec = keySpec;
    }

    public ArrayList<ArrayList<ColumnVal>> getKeyValues()
    {
        return keyValues;
    }

    public void setKeyValues(ArrayList<ArrayList<ColumnVal>> keyValues)
    {
        this.keyValues = keyValues;
    }

    public ActionType getAction()
    {
        return action;
    }

    public void setAction(ActionType action)
    {
        this.action = action;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public OneRowChange(String schemaName, String tableName, ActionType action,
            ArrayList<ColumnSpec> keySpec,
            ArrayList<ArrayList<ColumnVal>> keyValues,
            ArrayList<ColumnSpec> columnSpec,
            ArrayList<ArrayList<ColumnVal>> columnValues)
    {
        // TODO : Not referenced ? To be removed ?
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.action = action;
        this.keySpec = keySpec;
        this.keyValues = keyValues;
        this.columnSpec = columnSpec;
        this.columnValues = columnValues;
    }

    public OneRowChange(String schemaName, String tableName, ActionType action)
    {
        this();
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.action = action;
    }

    public OneRowChange()
    {
        keySpec = new ArrayList<ColumnSpec>();
        keyValues = new ArrayList<ArrayList<ColumnVal>>();
        columnSpec = new ArrayList<ColumnSpec>();
        columnValues = new ArrayList<ArrayList<ColumnVal>>();
        this.tableId = -1;
    }

    public void setTableId(long tableId)
    {
        this.tableId = tableId;
    }

    public long getTableId()
    {
        return tableId;
    }
}

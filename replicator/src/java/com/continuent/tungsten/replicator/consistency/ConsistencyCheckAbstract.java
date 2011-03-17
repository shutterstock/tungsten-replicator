/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2009 Continuent Inc.
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
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.consistency;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.ResultSet;

import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Table;

/**
 * This class defines a ConsistencyCheckAbstract
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alex Yurchenko</a>
 * @version 1.0
 */
public abstract class ConsistencyCheckAbstract implements ConsistencyCheck
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    protected int    id     = -1;
    protected Table  table  = null;
    protected String method = null;

    protected ConsistencyCheckAbstract(int id, Table table, String method)
    {
        this.id = id;
        this.table = table;
        this.method = method;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.consistency.ConsistencyCheck#getCheckId()
     */
    public final int getCheckId()
    {
        return id;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.consistency.ConsistencyCheck#getSchemaName()
     */
    public final String getSchemaName()
    {
        return table.getSchema();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.consistency.ConsistencyCheck#getTableName()
     */
    public final String getTableName()
    {
        return table.getName();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.consistency.ConsistencyCheck#getRowOffset()
     */
    public int getRowOffset()
    {
        return ConsistencyTable.ROW_UNSET;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.consistency.ConsistencyCheck#getRowLimit()
     */
    public int getRowLimit()
    {
        return ConsistencyTable.ROW_UNSET;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.consistency.ConsistencyCheck#getMethod()
     */
    public final String getMethod()
    {
        return method;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.consistency.ConsistencyCheck#performConsistencyCheck(com.continuent.tungsten.replicator.database.Database)
     */
    public abstract ResultSet performConsistencyCheck(Database conn)
            throws ConsistencyException;

    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("ID: ");
        sb.append(id);
        sb.append("; TABLE: ");
        sb.append(table.getSchema());
        sb.append('.');
        sb.append(table.getName());
        sb.append(", LIMITS: ");
        sb.append(getRowOffset());
        sb.append(", ");
        sb.append(getRowLimit());
        return sb.toString();
    }

    // serialization stuff
    private static final Object          serializer = new Object();
    private static ByteArrayOutputStream bos        = new ByteArrayOutputStream();
    private static ObjectOutputStream    oos;
    static
    {
        try
        {
            oos = new ObjectOutputStream(bos);
        }
        catch (IOException e)
        {
            throw new ExceptionInInitializerError(e);
        }
    }

    public byte[] serialize() throws ConsistencyException
    {
        byte[] ret = null;
        synchronized (serializer)
        {
            bos.reset();
            try
            {
                oos.reset();
                oos.writeObject(this);
                oos.flush();
            }
            catch (IOException e)
            {
                throw new ConsistencyException(
                        "Failed to serialize ConsistencyCheck object: "
                                + e.getMessage(), e);
            }
            ret = bos.toByteArray();
        }
        return ret;
    }

    public static ConsistencyCheck deserialize(byte[] bytes)
            throws ConsistencyException
    {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        try
        {
            ObjectInputStream ois = new ObjectInputStream(bis);
            Object obj = ois.readObject();
            if (obj instanceof ConsistencyCheck)
            {
                return (ConsistencyCheck) ois.readObject();
            }
            throw new ConsistencyException(
                    "This is not a ConsistencyCheck object.");
        }
        catch (Exception e)
        {
            throw new ConsistencyException(
                    "Failed to deserialize ConsistencyCheck object:"
                            + e.getMessage(), e);
        }
    }
}

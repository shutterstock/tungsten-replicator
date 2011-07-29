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
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.thl.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.thl.THLException;

/**
 * Implements serialization using default Java serialization.
 */
public class JavaSerializer implements Serializer
{
    /**
     * Deserializes THLEvent off the stream. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.serializer.Serializer#deserializeEvent(java.io.InputStream)
     */
    public THLEvent deserializeEvent(InputStream inStream) throws ReplicatorException,
            IOException
    {
        ObjectInputStream oIS = new ObjectInputStream(inStream);
        Object revent;
        try
        {
            revent = oIS.readObject();
        }
        catch (ClassNotFoundException e)
        {
            throw new THLException(
                    "Class not found while deserializing THLEvent", e);
        }
        if (revent instanceof THLEvent)
            return (THLEvent) revent;
        else
        {
            throw new THLException(
                    "Unexpected class found when deserializing: "
                            + revent.getClass().getName());
        }
    }

    /**
     * Serialize the THL event onto the stream. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.serializer.Serializer#serializeEvent(com.continuent.tungsten.replicator.thl.THLEvent,
     *      java.io.OutputStream)
     */
    public void serializeEvent(THLEvent event, OutputStream outStream)
            throws IOException
    {
        ObjectOutputStream oOS = new ObjectOutputStream(outStream);
        oOS.writeObject(event);
        oOS.flush();
    }
}
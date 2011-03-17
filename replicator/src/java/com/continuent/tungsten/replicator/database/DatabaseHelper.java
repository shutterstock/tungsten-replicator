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

import java.io.ByteArrayOutputStream;
import java.sql.SQLException;

import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;

/**
 * Implements helper methods for database operations.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class DatabaseHelper
{
    /**
     * Create a serializable blob from a byte array.
     * 
     * @param bytes Array from which to read
     * @throws SQLException Thrown if the safe blob cannot be instantiated.
     */
    public static SerialBlob getSafeBlob(byte[] bytes) throws SQLException
    {
        return getSafeBlob(bytes, 0, bytes.length);
    }

    /**
     * Create a serializable blob from a byte array.
     * 
     * @param bytes Array from which to read
     * @param off Offset into the array
     * @param len Length to read from offset
     * @throws SQLException Thrown if the safe blob cannot be instantiated.
     */
    public static SerialBlob getSafeBlob(byte[] bytes, int off, int len)
            throws SQLException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(bytes, off, len);
        byte[] newBytes = baos.toByteArray();
        return new SerialBlob(newBytes);
    }
}

/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2009 Continuent Inc.
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
package com.continuent.tungsten.replicator.extractor.mysql;

import java.sql.Blob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialException;

/**
 * This class defines a SerialBlob
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
@SuppressWarnings("serial")
public class SerialBlob extends javax.sql.rowset.serial.SerialBlob
{

    public SerialBlob(byte[] b) throws SerialException, SQLException
    {
        super(b);
    }

    public SerialBlob(Blob blob) throws SerialException, SQLException
    {
        super(blob);
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SerialException
    {
        if (length <= 0)
            return new byte[0];

        return super.getBytes(pos, length);
    }
}
